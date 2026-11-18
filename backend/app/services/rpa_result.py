"""RPA执行结果包领域服务。

职责边界（对应需求口径）：
1. 按 RPA 任务的动作序列与等待条件，解析每个执行批次的动作执行输出，归并等待条件结果，
   生成「RPA执行结果包」与「失败动作清单」。
2. 每个结果字段都能追溯到动作步骤（action_no/action_name）和执行批次（batch_no）；
   失败动作单独落清单，绝不被后续批次的成功结果覆盖。
3. 同一任务两个执行批次结果字段相同但动作完成情况不同（冲突）时支持两种取舍：
   - latest_converge：以最近完成批次生成当前结果包（current 收敛），前一失败批次保留并标注；
   - batch_expand：结果包按执行批次展开，多版本并存并保留失败差异。
4. 数据版本 data_version 单调递增，下游只处理 current=true 的当前版本；旧口径版本保留兼容。
5. 采集日志服务对接：同步落待处理清单、读取并返回核对状态、消费并把差异写回 RPA 任务、
   为失败动作生成关联任务日志。
6. 并发恢复：同一任务同一时刻只允许一个执行（重复启动拒绝），恢复时只放行一个中断任务。

实现为进程内内存仓储（与项目其他模块一致，仅依赖标准库），启动时由 seed 数据预热。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "rpa_result"

# 两种冲突取舍口径
POLICY_LATEST = "latest_converge"
POLICY_EXPAND = "batch_expand"
POLICIES = (POLICY_LATEST, POLICY_EXPAND)

# 批次/动作状态口径
BATCH_RUNNING = "执行中"
BATCH_DONE = "已完成"
BATCH_FAILED = "执行失败"
ACTION_OK = "成功"
ACTION_FAILED = "失败"
ACTION_SKIPPED = "未执行"

# 待处理清单与核对状态
PENDING_TODO = "待处理"
PENDING_DOING = "处理中"
PENDING_DONE = "已处理"
CHECK_UNCHECKED = "待核对"
CHECK_PASSED = "核对通过"
CHECK_DIFF = "存在差异"

# 旧口径兼容：下游声明版本时，低于当前数据版本的请求按旧口径只读返回
LEGACY_VERSION = "v0"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class RpaResultService:
    """结果包领域服务；所有写操作经同一把锁串行化，保证并发恢复只有一个任务继续。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data_version = 0
        # 任务级执行锁：task_id -> batch_no，存在即表示该任务有在途执行
        self._run_locks: dict[int, str] = {}
        # 中断待恢复登记：task_id -> {"batch_no", "resume_token"}
        self._interrupted: dict[int, dict[str, str]] = {}
        self._resume_token: str | None = None

    # ------------------------------------------------------------------
    # 内部仓储与版本
    # ------------------------------------------------------------------
    def _table(self) -> list[dict[str, Any]]:
        return store.rows(MODULE)

    def _next_data_version(self) -> str:
        self._data_version += 1
        return f"v{self._data_version}"

    def _packages_of(self, task_id: int) -> list[dict[str, Any]]:
        return [p for p in self._table() if int(p["task_id"]) == int(task_id)]

    def _find_package(self, package_id: str) -> dict[str, Any] | None:
        for package in self._table():
            if str(package.get("package_id")) == str(package_id):
                return package
        return None

    # ------------------------------------------------------------------
    # 解析引擎：动作序列 + 等待条件 + 批次输出 -> 结果包
    # ------------------------------------------------------------------
    def build_package(
        self,
        *,
        task: dict[str, Any],
        batch_no: str,
        batch_status: str,
        started_at: str,
        finished_at: str | None,
        action_outputs: list[dict[str, Any]],
        wait_outputs: list[dict[str, Any]] | None = None,
        conflict_policy: str = POLICY_LATEST,
    ) -> dict[str, Any]:
        """把一个执行批次的原始输出解析为结果包结构（不落库）。

        action_outputs 元素：{action_no, action_name, action_type, status, output_fields,
                              error_message, finished_at}
        wait_outputs 元素：{wait_no, action_no, condition, expected, actual, status, elapsed_ms}
        结果字段 output_fields 元素：{field_name, value}，解析后补全动作步骤与批次追溯。
        """
        if conflict_policy not in POLICIES:
            raise ValueError(f"冲突取舍口径 {conflict_policy} 不支持")
        wait_by_action: dict[int, list[dict[str, Any]]] = {}
        for wait in wait_outputs or []:
            wait_by_action.setdefault(int(wait["action_no"]), []).append(wait)

        steps: list[dict[str, Any]] = []
        result_fields: list[dict[str, Any]] = []
        failed_actions: list[dict[str, Any]] = []
        for raw in action_outputs:
            action_no = int(raw["action_no"])
            step_status = str(raw.get("status", ACTION_SKIPPED))
            waits = wait_by_action.get(action_no, [])
            # 等待条件归并到动作步骤：任一等待未满足即视为该步骤未完成
            wait_merged = "未配置等待" if not waits else (
                ACTION_OK if all(str(w.get("status")) == ACTION_OK for w in waits) else ACTION_FAILED
            )
            step = {
                "action_no": action_no,
                "action_name": str(raw.get("action_name", "")),
                "action_type": str(raw.get("action_type", "action")),
                "status": step_status,
                "wait_result": wait_merged,
                "waits": waits,
                "error_message": raw.get("error_message"),
                "finished_at": raw.get("finished_at"),
                # 追溯锚点：每个步骤都带任务、批次
                "trace": {"task_id": task["id"], "batch_no": batch_no, "action_no": action_no},
            }
            steps.append(step)

            # 结果字段只从真正成功且等待满足的步骤产出，避免未完成页面操作被当作有效产出
            step_complete = step_status == ACTION_OK and wait_merged in (ACTION_OK, "未配置等待")
            for field in raw.get("output_fields", []) or []:
                result_fields.append({
                    "field_name": str(field.get("field_name", "")),
                    "value": field.get("value"),
                    "valid": bool(step_complete),
                    "invalid_reason": None if step_complete else f"动作步骤{action_no}未完成（动作{step_status}/等待{wait_merged}）",
                    "action_no": action_no,
                    "action_name": step["action_name"],
                    "batch_no": batch_no,
                    "task_id": task["id"],
                })

            # 失败动作（含等待未满足）单独成清单，失败事实不随结果字段成败被覆盖
            if not step_complete:
                failed_actions.append({
                    "action_no": action_no,
                    "action_name": step["action_name"],
                    "batch_no": batch_no,
                    "task_id": task["id"],
                    "task_code": str(task.get("任务编号", "")),
                    "fail_reason": raw.get("error_message") or f"等待条件未满足（{wait_merged}）",
                    "wait_result": wait_merged,
                    "recorded_at": _now(),
                })

        package = {
            "package_id": f"RPACK-{task['id']}-{batch_no}",
            "task_id": task["id"],
            "task_code": str(task.get("任务编号", "")),
            "task_name": str(task.get("任务名称", "")),
            "batch_no": batch_no,
            "batch_status": batch_status,
            "conflict_policy": conflict_policy,
            "started_at": started_at,
            "finished_at": finished_at or _now(),
            "generated_at": _now(),
            "data_version": None,  # 落库时分配
            "current": False,
            "legacy_compatible": True,
            "action_total": len(steps),
            "action_done": sum(1 for s in steps if s["status"] == ACTION_OK),
            "action_failed": len(failed_actions),
            "fully_traced": True,  # 结果字段全部带动作步骤+执行批次追溯
            "steps": steps,
            "result_fields": result_fields,
            "failed_actions": failed_actions,
            "check_status": CHECK_UNCHECKED,
            "downstream_state": PENDING_TODO,
            "consumed": False,
        }
        package["package_ok"] = package["action_failed"] == 0 and batch_status == BATCH_DONE
        return package

    # ------------------------------------------------------------------
    # 结果包落库与冲突取舍
    # ------------------------------------------------------------------
    def save_package(self, package: dict[str, Any]) -> dict[str, Any]:
        """落库一个结果包，按冲突口径决定 current 收敛或批次展开。"""
        with self._lock:
            existing = self._packages_of(int(package["task_id"]))
            if any(p["package_id"] == package["package_id"] for p in existing):
                raise ValueError(f"执行批次 {package['batch_no']} 的结果包已存在，禁止重复生成")

            package["data_version"] = self._next_data_version()
            policy = package["conflict_policy"]

            if policy == POLICY_LATEST:
                # 最新完成批次收敛为唯一当前版本；历史批次保留 current=false 并标注完成情况
                for old in existing:
                    old["current"] = False
                    old["superseded_by"] = package["package_id"]
                package["current"] = True
            else:
                # 批次展开：每个结果包各自成版本；最近完成的作为当前版本，其余仍可独立核对
                latest = self._latest_finished(existing)
                for old in existing:
                    old["current"] = False
                package["current"] = latest is None or self._is_newer(package, latest)
                if not package["current"] and latest is not None:
                    package["superseded_by"] = latest["package_id"]

            self._table().append(package)
            self._mark_task_diff(package)
            return package

    @staticmethod
    def _is_newer(candidate: dict[str, Any], baseline: dict[str, Any]) -> bool:
        return str(candidate.get("finished_at", "")) >= str(baseline.get("finished_at", ""))

    def _latest_finished(self, packages: list[dict[str, Any]]) -> dict[str, Any] | None:
        finished = [p for p in packages if p.get("finished_at")]
        return max(finished, key=lambda p: str(p["finished_at"]), default=None)

    def _mark_task_diff(self, package: dict[str, Any]) -> None:
        """同任务多批次结果字段相同但动作完成情况不同 -> 在任务上登记 RPA 任务差异。"""
        task = store.find("robot_task", int(package["task_id"]))
        if task is None:
            return
        diffs = task.setdefault("result_diffs", [])
        for old in self._packages_of(int(package["task_id"])):
            if old["package_id"] == package["package_id"]:
                continue
            same_fields = self._same_field_signature(old, package)
            if same_fields and old["action_done"] != package["action_done"]:
                diffs.append({
                    "field_signature": "相同结果字段",
                    "batch_a": old["batch_no"],
                    "batch_b": package["batch_no"],
                    "action_done_a": old["action_done"],
                    "action_done_b": package["action_done"],
                    "policy": package["conflict_policy"],
                    "recorded_at": _now(),
                })

    @staticmethod
    def _same_field_signature(a: dict[str, Any], b: dict[str, Any]) -> bool:
        # 冲突口径是「两批次返回了相同结果字段」：按全部返回字段（含无效字段）比对，
        # 再由 action_done 差异判定动作完成情况不同，避免失败批次的字段被提前过滤掉。
        sig_a = sorted((f["field_name"], str(f.get("value"))) for f in a["result_fields"])
        sig_b = sorted((f["field_name"], str(f.get("value"))) for f in b["result_fields"])
        return bool(sig_a) and sig_a == sig_b

    # ------------------------------------------------------------------
    # 查询：概览 / 看板 / 明细 / 下载取舍
    # ------------------------------------------------------------------
    def list_packages(self, *, task_id: int | None = None, current_only: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            packages = self._table()
            if task_id is not None:
                packages = [p for p in packages if int(p["task_id"]) == int(task_id)]
            if current_only:
                packages = [p for p in packages if p["current"]]
            return sorted(packages, key=lambda p: (p["task_id"], p["finished_at"]), reverse=True)

    def current_package(self, task_id: int) -> dict[str, Any] | None:
        with self._lock:
            return next((p for p in self._packages_of(int(task_id)) if p["current"]), None)

    def get_package(self, package_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._find_package(package_id)

    def failed_actions(self, *, task_id: int | None = None, batch_no: str | None = None) -> list[dict[str, Any]]:
        """失败动作清单汇总：跨结果包抽取，失败事实独立保留，不被成功结果覆盖。"""
        with self._lock:
            items: list[dict[str, Any]] = []
            for package in self._table():
                if task_id is not None and int(package["task_id"]) != int(task_id):
                    continue
                for fail in package["failed_actions"]:
                    if batch_no is not None and str(fail["batch_no"]) != str(batch_no):
                        continue
                    row = dict(fail)
                    row["package_id"] = package["package_id"]
                    row["data_version"] = package["data_version"]
                    items.append(row)
            return items

    def overview(self) -> dict[str, Any]:
        """概览卡片 + 看板口径。"""
        with self._lock:
            packages = self._table()
            current = [p for p in packages if p["current"]]
            tasks = {p["task_id"] for p in packages}
            failed_count = sum(len(p["failed_actions"]) for p in packages)
            diff_tasks = sum(
                1 for task_id in tasks
                if store.find("robot_task", int(task_id)) and store.find("robot_task", int(task_id)).get("result_diffs")
            )
            pending = sum(1 for p in packages if p["downstream_state"] == PENDING_TODO)
            return {
                "cards": [
                    {"label": "结果包总数", "value": len(packages)},
                    {"label": "覆盖RPA任务", "value": len(tasks)},
                    {"label": "当前版本包", "value": len(current)},
                    {"label": "失败动作条数", "value": failed_count},
                    {"label": "存在差异任务", "value": diff_tasks},
                    {"label": "下游待处理", "value": pending},
                ],
                "data_version": f"v{self._data_version}",
            }

    def board(self) -> list[dict[str, Any]]:
        """看板：按任务聚合当前版本与历史批次，供页面列出 RPA 任务差异。"""
        with self._lock:
            board_rows: list[dict[str, Any]] = []
            for task_id in sorted({p["task_id"] for p in self._table()}):
                packages = self._packages_of(int(task_id))
                current = next((p for p in packages if p["current"]), None)
                task = store.find("robot_task", int(task_id))
                board_rows.append({
                    "task_id": int(task_id),
                    "task_code": current["task_code"] if current else "",
                    "task_name": current["task_name"] if current else "",
                    "package_count": len(packages),
                    "current_version": current["data_version"] if current else None,
                    "current_package_id": current["package_id"] if current else None,
                    "current_ok": current["package_ok"] if current else False,
                    "batch_nos": [p["batch_no"] for p in packages],
                    "failed_batch_nos": [p["batch_no"] for p in packages if p["failed_actions"]],
                    "failed_action_total": sum(len(p["failed_actions"]) for p in packages),
                    "has_diff": bool(task and task.get("result_diffs")),
                    "diffs": (task or {}).get("result_diffs", []),
                    "check_status": current["check_status"] if current else CHECK_UNCHECKED,
                    "downstream_state": current["downstream_state"] if current else PENDING_TODO,
                })
            return board_rows

    def downloadable_packages(self, task_id: int) -> dict[str, Any]:
        """处理边界：可下载核对的结果包取舍。

        latest_converge：只下载当前版本包 + 附失败批次标注；
        batch_expand：逐批次全部可下载，保留失败差异。两种口径都不破坏完整动作追溯。
        """
        with self._lock:
            packages = self._packages_of(int(task_id))
            current = next((p for p in packages if p["current"]), None)
            if current and current["conflict_policy"] == POLICY_LATEST:
                return {
                    "mode": POLICY_LATEST,
                    "download": [current["package_id"]],
                    "retained": [
                        {"package_id": p["package_id"], "batch_no": p["batch_no"],
                         "action_done": p["action_done"], "action_failed": p["action_failed"],
                         "note": "前一失败批次，仅留存核对，不作为下游当前版本"}
                        for p in packages if not p["current"]
                    ],
                }
            return {
                "mode": POLICY_EXPAND,
                "download": [p["package_id"] for p in packages],
                "retained": [],
            }

    # ------------------------------------------------------------------
    # 下游版本口径：只处理当前版本，以数据版本为准，旧口径兼容
    # ------------------------------------------------------------------
    def resolve_for_downstream(self, task_id: int, client_version: str | None) -> dict[str, Any]:
        with self._lock:
            current = self.current_package(int(task_id))
            if current is None:
                return {"ok": False, "message": f"RPA任务 {task_id} 暂无结果包", "current": None}
            legacy = bool(client_version and client_version != LEGACY_VERSION
                          and client_version != current["data_version"])
            return {
                "ok": True,
                "current": current,
                "data_version": current["data_version"],
                "client_version": client_version or LEGACY_VERSION,
                "legacy_mode": legacy,
                "message": "旧口径兼容只读返回" if legacy else "当前数据版本",
            }

    # ------------------------------------------------------------------
    # 采集日志服务对接
    # ------------------------------------------------------------------
    def sync_to_pending(self, *, task_id: int | None = None) -> dict[str, Any]:
        """采集日志服务同步读取结果包与失败动作清单，落到待处理清单。"""
        with self._lock:
            targets = [p for p in self._table()
                       if (task_id is None or int(p["task_id"]) == int(task_id))
                       and p["downstream_state"] == PENDING_TODO]
            pending_rows: list[dict[str, Any]] = []
            for package in targets:
                package["downstream_state"] = PENDING_DOING
                pending_rows.append({
                    "package_id": package["package_id"],
                    "task_id": package["task_id"],
                    "task_code": package["task_code"],
                    "batch_no": package["batch_no"],
                    "data_version": package["data_version"],
                    "current": package["current"],
                    "failed_action_count": len(package["failed_actions"]),
                    "state": PENDING_DOING,
                    "synced_at": _now(),
                })
            # 待处理清单持久化在独立表，供采集日志服务分页读取
            store.rows("rpa_result_pending").extend(pending_rows)
            return {"ok": True, "synced": len(pending_rows), "items": pending_rows}

    def pending_list(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(store.rows("rpa_result_pending"))

    def check_package(self, package_id: str) -> dict[str, Any]:
        """另一个入口：采集日志服务读取结果包并返回核对状态。"""
        with self._lock:
            package = self._find_package(package_id)
            if package is None:
                return {"ok": False, "message": f"结果包 {package_id} 不存在", "check_status": None}
            package["check_status"] = CHECK_DIFF if package["failed_actions"] else CHECK_PASSED
            return {
                "ok": True,
                "package_id": package_id,
                "check_status": package["check_status"],
                "data_version": package["data_version"],
                "current": package["current"],
                "failed_actions": package["failed_actions"],
                "trace_complete": package["fully_traced"],
            }

    def consume_package(self, package_id: str) -> dict[str, Any]:
        """采集日志服务消费结果包与失败动作清单，把差异写回 RPA 任务并生成关联任务日志。"""
        with self._lock:
            package = self._find_package(package_id)
            if package is None:
                return {"ok": False, "message": f"结果包 {package_id} 不存在"}
            if package["consumed"]:
                return {"ok": False, "message": "结果包已被消费，禁止重复处理"}
            package["consumed"] = True
            package["downstream_state"] = PENDING_DONE

            # 差异写回 RPA 任务
            task = store.find("robot_task", int(package["task_id"]))
            if task is not None:
                task.setdefault("consumed_packages", []).append({
                    "package_id": package_id,
                    "batch_no": package["batch_no"],
                    "data_version": package["data_version"],
                    "failed_action_count": len(package["failed_actions"]),
                    "consumed_at": _now(),
                })

            # 为每个失败动作生成关联任务日志，落到采集日志模块待处理
            logs = store.rows("log_viewer")
            created_logs: list[dict[str, Any]] = []
            for fail in package["failed_actions"]:
                log_id = max((int(r.get("id", 0)) for r in logs), default=0) + len(created_logs) + 1
                entry = {
                    "id": log_id,
                    "status": "未处理",
                    "pending": True,
                    "abnormal": True,
                    "日志编号": f"LOG-RPA-{package['batch_no']}-{fail['action_no']}",
                    "关联任务": package["task_code"],
                    "日志级别": "ERROR",
                    "记录时间": _now(),
                    "日志内容": (
                        f"RPA结果包{package_id}批次{package['batch_no']}"
                        f"动作{fail['action_no']}({fail['action_name']})失败：{fail['fail_reason']}"
                    ),
                    "错误堆栈": fail["fail_reason"],
                    "处理状态": "未处理",
                    "处理人": "",
                    # 批次回查锚点
                    "执行批次": package["batch_no"],
                    "结果包编号": package_id,
                    "动作步骤": fail["action_no"],
                }
                logs.append(entry)
                created_logs.append(entry)

            for row in store.rows("rpa_result_pending"):
                if row["package_id"] == package_id:
                    row["state"] = PENDING_DONE
            return {"ok": True, "package_id": package_id, "logs_created": len(created_logs),
                    "diff_written": task is not None, "log_ids": [log["id"] for log in created_logs]}

    # ------------------------------------------------------------------
    # 并发恢复：重复启动拒绝，恢复仅一个任务继续
    # ------------------------------------------------------------------
    def start_execution(self, task_id: int, batch_no: str) -> dict[str, Any]:
        """同一任务在途执行时拒绝重复启动。"""
        with self._lock:
            held = self._run_locks.get(int(task_id))
            if held is not None:
                return {"ok": False,
                        "message": f"RPA任务 {task_id} 已有在途执行批次 {held}，重复启动已拒绝"}
            self._run_locks[int(task_id)] = batch_no
            return {"ok": True, "message": f"RPA任务 {task_id} 批次 {batch_no} 已取得执行锁"}

    def interrupt_execution(self, task_id: int) -> dict[str, Any]:
        """登记中断，等待恢复；不立即释放锁，避免并发实例抢入。"""
        with self._lock:
            batch_no = self._run_locks.get(int(task_id))
            if batch_no is None:
                return {"ok": False, "message": f"RPA任务 {task_id} 没有在途执行，无需中断登记"}
            self._interrupted[int(task_id)] = {"batch_no": batch_no, "resume_token": f"RESUME-{task_id}-{batch_no}"}
            return {"ok": True, "message": f"RPA任务 {task_id} 批次 {batch_no} 已登记中断待恢复"}

    def recover_one(self) -> dict[str, Any]:
        """并发恢复：只放行一个中断任务继续，其余中断保持等待；同一时刻恢复令牌唯一。"""
        with self._lock:
            if self._resume_token is not None:
                return {"ok": False, "message": f"已有恢复令牌 {self._resume_token} 在执行，本次恢复拒绝"}
            if not self._interrupted:
                return {"ok": False, "message": "没有待恢复的中断任务"}
            task_id, info = sorted(self._interrupted.items())[0]
            self._interrupted.pop(task_id)
            self._run_locks[task_id] = info["batch_no"]
            self._resume_token = info["resume_token"]
            return {"ok": True, "task_id": task_id, "batch_no": info["batch_no"],
                    "resume_token": self._resume_token, "message": "仅一个任务被放行继续执行"}

    def finish_execution(self, task_id: int) -> dict[str, Any]:
        with self._lock:
            self._run_locks.pop(int(task_id), None)
            if self._resume_token and self._resume_token.startswith(f"RESUME-{task_id}-"):
                self._resume_token = None
            return {"ok": True, "message": f"RPA任务 {task_id} 执行锁已释放"}


# ----------------------------------------------------------------------
# 种子预热：构造可直接演示的结果包，覆盖冲突收敛/批次展开两种口径
# ----------------------------------------------------------------------
def seed_warmup() -> None:
    """启动时基于 robot_task 种子数据生成示例结果包。

    - 任务1：两批次结果字段相同但动作完成情况不同，按 latest_converge 收敛到最新完成批次，
      前一失败批次保留并标注；
    - 任务3：两批次按 batch_expand 展开，失败差异逐版本保留。
    """
    if service._table():
        return
    store.rows("rpa_result_pending")

    task1 = store.find("robot_task", 1)
    task3 = store.find("robot_task", 3)
    if task1 is None or task3 is None:
        return

    # 任务1 · 前一批次：提交动作失败，相同结果字段被标记为无效（动作未完成）
    first = service.build_package(
        task=task1, batch_no="B20261108-01", batch_status=BATCH_FAILED,
        started_at="2026-11-08T09:00:00", finished_at="2026-11-08T09:02:10",
        conflict_policy=POLICY_LATEST,
        action_outputs=[
            {"action_no": 1, "action_name": "打开页面", "action_type": "open", "status": ACTION_OK,
             "output_fields": [], "finished_at": "2026-11-08T09:00:20"},
            {"action_no": 2, "action_name": "填写表单", "action_type": "input", "status": ACTION_OK,
             "output_fields": [{"field_name": "客户名称", "value": "华北商贸"}],
             "finished_at": "2026-11-08T09:01:00"},
            {"action_no": 3, "action_name": "提交订单", "action_type": "click", "status": ACTION_FAILED,
             "output_fields": [{"field_name": "订单号", "value": "ORD-7788"},
                               {"field_name": "回执状态", "value": "待确认"}],
             "error_message": "提交按钮点击后接口超时", "finished_at": "2026-11-08T09:02:10"},
            {"action_no": 4, "action_name": "确认回执", "action_type": "wait", "status": ACTION_SKIPPED,
             "output_fields": [], "error_message": "前置动作失败，步骤未执行"},
        ],
        wait_outputs=[
            {"wait_no": 1, "action_no": 4, "condition": "回执弹窗可见", "expected": "可见",
             "actual": "未出现", "status": ACTION_FAILED, "elapsed_ms": 30000},
        ],
    )
    service.save_package(first)

    # 任务1 · 最新完成批次：相同结果字段，但动作全部完成 -> 当前版本收敛
    latest = service.build_package(
        task=task1, batch_no="B20261110-01", batch_status=BATCH_DONE,
        started_at="2026-11-10T09:00:00", finished_at="2026-11-10T09:01:40",
        conflict_policy=POLICY_LATEST,
        action_outputs=[
            {"action_no": 1, "action_name": "打开页面", "action_type": "open", "status": ACTION_OK,
             "output_fields": [], "finished_at": "2026-11-10T09:00:18"},
            {"action_no": 2, "action_name": "填写表单", "action_type": "input", "status": ACTION_OK,
             "output_fields": [{"field_name": "客户名称", "value": "华北商贸"}],
             "finished_at": "2026-11-10T09:00:58"},
            {"action_no": 3, "action_name": "提交订单", "action_type": "click", "status": ACTION_OK,
             "output_fields": [{"field_name": "订单号", "value": "ORD-7788"},
                               {"field_name": "回执状态", "value": "待确认"}],
             "finished_at": "2026-11-10T09:01:20"},
            {"action_no": 4, "action_name": "确认回执", "action_type": "wait", "status": ACTION_OK,
             "output_fields": [],
             "finished_at": "2026-11-10T09:01:40"},
        ],
        wait_outputs=[
            {"wait_no": 1, "action_no": 4, "condition": "回执弹窗可见", "expected": "可见",
             "actual": "可见", "status": ACTION_OK, "elapsed_ms": 1200},
        ],
    )
    service.save_package(latest)

    # 任务3 · 批次展开：前一批次校验失败，后一批次成功；两个版本都保留
    failed_batch = service.build_package(
        task=task3, batch_no="B20261114-01", batch_status=BATCH_FAILED,
        started_at="2026-11-14T14:00:00", finished_at="2026-11-14T14:03:00",
        conflict_policy=POLICY_EXPAND,
        action_outputs=[
            {"action_no": 1, "action_name": "打开同步页", "action_type": "open", "status": ACTION_OK,
             "output_fields": [], "finished_at": "2026-11-14T14:00:20"},
            {"action_no": 2, "action_name": "勾选客户", "action_type": "check", "status": ACTION_OK,
             "output_fields": [{"field_name": "同步客户数", "value": 120}],
             "finished_at": "2026-11-14T14:01:00"},
            {"action_no": 3, "action_name": "触发同步", "action_type": "click", "status": ACTION_OK,
             "output_fields": [{"field_name": "成功数", "value": 118}],
             "finished_at": "2026-11-14T14:02:00"},
            {"action_no": 4, "action_name": "校验结果", "action_type": "validate", "status": ACTION_FAILED,
             "output_fields": [{"field_name": "失败数", "value": 2}],
             "error_message": "成功数+失败数与同步客户数不一致",
             "finished_at": "2026-11-14T14:03:00"},
        ],
        wait_outputs=[
            {"wait_no": 1, "action_no": 3, "condition": "成功提示出现", "expected": "出现",
             "actual": "出现", "status": ACTION_OK, "elapsed_ms": 2200},
        ],
    )
    service.save_package(failed_batch)

    ok_batch = service.build_package(
        task=task3, batch_no="B20261115-02", batch_status=BATCH_DONE,
        started_at="2026-11-15T14:00:00", finished_at="2026-11-15T14:02:40",
        conflict_policy=POLICY_EXPAND,
        action_outputs=[
            {"action_no": 1, "action_name": "打开同步页", "action_type": "open", "status": ACTION_OK,
             "output_fields": [], "finished_at": "2026-11-15T14:00:18"},
            {"action_no": 2, "action_name": "勾选客户", "action_type": "check", "status": ACTION_OK,
             "output_fields": [{"field_name": "同步客户数", "value": 120}],
             "finished_at": "2026-11-15T14:00:58"},
            {"action_no": 3, "action_name": "触发同步", "action_type": "click", "status": ACTION_OK,
             "output_fields": [{"field_name": "成功数", "value": 120}],
             "finished_at": "2026-11-15T14:01:50"},
            {"action_no": 4, "action_name": "校验结果", "action_type": "validate", "status": ACTION_OK,
             "output_fields": [{"field_name": "失败数", "value": 0}],
             "finished_at": "2026-11-15T14:02:40"},
        ],
        wait_outputs=[
            {"wait_no": 1, "action_no": 3, "condition": "成功提示出现", "expected": "出现",
             "actual": "出现", "status": ACTION_OK, "elapsed_ms": 1800},
        ],
    )
    service.save_package(ok_batch)


service = RpaResultService()
seed_warmup()
