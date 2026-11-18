"""RPA任务编排 时间线服务：生成作业时间序列和示例数据。"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.services.rpa_result import service as rpa_result_service
from app.store import store

MODULE = "robot_task"
REQUIRED_FIELDS = ["任务编号", "任务名称", "页面定位器"]
STATUS_ORDER = ["待执行", "执行中", "已完成", "执行失败"]
ACTION_RULES = {"开始执行": "执行中", "暂停任务": "待执行", "标记失败": "执行失败"}
NEGATIVE_ACTIONS = []


class RobotTaskService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"RPA任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于RPA任务编排可执行范围"
        target = ACTION_RULES[action]
        if action == "开始执行":
            # 并发边界：同一任务在途执行时拒绝重复启动
            batch_no = str(entry.get("执行批次") or f"B-{entry_id}")
            lock = rpa_result_service.start_execution(entry_id, batch_no)
            if not lock["ok"]:
                return None, lock["message"]
        elif entry.get("status") == "执行中":
            # 离开执行中即释放任务级执行锁
            rpa_result_service.finish_execution(entry_id)
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"RPA任务已{action}"

    def recover_interrupted(self) -> tuple[dict[str, Any] | None, str]:
        """并发恢复：同一时刻只放行一个中断任务继续，重复恢复会被拒绝。"""
        result = rpa_result_service.recover_one()
        if not result["ok"]:
            return None, result["message"]
        entry = store.find(MODULE, int(result["task_id"]))
        if entry is not None:
            entry["status"] = "执行中"
            entry["pending"] = True
        return entry, f"任务 {result['task_id']} 已恢复执行（恢复令牌 {result['resume_token']}）"
