"""RPA执行结果包接口。

对外能力：
- 生成结果包（动作序列+等待条件+批次输出解析，含冲突取舍口径）
- 概览/看板/明细/失败动作清单/差异查询
- 当前版本解析（下游只处理当前版本，旧口径兼容）
- 下载结果包（JSON 核对包与 CSV 明细）
- 采集日志服务对接：同步待处理、核对状态、消费写回
- 并发恢复：启动锁、中断登记、单任务恢复
"""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.rpa_result import (
    ACTION_OK,
    BATCH_DONE,
    LEGACY_VERSION,
    POLICIES,
    POLICY_LATEST,
    service,
)
from app.store import store

router = APIRouter(prefix="/api/rpa_result", tags=["RPA执行结果包"])


class WaitOutput(BaseModel):
    wait_no: int
    action_no: int
    condition: str = ""
    expected: str | None = None
    actual: str | None = None
    status: str = ACTION_OK
    elapsed_ms: int | None = None


class ActionOutput(BaseModel):
    action_no: int
    action_name: str
    action_type: str = "action"
    status: str
    output_fields: list[dict[str, Any]] = Field(default_factory=list)
    error_message: str | None = None
    finished_at: str | None = None


class BuildPayload(BaseModel):
    task_id: int
    batch_no: str
    batch_status: str = BATCH_DONE
    started_at: str
    finished_at: str | None = None
    conflict_policy: str = POLICY_LATEST
    action_outputs: list[ActionOutput]
    wait_outputs: list[WaitOutput] | None = None


def _require_task(task_id: int) -> dict[str, Any]:
    task = store.find("robot_task", task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"RPA任务 {task_id} 不存在")
    return task


@router.get("/overview")
def overview() -> dict[str, Any]:
    """概览卡片与当前数据版本。"""
    return service.overview()


@router.get("/board")
def board() -> dict[str, Any]:
    """看板：按任务聚合当前版本、失败批次与 RPA 任务差异。"""
    return {"items": service.board()}


@router.get("")
def list_packages(
    task_id: int | None = Query(default=None),
    current_only: bool = Query(default=False),
) -> dict[str, Any]:
    """结果包明细列表，可按任务或仅当前版本过滤。"""
    return {"items": service.list_packages(task_id=task_id, current_only=current_only)}


@router.get("/failed-actions")
def failed_actions(
    task_id: int | None = Query(default=None),
    batch_no: str | None = Query(default=None),
) -> dict[str, Any]:
    """失败动作清单：失败事实独立保留，可按任务/批次回查。"""
    return {"items": service.failed_actions(task_id=task_id, batch_no=batch_no)}


@router.get("/tasks/{task_id}/download-scope")
def download_scope(task_id: int) -> dict[str, Any]:
    """可下载核对的结果包取舍（收敛/展开两种边界）。"""
    _require_task(task_id)
    return service.downloadable_packages(task_id)


@router.get("/tasks/{task_id}/current")
def current_for_downstream(
    task_id: int,
    client_version: str | None = Query(default=LEGACY_VERSION, description="下游持有的数据版本"),
) -> dict[str, Any]:
    """下游只处理当前版本：以数据版本为准判定，旧口径兼容只读返回。"""
    _require_task(task_id)
    result = service.resolve_for_downstream(task_id, client_version)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.post("/packages")
def build_package(payload: BuildPayload) -> dict[str, Any]:
    """解析动作执行输出并归并等待条件，生成 RPA 执行结果包。"""
    if payload.conflict_policy not in POLICIES:
        raise HTTPException(status_code=400, detail=f"冲突口径仅支持 {POLICIES}")
    task = _require_task(payload.task_id)
    try:
        package = service.build_package(
            task=task,
            batch_no=payload.batch_no,
            batch_status=payload.batch_status,
            started_at=payload.started_at,
            finished_at=payload.finished_at,
            action_outputs=[item.model_dump() for item in payload.action_outputs],
            wait_outputs=[item.model_dump() for item in payload.wait_outputs] if payload.wait_outputs else None,
            conflict_policy=payload.conflict_policy,
        )
        saved = service.save_package(package)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, "message": "RPA执行结果包已生成", "package": saved}


@router.get("/packages/{package_id}")
def get_package(package_id: str) -> dict[str, Any]:
    package = service.get_package(package_id)
    if package is None:
        raise HTTPException(status_code=404, detail=f"结果包 {package_id} 不存在")
    return package


@router.get("/packages/{package_id}/check")
def check_package(package_id: str) -> dict[str, Any]:
    """采集日志服务读取结果包并返回核对状态。"""
    result = service.check_package(package_id)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.post("/packages/{package_id}/consume")
def consume_package(package_id: str) -> dict[str, Any]:
    """采集日志服务消费结果包：差异写回 RPA 任务，失败动作生成关联任务日志。"""
    result = service.consume_package(package_id)
    if not result["ok"]:
        raise HTTPException(status_code=409, detail=result["message"])
    return result


@router.get("/packages/{package_id}/download")
def download_package(package_id: str, fmt: str = Query(default="json")) -> StreamingResponse:
    """下载结果包用于人工/下游核对：JSON 完整追溯包或 CSV 结果字段明细。"""
    package = service.get_package(package_id)
    if package is None:
        raise HTTPException(status_code=404, detail=f"结果包 {package_id} 不存在")

    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(["结果字段", "字段值", "是否有效", "动作步骤", "动作名称", "执行批次", "无效原因"])
        for field in package["result_fields"]:
            writer.writerow([
                field["field_name"], field.get("value"), "是" if field["valid"] else "否",
                field["action_no"], field["action_name"], field["batch_no"],
                field.get("invalid_reason") or "",
            ])
        for fail in package["failed_actions"]:
            writer.writerow([
                f"[失败动作]{fail['action_name']}", "", "否", fail["action_no"],
                fail["action_name"], fail["batch_no"], fail["fail_reason"],
            ])
        return StreamingResponse(
            iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{package_id}.csv"'},
        )

    payload = json.dumps(package, ensure_ascii=False, indent=2)
    return StreamingResponse(
        iter([payload]), media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{package_id}.json"'},
    )


# ----------------------------------------------------------------------
# 采集日志服务：同步待处理清单
# ----------------------------------------------------------------------
@router.post("/sync-pending")
def sync_pending(task_id: int | None = Query(default=None)) -> dict[str, Any]:
    """采集日志服务同步读取结果包与失败动作清单，落到待处理清单。"""
    return service.sync_to_pending(task_id=task_id)


@router.get("/pending")
def pending_list() -> dict[str, Any]:
    return {"items": service.pending_list()}


# ----------------------------------------------------------------------
# 并发恢复
# ----------------------------------------------------------------------
class ExecutionPayload(BaseModel):
    task_id: int
    batch_no: str


@router.post("/executions/start")
def start_execution(payload: ExecutionPayload) -> dict[str, Any]:
    """同一任务重复启动直接拒绝。"""
    _require_task(payload.task_id)
    result = service.start_execution(payload.task_id, payload.batch_no)
    if not result["ok"]:
        raise HTTPException(status_code=409, detail=result["message"])
    return result


@router.post("/executions/{task_id}/interrupt")
def interrupt_execution(task_id: int) -> dict[str, Any]:
    result = service.interrupt_execution(task_id)
    if not result["ok"]:
        raise HTTPException(status_code=409, detail=result["message"])
    return result


@router.post("/executions/recover")
def recover_one() -> dict[str, Any]:
    """并发恢复：仅一个中断任务被放行继续。"""
    result = service.recover_one()
    if not result["ok"]:
        raise HTTPException(status_code=409, detail=result["message"])
    return result


@router.post("/executions/{task_id}/finish")
def finish_execution(task_id: int) -> dict[str, Any]:
    return service.finish_execution(task_id)
