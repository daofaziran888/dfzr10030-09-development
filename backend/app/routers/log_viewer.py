"""采集日志接口：维护日志条目，覆盖开始处理、标记已处理、忽略日志等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.log_viewer import LogViewerService
from app.services.rpa_result import service as rpa_result_service

router = APIRouter(prefix="/api/log_viewer", tags=["采集日志"])

service = LogViewerService()

LIST_FIELDS = ["日志编号", "关联任务", "日志级别", "记录时间", "日志内容", "错误堆栈", "处理状态", "处理人"]
STATUSES = ["未处理", "处理中", "已处理", "已忽略"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按日志编号检索"),
    status: str | None = Query(default=None, description="未处理、处理中、已处理、已忽略"),
    batch_no: str | None = Query(default=None, description="按RPA执行批次回查关联任务日志"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、日志编号关键字、执行批次与状态过滤采集日志列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status,
        batch_no=batch_no, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/result-packages/pending")
def result_package_pending() -> dict:
    """采集日志服务读取结果包待处理清单（由同步接口落库）。"""
    return {"items": rpa_result_service.pending_list()}


@router.post("/result-packages/sync")
def sync_result_packages(task_id: int | None = Query(default=None)) -> dict:
    """采集日志服务同步读取RPA执行结果包及失败动作清单并落到待处理清单。"""
    return rpa_result_service.sync_to_pending(task_id=task_id)


@router.get("/result-packages/{package_id}/check")
def check_result_package(package_id: str) -> dict:
    """另一个入口：采集日志服务读取结果包并返回核对状态。"""
    result = rpa_result_service.check_package(package_id)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.post("/result-packages/{package_id}/consume")
def consume_result_package(package_id: str) -> dict:
    """采集日志服务消费结果包及失败动作清单，差异写回RPA任务并生成关联任务日志。"""
    result = rpa_result_service.consume_package(package_id)
    if not result["ok"]:
        raise HTTPException(status_code=409, detail=result["message"])
    return result


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    batch_no: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status,
        batch_no=batch_no, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="log_viewer.csv"'})


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条日志条目明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"日志条目 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条日志条目，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="日志条目已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条日志条目执行开始处理、标记已处理、忽略日志；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
