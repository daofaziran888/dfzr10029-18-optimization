"""数据快照 时间线接口：作业时间序列、分片复用清单校验与导出。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, ValidatePayload
from app.services.snapshot import SnapshotService

router = APIRouter(prefix="/api/snapshot", tags=["数据快照"])
service = SnapshotService()
LIST_FIELDS = ["快照编号", "数据集名称", "快照时间", "行数快照", "校验哈希", "快照策略", "存储位置", "快照状态"]
STATUSES = ["待执行", "执行中", "已暂停", "已完成", "执行失败"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["id", "name", "start", "end", "status", "checkpoint_version", "done_shards", "total_shards"])
    for item in items:
        writer.writerow([
            csv_cell(item.get("id")),
            csv_cell(item.get("name")),
            csv_cell(item.get("start")),
            csv_cell(item.get("end")),
            csv_cell(item.get("status")),
            csv_cell((item.get("checkpoint") or {}).get("version")),
            csv_cell((item.get("progress") or {}).get("done")),
            csv_cell((item.get("progress") or {}).get("total")),
        ])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=snapshot.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"快照记录 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="快照记录已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action, payload.expected_version)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.post("/{entry_id}/validate")
def validate_entry(entry_id: int, payload: ValidatePayload) -> ActionResult:
    """快照校验接口：消费并持久化分片复用清单，核对主版本边界。"""
    manifest, message = service.validate(entry_id, payload.version)
    if manifest is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=manifest)


@router.get("/{entry_id}/reuse-manifest")
def get_reuse_manifest(entry_id: int) -> dict[str, Any]:
    manifest = service.get_reuse_manifest(entry_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"快照记录 {entry_id} 不存在")
    return manifest


@router.get("/{entry_id}/reuse-manifest/export")
def export_reuse_manifest(entry_id: int) -> StreamingResponse:
    """导出分片复用清单：清单头 + 复用分片明细。"""
    manifest = service.get_reuse_manifest(entry_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"快照记录 {entry_id} 不存在")
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["快照编号", "清单主版本", "阶段", "核对状态", "来源段"])
    writer.writerow([
        csv_cell(manifest.get("快照编号")),
        csv_cell(manifest.get("version")),
        csv_cell(manifest.get("stage")),
        csv_cell(manifest.get("status")),
        csv_cell(manifest.get("source_segment")),
    ])
    writer.writerow([])
    writer.writerow(["分片编号", "来源段", "复用时间"])
    for item in manifest.get("items", []):
        writer.writerow([csv_cell(item.get("shard_id")), csv_cell(item.get("origin")), csv_cell(item.get("reused_at"))])
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=snapshot-{entry_id}-reuse-manifest.csv"},
    )
