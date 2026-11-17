"""数据快照 时间线服务：分片状态、恢复检查点与分片复用清单。

针对「大快照暂停后再次启动，已完成分片仍被重新扫描、时间线出现多个模糊完成时间」：
- 输入为快照分片状态与暂停恢复记录，先清洗现有输入（去重、补齐字段），
  再按恢复检查点推进快照完成状态，形成数据快照完成时间线；
- 分片完成顺序和快照最终状态共享同一份恢复检查点，同一分片只出现一次完成记录；
- 恢复时已完成分片记入分片复用清单（只复用、不重扫），只重扫未完成分片；
- 分片复用清单只在当前阶段完成后推进状态，恢复和归档回到对应节点；
  回退后重新打开清单，并可追溯同一段来源；
- 快照校验接口消费并持久化分片复用清单，核对主版本边界，冲突结果等待核对；
- 并发处理按检查点主版本做乐观控制，最终只落一个有效版本。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Any

from app.store import store

MODULE = "snapshot"
REQUIRED_FIELDS = ["快照编号", "数据集名称", "快照时间"]
STATUS_ORDER = ["待执行", "执行中", "已暂停", "已完成", "执行失败"]
ACTION_RULES = {
    "执行快照": "执行中",
    "恢复快照": "执行中",
    "暂停快照": "已暂停",
    "回退快照": "已暂停",
    "归档快照": "已完成",
    "标记失败": "执行失败",
}
NEGATIVE_ACTIONS = ["标记失败"]
RUN_ACTIONS = {"执行快照", "恢复快照"}
SHARD_TODO = "待扫描"
SHARD_DONE = "已完成"
MANIFEST_STAGES = ["采集", "校验", "归档"]
_LOCK = threading.RLock()


def _now() -> datetime:
    return datetime.now()


def _iso(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def _parse_time(value: Any) -> datetime:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return datetime(2026, 1, 1, 9, 0, 0)


class SnapshotService:
    """数据快照的分片级状态流转：所有状态修改只发生在本服务内。"""

    # ---- 输入清洗：把既有记录整理成分片状态 + 恢复检查点 + 复用清单 ----
    def _ensure_state(self, entry: dict[str, Any]) -> dict[str, Any]:
        entry_id = int(entry.get("id", 0) or 0)
        snap_no = str(entry.get("快照编号") or f"SNAP-{entry_id:04d}")
        shards = entry.get("shards")
        if not isinstance(shards, list) or not shards:
            shards = self._build_shards(entry, snap_no)
        cleaned: list[dict[str, Any]] = []
        seen: set[str] = set()
        for n, shard in enumerate(shards, start=1):
            sid = str(shard.get("shard_id") or f"{snap_no}-SH-{n:02d}")
            if sid in seen:
                continue  # 清洗：同一分片只保留一条状态
            seen.add(sid)
            cleaned.append({
                "shard_id": sid,
                "state": str(shard.get("state") or SHARD_TODO),
                "seq": shard.get("seq"),
                "completed_at": shard.get("completed_at"),
                "scan_count": int(shard.get("scan_count") or 0),
                "reused": bool(shard.get("reused")),
            })
        entry["shards"] = cleaned
        # 清洗完成时间线：同一分片只出现一次完成记录，按完成序号排序
        timeline = entry.get("completion_timeline")
        if not isinstance(timeline, list):
            timeline = [
                {"shard_id": s["shard_id"], "seq": s["seq"], "completed_at": s["completed_at"]}
                for s in cleaned
                if s["state"] == SHARD_DONE and s.get("seq") is not None
            ]
        dedup: dict[str, dict[str, Any]] = {}
        for rec in timeline:
            sid = str(rec.get("shard_id") or "")
            if sid and sid in seen and sid not in dedup:
                dedup[sid] = {"shard_id": sid, "seq": rec.get("seq"), "completed_at": rec.get("completed_at")}
        entry["completion_timeline"] = sorted(
            dedup.values(), key=lambda r: (r.get("seq") is None, r.get("seq") or 0)
        )
        # 恢复检查点：分片完成顺序和快照最终状态共享这一份
        checkpoint = entry.get("checkpoint")
        if not isinstance(checkpoint, dict):
            checkpoint = {"version": 1, "completed": [], "last_seq": 0, "updated_at": None}
        if not isinstance(checkpoint.get("completed"), list):
            checkpoint["completed"] = []
        checkpoint["completed"] = [sid for sid in checkpoint["completed"] if sid in seen]
        if not checkpoint["completed"]:
            checkpoint["completed"] = [s["shard_id"] for s in cleaned if s["state"] == SHARD_DONE]
        checkpoint["version"] = int(checkpoint.get("version") or 1)
        checkpoint["last_seq"] = int(
            checkpoint.get("last_seq")
            or max((int(s["seq"] or 0) for s in cleaned), default=0)
        )
        entry["checkpoint"] = checkpoint
        # 暂停恢复记录
        if not isinstance(entry.get("resume_log"), list):
            entry["resume_log"] = []
        # 分片复用清单：保留处理输入，持久化在快照记录上
        manifest = entry.get("reuse_manifest")
        if not isinstance(manifest, dict):
            manifest = {
                "version": checkpoint["version"],
                "stage": MANIFEST_STAGES[1] if entry.get("status") == "已完成" else MANIFEST_STAGES[0],
                "status": "待核对",
                "source_segment": f"{snap_no}/seg-0",
                "items": [],
                "updated_at": None,
            }
        if not isinstance(manifest.get("items"), list):
            manifest["items"] = []
        manifest.setdefault("source_segment", f"{snap_no}/seg-0")
        manifest.setdefault("stage", MANIFEST_STAGES[0])
        manifest.setdefault("status", "待核对")
        manifest["version"] = int(manifest.get("version") or checkpoint["version"])
        entry["reuse_manifest"] = manifest
        # 时间线边界：按完成时间推进
        entry.setdefault("started_at", _iso(_parse_time(entry.get("快照时间"))))
        if entry["completion_timeline"] and entry.get("status") == "已完成":
            entry.setdefault("finished_at", entry["completion_timeline"][-1]["completed_at"])
        return entry

    def _build_shards(self, entry: dict[str, Any], snap_no: str) -> list[dict[str, Any]]:
        """按既有状态回填分片进度：已完成→全部完成；执行中→前一半完成；其余→未开始。"""
        entry_id = int(entry.get("id", 0) or 0)
        base = _parse_time(entry.get("快照时间"))
        count = 3 + entry_id % 3
        status = str(entry.get("status") or STATUS_ORDER[0])
        done = count if status == "已完成" else (count // 2 if status == "执行中" else 0)
        shards: list[dict[str, Any]] = []
        for n in range(1, count + 1):
            completed = n <= done
            shards.append({
                "shard_id": f"{snap_no}-SH-{n:02d}",
                "state": SHARD_DONE if completed else SHARD_TODO,
                "seq": n if completed else None,
                "completed_at": _iso(base + timedelta(minutes=15 * n)) if completed else None,
                "scan_count": 1 if completed else 0,
                "reused": False,
            })
        return shards

    # ---- 时间线视图：按唯一完成顺序展示 ----
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        items: list[dict] = []
        for i, row in enumerate(rows):
            self._ensure_state(row)
            timeline = row["completion_timeline"]
            start = str(row.get("started_at") or "")
            end = str(row.get("finished_at") or (timeline[-1]["completed_at"] if timeline else start))
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get("数据集名称") or row.get("快照编号") or ""),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[0])),
                "progress": {"done": len(row["checkpoint"]["completed"]), "total": len(row["shards"])},
                "checkpoint": row["checkpoint"],
                "shards": row["shards"],
                "completion_timeline": timeline,
                "reuse_manifest": row["reuse_manifest"],
                "resume_log": row["resume_log"],
                "logs": self._render_logs(row),
            })
        return items

    def _render_logs(self, row: dict[str, Any]) -> str:
        events: list[tuple[str, str]] = [(str(row.get("started_at") or ""), f"作业启动：{row.get('快照编号', '')}")]
        for rec in row["completion_timeline"]:
            events.append((str(rec.get("completed_at") or ""), f"分片 {rec.get('shard_id')} 完成（序号 {rec.get('seq')}）"))
        for ev in row["resume_log"]:
            events.append((str(ev.get("at") or ""), str(ev.get("text") or ev.get("event") or "")))
        if row.get("status") == "已完成" and row.get("finished_at"):
            events.append((str(row["finished_at"]), "任务完成"))
        events.sort(key=lambda e: e[0])
        return "\n".join(f"[{t}] {text}" for t, text in events if text)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return self._ensure_state(entry)

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
        self._ensure_state(entry)
        return entry, []

    # ---- 状态动作：恢复检查、暂停、回退、归档、失败 ----
    def run_action(self, entry_id: int, action: str, expected_version: int | None = None) -> tuple[dict[str, Any] | None, str]:
        with _LOCK:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"快照记录 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于数据快照可执行范围"
            self._ensure_state(entry)
            if action in RUN_ACTIONS:
                return self._run(entry, action, expected_version)
            if action == "暂停快照":
                return self._pause(entry)
            if action == "回退快照":
                return self._rollback(entry)
            if action == "归档快照":
                return self._archive(entry)
            entry["status"] = ACTION_RULES[action]
            entry["pending"] = False
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            return entry, f"快照记录已{action}"

    def _run(self, entry: dict[str, Any], action: str, expected_version: int | None) -> tuple[dict[str, Any] | None, str]:
        checkpoint = entry["checkpoint"]
        if expected_version is not None and int(expected_version) != int(checkpoint["version"]):
            return None, f"检查点主版本冲突：当前 v{checkpoint['version']}，提交 v{expected_version}，本次结果未落库"
        now = _now()
        manifest = entry["reuse_manifest"]
        completed = set(checkpoint["completed"])
        timeline = entry["completion_timeline"]
        seen = {rec["shard_id"] for rec in timeline}
        seq = int(checkpoint.get("last_seq") or 0)
        reused: list[str] = []
        scanned: list[str] = []
        changed = False
        for shard in entry["shards"]:
            sid = shard["shard_id"]
            if shard["state"] == SHARD_DONE and sid in completed:
                # 恢复检查：已完成分片不重复扫描，记入分片复用清单
                shard["reused"] = True
                if not any(item["shard_id"] == sid for item in manifest["items"]):
                    manifest["items"].append({
                        "shard_id": sid,
                        "origin": manifest["source_segment"],
                        "reused_at": _iso(now),
                    })
                    changed = True
                reused.append(sid)
                continue
            # 只重扫未完成分片，完成记录只追加一次
            shard["scan_count"] = int(shard.get("scan_count") or 0) + 1
            shard["state"] = SHARD_DONE
            seq += 1
            shard["seq"] = seq
            shard["completed_at"] = _iso(now + timedelta(seconds=seq))
            completed.add(sid)
            if sid not in seen:
                timeline.append({"shard_id": sid, "seq": seq, "completed_at": shard["completed_at"]})
                seen.add(sid)
            scanned.append(sid)
            changed = True
        if not changed:
            return entry, "所有分片均已完成并复用，未触发重复扫描"
        # 共享检查点：分片完成顺序与快照最终状态同源推进
        checkpoint["completed"] = [s["shard_id"] for s in entry["shards"] if s["shard_id"] in completed]
        checkpoint["last_seq"] = seq
        checkpoint["version"] = int(checkpoint.get("version") or 0) + 1
        checkpoint["updated_at"] = _iso(now)
        manifest["version"] = checkpoint["version"]
        manifest["updated_at"] = _iso(now)
        if len(checkpoint["completed"]) == len(entry["shards"]):
            entry["status"] = "已完成"
            # 时间线按完成时间推进：快照完成时间取最后一条分片完成记录
            entry["finished_at"] = max(timeline, key=lambda r: (r.get("seq") is None, r.get("seq") or 0))["completed_at"]
            if manifest["stage"] == MANIFEST_STAGES[0]:
                manifest["stage"] = MANIFEST_STAGES[1]  # 采集阶段完成后才推进到校验
        else:
            entry["status"] = "执行中"
        entry["pending"] = entry["status"] not in ("已完成", "执行失败")
        entry["abnormal"] = False
        event = "恢复" if action == "恢复快照" or reused else "执行"
        entry["resume_log"].append({
            "event": event,
            "at": _iso(now),
            "version": checkpoint["version"],
            "reused": len(reused),
            "scanned": len(scanned),
            "text": f"{event}：复用 {len(reused)} 个已完成分片，重扫 {len(scanned)} 个未完成分片（检查点 v{checkpoint['version']}）",
        })
        return entry, f"快照已{action}：复用 {len(reused)} 个分片，重扫 {len(scanned)} 个分片"

    def _pause(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        now = _iso(_now())
        entry["status"] = "已暂停"
        entry["pending"] = True
        entry["abnormal"] = False
        checkpoint = entry["checkpoint"]
        checkpoint["updated_at"] = now
        entry["resume_log"].append({
            "event": "暂停",
            "at": now,
            "version": checkpoint["version"],
            "text": f"作业暂停：已完成 {len(checkpoint['completed'])}/{len(entry['shards'])} 个分片，进度写入恢复检查点 v{checkpoint['version']}",
        })
        return entry, "快照已暂停：进度已写入恢复检查点"

    def _rollback(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        now = _iso(_now())
        manifest = entry["reuse_manifest"]
        manifest["stage"] = MANIFEST_STAGES[0]  # 回到采集节点
        manifest["status"] = "待核对"  # 回退后重新打开分片复用清单
        for item in manifest["items"]:
            item["origin"] = manifest["source_segment"]  # 追溯同一段来源
        manifest["updated_at"] = now
        entry["status"] = "已暂停"
        entry["pending"] = True
        entry["resume_log"].append({
            "event": "回退",
            "at": now,
            "version": entry["checkpoint"]["version"],
            "text": f"回退到采集节点：复用清单重新打开，可追溯来源段 {manifest['source_segment']}",
        })
        return entry, "快照已回退：分片复用清单已重新打开，可追溯同一段来源"

    def _archive(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        manifest = entry["reuse_manifest"]
        if manifest["status"] != "已核对":
            return None, "分片复用清单未核对，不能归档；请先通过快照校验"
        now = _iso(_now())
        manifest["stage"] = MANIFEST_STAGES[2]  # 归档回到对应节点
        manifest["updated_at"] = now
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["resume_log"].append({
            "event": "归档",
            "at": now,
            "version": entry["checkpoint"]["version"],
            "text": "复用清单已核对，快照归档",
        })
        return entry, "快照已归档：分片复用清单已持久化"

    # ---- 快照校验：消费并持久化分片复用清单，核对主版本边界 ----
    def validate(self, entry_id: int, version: int) -> tuple[dict[str, Any] | None, str]:
        with _LOCK:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"快照记录 {entry_id} 不存在或已归档"
            self._ensure_state(entry)
            manifest = entry["reuse_manifest"]
            checkpoint = entry["checkpoint"]
            if manifest["stage"] == MANIFEST_STAGES[0]:
                return None, "采集阶段未完成：分片复用清单待当前阶段完成后才能校验"
            now = _iso(_now())
            if int(version) != int(checkpoint["version"]):
                # 主版本边界核对失败：冲突结果等待核对
                manifest["status"] = "冲突待核对"
                manifest["updated_at"] = now
                return None, f"主版本边界核对失败：清单版本 v{version} ≠ 检查点版本 v{checkpoint['version']}，冲突结果等待核对"
            manifest["status"] = "已核对"
            manifest["version"] = checkpoint["version"]
            manifest["updated_at"] = now
            if manifest["stage"] == MANIFEST_STAGES[1]:
                manifest["stage"] = MANIFEST_STAGES[2]  # 校验阶段完成后才推进到归档
            entry["resume_log"].append({
                "event": "校验",
                "at": now,
                "version": checkpoint["version"],
                "text": f"复用清单已核对并持久化（{len(manifest['items'])} 个复用分片，主版本 v{checkpoint['version']}）",
            })
            return manifest, "分片复用清单已核对并持久化"

    def get_reuse_manifest(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        self._ensure_state(entry)
        manifest = entry["reuse_manifest"]
        return {
            "snapshot_id": int(entry.get("id", 0) or 0),
            "快照编号": entry.get("快照编号"),
            **manifest,
            "checkpoint_version": entry["checkpoint"]["version"],
        }


def load_reuse_manifest(snapshot_id: int) -> dict[str, Any] | None:
    """跨服务模块读取分片复用清单：沿着既有结果数据流定位清单变化。"""
    return SnapshotService().get_reuse_manifest(snapshot_id)
