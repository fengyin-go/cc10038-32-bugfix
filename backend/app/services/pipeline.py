"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from app.services import rule_proof as rp
from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # 并发测试观察点：解析暂存完成、正式落库之前触发。
    # 默认 None，生产调用不会受影响；测试可用它把“指定规则恰好在此刻发布”
    # 变成确定性场景，验证核验逻辑而不是靠线程调度碰运气。
    parse_midpoint_hook = None

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。

        规则选择是整个链路的信任根：
        - 传空规则名（None / 空串）表示“未指定”，沿用已有的默认映射；
        - 传了非空规则名就必须找到同名且已发布的规则，找不到直接报错，
          绝不回退到默认规则或别的已发布规则；
        - 规则快照在锁内冻结，明细写入前再次复核版本与指纹，只有能证明
          使用了所指定规则的输出才进入正式明细。

        解析计算刻意放在锁外，让“指定规则恰好在解析期间发布完成”成为
        真实可达的并发窗口，而不是靠线程调度碰运气；提交阶段重新加锁并
        复核规则身份。
        """
        requested = (parser_rule_name or "").strip() or None

        # ---- 阶段 A：锁内读取批次快照并选择规则 ----
        with store.lock:
            batch_raw = [r for r in store.rows("raw_records")
                         if r.get("批次编号") == batch_no]
            if not batch_raw:
                return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}
            selection = self._select_parser_rule(requested)
            if not selection["ok"]:
                return self._fail_without_rule(batch_no, requested, selection, batch_raw)
            schema_ver = next(
                (sv for sv in store.rows("schema_version")
                 if sv.get("status") == "已发布"),
                None,
            )
            snapshot_rows = [
                {
                    "id": r["id"],
                    "body": r.get("原始正文", ""),
                    "line_no": r.get("原始行号"),
                    "status": r.get("status"),
                }
                for r in batch_raw
            ]

        # ---- 阶段 B：锁外计算解析结果（并发发布可以真实发生在这里） ----
        staged, failed_ids, parse_counts, already_parsed = self._stage_parse(
            snapshot_rows, selection
        )
        hook = type(self).parse_midpoint_hook
        if hook is not None:
            hook(self, batch_no, requested, selection)

        # ---- 阶段 C：锁内复核并提交 ----
        with store.lock:
            return self._commit_parse(
                batch_no=batch_no,
                requested=requested,
                selection=selection,
                staged=staged,
                failed_ids=failed_ids,
                parse_counts=parse_counts,
                already_parsed=already_parsed,
                schema_ver=schema_ver,
            )

    def _stage_parse(
        self,
        snapshot_rows: list[dict[str, Any]],
        selection: dict[str, Any],
    ) -> tuple[
        list[tuple[dict[str, Any], dict[str, Any]]],
        list[int],
        dict[str, int],
        list[int],
    ]:
        """按选定的字段顺序解析正文，不改动任何共享状态。"""
        staged: list[tuple[dict[str, Any], dict[str, Any]]] = []
        failed_ids: list[int] = []
        parse_counts: dict[str, int] = {}
        already_parsed: list[int] = []
        for row in snapshot_rows:
            if row["status"] == "已解析" and not selection["explicit"]:
                # 未指定规则时沿用默认映射：已完成的原始行不重复解析。
                already_parsed.append(row["id"])
                continue
            try:
                fields, part_count, field_count = rp.parse_body(
                    row["body"], selection["field_names"]
                )
                if selection["explicit"] and part_count != field_count:
                    raise ValueError(
                        f"字段数不匹配：正文 {part_count} 段，规则 {field_count} 字段"
                    )
                signature = ",".join(fields.keys())
                staged.append((row, {
                    "fields": fields,
                    "field_signature": signature,
                    "content_digest": _stable_digest(fields),
                    "error": "",
                }))
                parse_counts[signature] = parse_counts.get(signature, 0) + 1
            except Exception as exc:  # noqa: BLE001 - 失败原因要回写到处置链路
                staged.append((row, {"fields": None, "error": str(exc)}))
                failed_ids.append(row["id"])
        return staged, failed_ids, parse_counts, already_parsed

    def _commit_parse(
        self,
        *,
        batch_no: str,
        requested: str | None,
        selection: dict[str, Any],
        staged: list[tuple[dict[str, Any], dict[str, Any]]],
        failed_ids: list[int],
        parse_counts: dict[str, int],
        already_parsed: list[int],
        schema_ver: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """锁内复核规则身份，然后写入明细、日志和处置清单。"""
        live_raw = {
            r["id"]: r for r in store.rows("raw_records")
            if r.get("批次编号") == batch_no
        }
        verification = self._verify_rule_unchanged(selection)
        if not verification["ok"]:
            # 核验失败：本次暂存结果作废，不写入任何明细。
            # 指定规则没能兑现，之前的结果也无法证明是按该规则产出的，
            # 一并退出正式明细（保留行以便追溯），避免错位明细残留。
            superseded = []
            if selection["explicit"]:
                superseded = self._supersede_batch_entities(
                    batch_no, "规则核验失败：" + verification["message"]
                )
            covered = {
                str(e.get("原始记录ID"))
                for e in store.rows("parsed_entity")
                if e.get("来源批次") == batch_no and e.get("formal_detail") is not False
            }
            pending_raw = [
                r for r in live_raw.values() if str(r["id"]) not in covered
            ]
            raw_ids = [r["id"] for r in pending_raw]
            self._mark_raw_pending(pending_raw, verification["message"])
            self._enqueue_retry(batch_no, raw_ids, selection, verification["message"])
            journal_entry = self._write_parse_journal(
                batch_no=batch_no,
                success=0,
                failure=len(raw_ids),
                status="失败",
                result="失败",
                requested_rule=requested,
                rule_selection={**selection, "ok": False, "reason": "rule_changed"},
                failed_ids=raw_ids,
                parse_counts={},
                note=verification["message"],
                skipped_already_parsed=already_parsed,
                superseded_entities=superseded,
            )
            return {
                "ok": False,
                "message": verification["message"],
                "rule_verification": verification,
                "rule_selection": {k: v for k, v in selection.items() if k != "rule"},
                "journal_id": journal_entry["id"],
                "failed_ids": raw_ids,
                "pending_record_ids": raw_ids,
                "skipped_already_parsed": already_parsed,
                "superseded_entity_ids": superseded,
            }

        superseded: list[int] = []
        if selection["explicit"]:
            # 显式指定规则是一次权威重做：旧明细（可能来自默认映射或旧字段
            # 顺序）退出正式明细，但保留行以备追溯，原始行号保持不变。
            superseded = self._supersede_batch_entities(batch_no, "按指定规则重做")

        rule = selection["rule"]
        rule_snapshot = selection["snapshot"]
        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        parsed_ids: list[int] = []
        live_failed_ids: list[int] = []

        for row, parsed in staged:
            raw = live_raw.get(row["id"])
            if raw is None:
                continue
            if parsed["fields"] is None:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                raw["parse_error"] = parsed["error"]
                live_failed_ids.append(raw["id"])
                continue
            ent_id = _next_id("parsed_entity")
            entity = {
                "id": ent_id,
                "来源批次": batch_no,
                "原始记录ID": str(raw["id"]),
                "解析规则版本": rule.get("规则名称", "unknown"),
                "标准化字段集": json.dumps(parsed["fields"], ensure_ascii=False),
                "校验状态": "",
                "status": "待校验",
                "pending": True,
                "abnormal": False,
                "parser_rule": rule.get("规则名称"),
                "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                "parsed_at": rp.now_iso(),
                "content_digest": parsed["content_digest"],
                # 规则核验证据：请求的是哪条规则、实际用了哪条、字段顺序是什么、
                # 是否回退到默认映射，全部随明细落库。
                "rule_requested": requested or "",
                "rule_mode": "explicit" if selection["explicit"] else "default",
                "rule_fallback": selection["explicit"] is False,
                "rule_id": rule_snapshot["rule_id"],
                "rule_version": rule_snapshot["rule_version"],
                "rule_digest": rule_snapshot["rule_digest"],
                "rule_field_signature": parsed["field_signature"],
                "rule_verified": True,
                "rule_proof": verification["proof"],
                "formal_detail": True,
                "raw_line_no": raw.get("原始行号"),
            }
            parsed_entities.append(entity)
            raw["status"] = "已解析"
            raw["pending"] = False
            raw["解析状态"] = "已解析"
            raw["abnormal"] = False
            raw["parse_error"] = ""
            success_count += 1
            parsed_ids.append(ent_id)

        live_failed_ids = list(dict.fromkeys(live_failed_ids))
        failure_count = len(live_failed_ids)

        # 更新批次状态
        for b in store.rows("ingest_batch"):
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        journal_status = "成功" if failure_count == 0 else "部分成功"
        journal_entry = self._write_parse_journal(
            batch_no=batch_no,
            success=success_count,
            failure=failure_count,
            status=journal_status,
            result=journal_status,
            requested_rule=requested,
            rule_selection=selection,
            failed_ids=live_failed_ids,
            parse_counts=parse_counts,
            note="",
            skipped_already_parsed=already_parsed,
            superseded_entities=superseded,
        )

        succeeded_raw_ids: list[int] = []
        for ent_id in parsed_ids:
            entity = store.find("parsed_entity", ent_id)
            if entity is None:
                continue
            entity["parse_journal_id"] = journal_entry["id"]
            succeeded_raw_ids.append(int(entity["原始记录ID"]))
        for raw_id in succeeded_raw_ids:
            raw = live_raw.get(raw_id)
            if raw is not None:
                raw["parse_journal_id"] = journal_entry["id"]

        # 补做成功：处置清单里的同一原始行关闭待办，并回写核验日志。
        resolved = self._resolve_retry_entries(batch_no, succeeded_raw_ids)
        journal_entry["resolved_record_ids"] = json.dumps(resolved)
        journal_entry["resolved_record_count"] = str(len(resolved))

        # 失败记录进入处置清单（重试队列），并保留可读原文。
        self._enqueue_retry(batch_no, live_failed_ids, selection, "")

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "skipped_already_parsed": already_parsed,
            "superseded_entity_ids": superseded,
            "parsed_ids": parsed_ids,
            "failed_ids": live_failed_ids,
            "journal_id": journal_entry["id"],
            "rule_selection": {k: v for k, v in selection.items() if k != "rule"},
            "rule_verification": {
                "requested_rule": requested or "",
                "resolved_rule": rule.get("规则名称"),
                "mode": "explicit" if selection["explicit"] else "default",
                "fallback": selection["explicit"] is False,
                "rule_version": rule_snapshot["rule_version"],
                "rule_digest": rule_snapshot["rule_digest"],
                "proof": verification["proof"],
                "field_order": selection["field_names"],
                "field_signature": rule_snapshot["field_signature"],
                "matched_entities": sum(parse_counts.values()),
                "unmatched_entities": failure_count,
                "observed_field_signatures": parse_counts,
            },
        }

    def _fail_without_rule(
        self,
        batch_no: str,
        requested: str | None,
        selection: dict[str, Any],
        batch_raw: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """指定规则无法兑现时的统一失败路径。调用方必须已持有 store.lock。"""
        # 规则找不到时不得动用默认映射，否则成功日志背后是错位字段。
        # 显式指定但无法兑现时，整批原始行都视为“未按指定规则完成”：
        # 原始行退回待解析、原文保持可读，旧明细降级为非正式，等待补做。
        previous = self._supersede_batch_entities(batch_no, selection["message"])
        pending_ids = [r["id"] for r in batch_raw]
        self._mark_raw_pending(batch_raw, selection["message"])
        self._enqueue_retry(batch_no, pending_ids, selection, selection["message"])
        journal_entry = self._write_parse_journal(
            batch_no=batch_no,
            success=0,
            failure=len(pending_ids),
            status="失败",
            result="失败",
            requested_rule=requested,
            rule_selection=selection,
            failed_ids=pending_ids,
            parse_counts={},
            note=selection["message"],
            superseded_entities=previous,
        )
        return {
            "ok": False,
            "message": selection["message"],
            "rule_selection": selection,
            "journal_id": journal_entry["id"],
            "failed_ids": pending_ids,
            "pending_record_ids": pending_ids,
            "superseded_entity_ids": previous,
        }

    def _find_rule_by_id(self, rule_id: Any) -> dict[str, Any] | None:
        if rule_id is None:
            return None
        for rule in store.rows("field_parser"):
            if rule.get("id") == rule_id:
                return rule
        return None

    def _verify_rule_unchanged(self, selection: dict[str, Any]) -> dict[str, Any]:
        """复核执行期间规则没有被重新发布。

        默认映射模式没有“指定规则”可证明，沿用历史行为；显式模式必须在写入
        前再次确认规则版本和指纹与解析开始时一致。
        """
        snapshot = selection.get("snapshot") or {}
        rule_name = (selection.get("rule") or {}).get("规则名称")
        if not selection.get("explicit"):
            # 默认映射：没有指定规则时，如果实际借用了某条已发布规则，
            # 同样要确认它在解析期间没有被换版本；纯内置默认映射天然稳定。
            if snapshot.get("rule_id") is None:
                return {
                    "ok": True,
                    "proof": "default_mapping",
                    "message": "未指定规则名，使用内置默认字段映射",
                    "expected_digest": snapshot.get("rule_digest", ""),
                    "actual_digest": snapshot.get("rule_digest", ""),
                }
            current = self._find_rule_by_id(snapshot.get("rule_id"))
            current_snapshot = rp.rule_snapshot(current) if current else None
            if current_snapshot is not None and current_snapshot["rule_digest"] == snapshot.get("rule_digest"):
                return {
                    "ok": True,
                    "proof": "default_mapping",
                    "message": "未指定规则名，沿用的已发布规则在执行期间未变化",
                    "expected_digest": snapshot.get("rule_digest", ""),
                    "actual_digest": current_snapshot["rule_digest"],
                }
            return {
                "ok": False,
                "proof": "unproven",
                "reason": "rule_changed",
                "message": (
                    f"未指定规则名时沿用的已发布规则「{rule_name}」在解析期间被重新发布，"
                    "本次输出不进入正式明细"
                ),
                "expected_digest": snapshot.get("rule_digest", ""),
                "actual_digest": current_snapshot["rule_digest"] if current_snapshot else "",
            }
        current = None
        for candidate in store.rows("field_parser"):
            if candidate.get("规则名称") == rule_name:
                current = candidate
                break
        if current is None:
            return {
                "ok": False,
                "proof": "unproven",
                "reason": "rule_changed",
                "message": f"解析期间指定规则「{rule_name}」被移除，结果不进入正式明细",
                "expected_digest": snapshot.get("rule_digest", ""),
                "actual_digest": "",
            }
        current_snapshot = rp.rule_snapshot(current)
        same = (
            current.get("status") == "已发布"
            and current_snapshot["rule_digest"] == snapshot.get("rule_digest")
        )
        if not same:
            return {
                "ok": False,
                "proof": "unproven",
                "reason": "rule_changed",
                "message": (
                    f"解析期间指定规则「{rule_name}」被重新发布"
                    f"（版本 {snapshot.get('rule_version')} → {current_snapshot['rule_version']}），"
                    "本次输出无法证明使用了指定版本，不进入正式明细"
                ),
                "expected_digest": snapshot.get("rule_digest", ""),
                "actual_digest": current_snapshot["rule_digest"],
            }
        return {
            "ok": True,
            "proof": "explicit_verified",
            "message": f"指定规则「{rule_name}」版本与指纹复核一致",
            "expected_digest": snapshot.get("rule_digest", ""),
            "actual_digest": current_snapshot["rule_digest"],
        }

    def _mark_raw_pending(self, batch_raw: list[dict[str, Any]], reason: str) -> None:
        """规则核验失败时原始行保持待解析，原文可读，编号不变。"""
        for raw in batch_raw:
            raw["status"] = "待解析"
            raw["解析状态"] = "待解析"
            raw["pending"] = True
            raw["abnormal"] = False
            raw["parse_error"] = reason

    def _supersede_batch_entities(self, batch_no: str, reason: str) -> list[int]:
        """把该批次此前作为正式明细的结果降级。

        指定规则找不到时，历史结果很可能来自默认映射或另一条规则。它们必须
        退出正式明细，否则“解析失败”的日志旁边仍残留着字段错位的明细。
        记录本身保留，便于追溯，但 formal_detail 置为 False。
        """
        superseded: list[int] = []
        for entity in store.rows("parsed_entity"):
            if entity.get("来源批次") != batch_no:
                continue
            if entity.get("status") in ("已作废",) or entity.get("formal_detail") is False:
                continue
            entity["formal_detail"] = False
            entity["rule_proof"] = "superseded"
            entity["status"] = "校验失败"
            entity["校验状态"] = "已作废"
            entity["pending"] = False
            entity["abnormal"] = True
            entity["invalidated_reason"] = reason
            entity["invalidated_at"] = rp.now_iso()
            superseded.append(int(entity.get("id", 0)))
        return superseded

    def _enqueue_retry(
        self,
        batch_no: str,
        failed_ids: list[int],
        selection: dict[str, Any],
        reason: str,
    ) -> None:
        """把失败/待补做的原始行登记进处置清单，原文保持可读。"""
        if not failed_ids:
            return
        retry_q = store.rows("retry_queue")
        raw_by_id = {int(r.get("id", 0)): r for r in store.rows("raw_records")}
        snapshot = selection.get("snapshot") or {}
        for fid in failed_ids:
            raw = raw_by_id.get(int(fid))
            if raw is None:
                continue
            existing = next(
                (r for r in retry_q if r.get("失败记录ID") == str(fid)
                 and r.get("batch_no") == batch_no and r.get("status") == "待重试"),
                None,
            )
            if existing is not None:
                # 同一原始行只保留一条待处置登记，不能按解析次数重复累计。
                existing["pending_raw_body"] = raw.get("原始正文", "")
                existing["rule_reason"] = reason or existing.get("rule_reason", "")
                continue
            retry_entry = {
                "id": _next_id("retry_queue"),
                "失败记录ID": str(fid),
                "失败原因": reason or "字段解析失败",
                "重试次数": "0",
                "最大重试": "3",
                "下次重试时间": rp.now_iso(),
                "status": "待重试",
                "pending": True,
                "abnormal": False,
                "batch_no": batch_no,
                "parser_rule": (selection.get("rule") or {}).get("规则名称"),
                "rule_reason": selection.get("reason", ""),
                "rule_digest": snapshot.get("rule_digest", ""),
                # 待补做原文必须可读，且记录原始行号；补做时沿用同一 raw id。
                "raw_line_no": raw.get("原始行号"),
                "pending_raw_body": raw.get("原始正文", ""),
            }
            retry_q.append(retry_entry)

    def _resolve_retry_entries(self, batch_no: str, raw_ids: list[int]) -> list[int]:
        """原始行补做成功后，关闭处置清单里对应的待办登记。"""
        if not raw_ids:
            return []
        targets = {str(rid) for rid in raw_ids}
        resolved: list[int] = []
        for entry in store.rows("retry_queue"):
            if (
                entry.get("batch_no") == batch_no
                and entry.get("失败记录ID") in targets
                and entry.get("status") in ("待重试", "重试中")
            ):
                entry["status"] = "已补做"
                entry["pending"] = False
                entry["abnormal"] = False
                entry["resolved_at"] = rp.now_iso()
                resolved.append(int(entry.get("失败记录ID", 0)))
        return resolved

    def _select_parser_rule(self, parser_rule_name: str | None) -> dict[str, Any]:
        """选择解析规则，并把“为什么是这条规则”记录成可核验结构。"""
        parsers = store.rows("field_parser")

        if not parser_rule_name:
            # 未指定名称：兼容已有行为，优先使用已发布规则，其次沿用默认映射。
            rule = None
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
            if rule is not None:
                explicit_fields = rp.normalize_output_fields(rule.get("输出字段列表"))
                field_names = explicit_fields or list(rp.DEFAULT_FIELDS)
                snapshot = rp.rule_snapshot(rule)
                return {
                    "ok": True,
                    "explicit": False,
                    "mode": "default",
                    "rule": rule,
                    "snapshot": snapshot,
                    "field_names": field_names,
                    "used_default_fields": explicit_fields is None,
                    "message": "未指定规则名，沿用已发布规则",
                }
            placeholder = {
                "id": None,
                "规则名称": "default",
                "status": "默认",
                "rule_version": 0,
                "输出字段列表": ",".join(rp.DEFAULT_FIELDS),
            }
            snapshot = rp.rule_snapshot(placeholder)
            return {
                "ok": True,
                "explicit": False,
                "mode": "default",
                "rule": placeholder,
                "snapshot": snapshot,
                "field_names": list(rp.DEFAULT_FIELDS),
                "used_default_fields": True,
                "message": "未指定规则名且无已发布规则，使用默认字段映射",
            }

        matches = [p for p in parsers if p.get("规则名称") == parser_rule_name]
        if not matches:
            return {
                "ok": False,
                "explicit": True,
                "mode": "explicit",
                "requested_rule": parser_rule_name,
                "rule": None,
                "snapshot": None,
                "field_names": [],
                "reason": "not_found",
                "message": f"指定的解析规则「{parser_rule_name}」不存在，已停止解析，未回退默认规则",
            }
        if len(matches) > 1:
            return {
                "ok": False,
                "explicit": True,
                "mode": "explicit",
                "requested_rule": parser_rule_name,
                "rule": None,
                "snapshot": None,
                "field_names": [],
                "reason": "ambiguous",
                "message": (
                    f"指定的解析规则「{parser_rule_name}」存在 {len(matches)} 条同名记录，"
                    "无法确定使用哪一条，已停止解析"
                ),
            }
        rule = matches[0]
        if rule.get("status") != "已发布":
            return {
                "ok": False,
                "explicit": True,
                "mode": "explicit",
                "requested_rule": parser_rule_name,
                "rule": rule,
                "snapshot": rp.rule_snapshot(rule),
                "field_names": [],
                "reason": "not_published",
                "message": (
                    f"指定的解析规则「{parser_rule_name}」当前状态为"
                    f"「{rule.get('status')}」，尚未发布，已停止解析"
                ),
            }
        explicit_fields = rp.normalize_output_fields(rule.get("输出字段列表"))
        if explicit_fields is None:
            return {
                "ok": False,
                "explicit": True,
                "mode": "explicit",
                "requested_rule": parser_rule_name,
                "rule": rule,
                "snapshot": rp.rule_snapshot(rule),
                "field_names": [],
                "reason": "no_field_mapping",
                "message": (
                    f"指定的解析规则「{parser_rule_name}」没有可核验的输出字段列表，"
                    "已停止解析，不使用默认字段映射"
                ),
            }
        snapshot = rp.rule_snapshot(rule)
        return {
            "ok": True,
            "explicit": True,
            "mode": "explicit",
            "rule": rule,
            "snapshot": snapshot,
            "field_names": explicit_fields,
            "used_default_fields": False,
            "message": f"使用指定规则「{parser_rule_name}」",
        }

    def _write_parse_journal(
        self,
        *,
        batch_no: str,
        success: int,
        failure: int,
        status: str,
        result: str,
        requested_rule: str | None,
        rule_selection: dict[str, Any],
        failed_ids: list[int],
        parse_counts: dict[str, int],
        note: str,
        skipped_already_parsed: list[int] | None = None,
        superseded_entities: list[int] | None = None,
    ) -> dict[str, Any]:
        """写解析操作日志。无论成功失败，都带上规则核验结果和待处置原文。"""
        skipped_already_parsed = skipped_already_parsed or []
        superseded_entities = superseded_entities or []
        snapshot = rule_selection.get("snapshot") or {}
        rule = rule_selection.get("rule") or {}
        mode = "explicit" if rule_selection.get("explicit") else "default"
        # 规则核验：显式指定且全部成功才算“可证明”；默认映射按历史行为放行，
        # 但日志要明确标注它不可用于证明“用了所指定规则”。
        selection_ok = bool(rule_selection.get("ok", False))
        explicit = bool(rule_selection.get("explicit"))
        proven = selection_ok and explicit and success > 0 and failure == 0
        # 有产出才谈得上进入正式明细；核验是否可证明由 rule_verified 单独标记。
        formal_detail = selection_ok and success > 0
        proof_label = (
            "explicit_verified" if proven
            else "default_mapping" if selection_ok and not explicit
            else "unproven"
        )
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": rp.now_iso(),
            "影响的记录数": str(success),
            "执行结果": result,
            "status": status,
            "pending": status != "成功",
            "abnormal": status == "失败",
            "success_count": str(success),
            "failure_count": str(failure),
            # 规则核验结果：请求、解析到、模式、版本、指纹、字段顺序、是否回退。
            "requested_rule": requested_rule or "",
            "resolved_rule": rule.get("规则名称"),
            "rule_mode": mode,
            "rule_fallback": bool(rule_selection.get("explicit") is False),
            "rule_reason": rule_selection.get("reason", ""),
            "rule_version": snapshot.get("rule_version"),
            "rule_digest": snapshot.get("rule_digest", ""),
            "field_signature": snapshot.get("field_signature", ""),
            "field_order": json.dumps(
                (snapshot.get("output_fields") or rule_selection.get("field_names") or []),
                ensure_ascii=False,
            ),
            "observed_field_signatures": json.dumps(parse_counts, ensure_ascii=False),
            "rule_verified": bool(proven),
            "rule_proof": proof_label,
            "formal_detail": bool(formal_detail),
            "skipped_parsed_record_ids": json.dumps(skipped_already_parsed),
            "skipped_parsed_count": str(len(skipped_already_parsed)),
            "superseded_entity_ids": json.dumps(superseded_entities),
            "superseded_entity_count": str(len(superseded_entities)),
            "failed_record_ids": json.dumps(failed_ids),
            "failed_record_count": str(len(failed_ids)),
            "note": note,
        }
        if failed_ids:
            # 待补做原文快照：retry_queue 里要有可读原文，不能只剩一个 ID。
            raw_by_id = {int(r.get("id", 0)): r for r in store.rows("raw_records")}
            pending_rows = []
            for fid in failed_ids:
                raw = raw_by_id.get(int(fid))
                if raw is None:
                    continue
                pending_rows.append({
                    "raw_id": raw.get("id"),
                    "line_no": raw.get("原始行号"),
                    "body": raw.get("原始正文"),
                    "batch_no": raw.get("批次编号"),
                    "error": raw.get("parse_error", ""),
                })
            journal_entry["pending_raw_rows"] = json.dumps(pending_rows, ensure_ascii=False)
            journal_entry["pending_raw_count"] = str(len(pending_rows))
        else:
            journal_entry["pending_raw_rows"] = "[]"
            journal_entry["pending_raw_count"] = "0"
        store.rows("parse_journal").append(journal_entry)
        return journal_entry

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """兼容旧调用：按规则字段顺序解析；无字段列表时使用默认字段名。"""
        field_names = rp.normalize_output_fields(rule.get("输出字段列表")) or list(
            rp.DEFAULT_FIELDS
        )
        fields, _, _ = rp.parse_body(body, field_names)
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。"""
        parsed = store.rows("parsed_entity")
        results: dict[str, dict[str, Any]] = {}

        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": results,
            "refreshed_at": _now_iso(),
            "source_records": len(parsed),
        }
        aggs.append(agg_entry)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "results": results_data,
            "record_count": len(results_data),
            "exported_at": _now_iso(),
        }
        exports.append(export_entry)

        # 记录血缘
        lineage_entry = {
            "id": _next_id("data_lineage"),
            "来源批次": view_name,
            "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
            "标准化版本": view_name,
            "下游消费标识": f"export-{export_id}",
            "记录时间": _now_iso(),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "export_id": str(export_id),
            "export_format": export_format,
        }
        lineages.append(lineage_entry)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。

        重放必须真正按新规则重新解析，不能只把明细的“解析规则版本”标签改掉
        ——那会再次制造“日志说用了新规则、字段其实没变”的错位。找不到规则
        时直接失败，不生成重放任务。
        """
        with store.lock:
            selection = self._select_parser_rule((new_parser_rule or "").strip() or None)
            if not selection["ok"]:
                return {
                    "ok": False,
                    "message": selection["message"],
                    "rule_selection": selection,
                }

            replayers = store.rows("replay_engine")
            replay_id = _next_id("replay_engine")
            replay_entry = {
                "id": replay_id,
                "历史批次范围": history_batch_range,
                "新解析规则版本": new_parser_rule,
                "重放状态": "已创建",
                "产出记录数": "0",
                "执行时间": _now_iso(),
                "status": "重放中",
                "pending": True,
                "abnormal": False,
                "rule_version": selection["snapshot"]["rule_version"],
                "rule_digest": selection["snapshot"]["rule_digest"],
                "rule_proof": "explicit_verified",
            }
            replayers.append(replay_entry)

        # 历史批次范围既可能是单个批次号，也可能是逗号分隔的批次列表。
        wanted = {
            part.strip() for part in str(history_batch_range).replace("，", ",").split(",")
            if part.strip()
        }
        batch_nos = [
            b.get("批次编号") for b in store.rows("ingest_batch")
            if b.get("批次编号") in wanted
        ]

        results = [self.parse_batch(batch_no, new_parser_rule) for batch_no in batch_nos]
        replay_count = sum(int(r.get("success", 0)) for r in results if r.get("ok"))

        with store.lock:
            replay_entry["产出记录数"] = str(replay_count)
            replay_entry["status"] = "已完成" if all(r.get("ok") for r in results) else "部分失败"
            replay_entry["pending"] = not all(r.get("ok") for r in results)
            replay_entry["abnormal"] = not all(r.get("ok") for r in results)
            replay_entry["重放状态"] = "已完成" if replay_entry["status"] == "已完成" else "部分失败"
            replay_entry["batch_results"] = [
                {
                    "batch_no": batch_no,
                    "ok": result.get("ok"),
                    "success": result.get("success", 0),
                    "failure": result.get("failure", 0),
                    "journal_id": result.get("journal_id"),
                }
                for batch_no, result in zip(batch_nos, results)
            ]

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """把失败/待解析的原始行重新送回解析链路。

        恢复只负责把原始行恢复成“待解析”，真正的产出仍统一由 parse_batch
        按指定规则生成，保证补做与首次解析走同一套核验逻辑，且原始行号不变。
        """
        requested = (parser_rule_name or "").strip() or None
        with store.lock:
            retry_q = store.rows("retry_queue")
            raw_records_list = store.rows("raw_records")

            failed_raw = [r for r in raw_records_list
                          if r.get("批次编号") == batch_no
                          and r.get("status") in ("解析失败", "待解析")]

            recovered = 0
            still_failed = 0
            for raw in failed_raw:
                retry_entry = None
                for r in retry_q:
                    if (r.get("失败记录ID") == str(raw["id"])
                            and r.get("batch_no") == batch_no
                            and r.get("status") in ("待重试", "重试中")):
                        retry_entry = r
                        break
                if retry_entry:
                    retry_count = int(retry_entry.get("重试次数", "0"))
                    max_retry = int(retry_entry.get("最大重试", "3"))
                    if retry_count >= max_retry:
                        retry_entry["status"] = "超过上限"
                        still_failed += 1
                        continue
                    retry_entry["重试次数"] = str(retry_count + 1)
                    retry_entry["status"] = "重试中"
                    retry_entry["下次重试时间"] = _now_iso()

                # 不改变原始日志编号：沿用同一 raw id，只把它放回待解析队列。
                raw["status"] = "待解析"
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1

            if recovered and requested is not None:
                # parse_batch 自己加锁，这里用可重入锁无碍；调用时仍持锁避免
                # 恢复与并发解析交错把同一原始行计两次。
                result = self.parse_batch(batch_no, requested)
                return {
                    "ok": result["ok"],
                    "message": result["message"],
                    "recovered": recovered,
                    "still_failed": still_failed,
                    "parse_result": result,
                }

            return {
                "ok": True,
                "message": f"批次 {batch_no} 恢复完成，等待指定规则后重新解析",
                "recovered": recovered,
                "still_failed": still_failed,
                "pending_rule": requested,
            }


pipeline = IngestPipeline()