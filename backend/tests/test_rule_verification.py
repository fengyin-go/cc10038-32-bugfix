"""解析规则核验与静默回退修复的行为测试。

覆盖的场景来自真实故障：指定了规则却找不到时，旧实现回退到默认映射，
解析日志显示成功，但明细字段已经错位。这里逐条固定修复后的可观察结果：

1. 空规则名 = 未指定，沿用既有默认映射（保持兼容）。
2. 不存在的规则名必须报错，不能产出任何正式明细。
3. 解析操作日志要按规则核验结果登记待补做的原始行，原文可读。
4. 失败结果回写处置清单；发布规则后可以从原批补做，原始行号不变。
5. 同一批次显式规则与默认规则的选择冲突：指定规则发布恰在解析尝试期间
   完成时，只有能证明用了指定规则的输出才进入正式明细。
6. 补做不会让同一次成功计量两次。
"""
from __future__ import annotations

import json

import pytest

from app.services import SERVICES
from app.services.pipeline import pipeline
from app.store import store


def _setup_batch(source_id: str, bodies: list[str]) -> str:
    pipeline.register_source(source_id, "http", "default")
    pipeline.validate_source(source_id)
    logs = [{"body": body} for body in bodies]
    result = pipeline.start_ingest_batch(source_id, "2026Q1", logs)
    assert result["ok"] is True
    return result["batch"]["批次编号"]


def _create_rule(name: str, fields: str, status: str = "已发布") -> dict:
    service = SERVICES["field_parser"]
    entry, missing = service.create_entry({
        "规则名称": name,
        "适用数据源": "demo",
        "输入格式": "pipe",
        "输出字段列表": fields,
        "解析引擎": "builtin",
    })
    assert not missing
    if status == "已发布":
        service.run_action(entry["id"], "发布规则")
    return entry


def _entities(batch_no: str) -> list[dict]:
    return [e for e in store.rows("parsed_entity") if e.get("来源批次") == batch_no]


def _formal_entities(batch_no: str) -> list[dict]:
    return [e for e in _entities(batch_no) if e.get("formal_detail") is not False]


def _raw(batch_no: str) -> list[dict]:
    return [r for r in store.rows("raw_records") if r.get("批次编号") == batch_no]


def _journal(batch_no: str) -> list[dict]:
    return [j for j in store.rows("parse_journal") if j.get("批次编号") == batch_no]


class TestEmptyRuleNameCompatibility:
    """空规则名表示“未指定”，必须保留已有默认映射行为。"""

    def test_empty_name_uses_default_mapping(self):
        batch_no = _setup_batch("empty-src", ["t1|INFO|hello", "t2|WARN|disk"])
        result = pipeline.parse_batch(batch_no, "")
        assert result["ok"] is True
        assert result["success"] == 2
        fields = [json.loads(e["标准化字段集"]) for e in _entities(batch_no)]
        assert fields[0] == {"time": "t1", "level": "INFO", "message": "hello"}
        assert fields[1] == {"time": "t2", "level": "WARN", "message": "disk"}
        journal = _journal(batch_no)[-1]
        assert journal["rule_mode"] == "default"
        assert journal["rule_fallback"] is True
        assert journal["rule_verified"] is False
        assert journal["rule_proof"] == "default_mapping"

    def test_none_and_whitespace_are_also_unspecified(self):
        batch_no = _setup_batch("none-src", ["t1|INFO|hello"])
        assert pipeline.parse_batch(batch_no)["ok"] is True
        batch2 = _setup_batch("blank-src", ["t1|INFO|hello"])
        assert pipeline.parse_batch(batch2, "   ")["ok"] is True


class TestMissingRuleFailsLoudly:
    """不存在的规则名必须报错，且不能留下错位明细。"""

    def test_missing_rule_reports_error(self):
        batch_no = _setup_batch("miss-src", ["t1|INFO|hello"])
        result = pipeline.parse_batch(batch_no, "no-such-rule")
        assert result["ok"] is False
        assert "不存在" in result["message"]
        assert "未回退" in result["message"]

    def test_missing_rule_produces_no_formal_detail(self):
        batch_no = _setup_batch("miss2-src", ["t1|INFO|hello"])
        pipeline.parse_batch(batch_no, "no-such-rule")
        assert _formal_entities(batch_no) == []
        assert _raw(batch_no)[0]["status"] == "待解析"

    def test_existing_but_unpublished_rule_is_not_used(self):
        batch_no = _setup_batch("draft-src", ["t1|INFO|hello"])
        _create_rule("draft-rule", "a,b,c", status="草稿")
        result = pipeline.parse_batch(batch_no, "draft-rule")
        assert result["ok"] is False
        assert "尚未发布" in result["message"]

    def test_rule_without_verifiable_field_list_is_rejected(self):
        batch_no = _setup_batch("nofields-src", ["t1|INFO|hello"])
        _create_rule("placeholder-rule", "某条占位文本")
        result = pipeline.parse_batch(batch_no, "placeholder-rule")
        assert result["ok"] is False
        assert _formal_entities(batch_no) == []

    def test_previous_default_detail_is_superseded(self):
        """先按默认映射解析成功，再用不存在的规则重做，旧明细必须退出正式集。"""
        batch_no = _setup_batch("supersede-src", ["t1|INFO|hello"])
        assert pipeline.parse_batch(batch_no, "")["ok"] is True
        assert len(_formal_entities(batch_no)) == 1

        result = pipeline.parse_batch(batch_no, "ghost-rule")
        assert result["ok"] is False
        assert _formal_entities(batch_no) == []
        assert result["superseded_entity_ids"] == [e["id"] for e in _entities(batch_no)]


class TestJournalRecordsReworkRows:
    """操作日志必须按规则核验结果登记待补做原始行，失败回写处置清单。"""

    def test_journal_contains_rule_proof_and_pending_rows(self):
        batch_no = _setup_batch("journal-src", ["t1|INFO|a", "t2|WARN|b"])
        pipeline.parse_batch(batch_no, "ghost")
        journal = _journal(batch_no)[-1]
        assert journal["执行结果"] == "失败"
        assert journal["rule_reason"] == "not_found"
        assert journal["requested_rule"] == "ghost"
        assert journal["rule_proof"] == "unproven"
        assert journal["rule_verified"] is False
        assert journal["formal_detail"] is False
        pending = json.loads(journal["pending_raw_rows"])
        assert [row["body"] for row in pending] == ["t1|INFO|a", "t2|WARN|b"]
        assert journal["pending_raw_count"] == "2"

    def test_failed_rows_enter_retry_queue_with_readable_body(self):
        batch_no = _setup_batch("retry-src", ["t1|INFO|a"])
        pipeline.parse_batch(batch_no, "ghost")
        tasks = [t for t in store.rows("retry_queue") if t.get("batch_no") == batch_no]
        assert len(tasks) == 1
        assert tasks[0]["status"] == "待重试"
        assert tasks[0]["pending_raw_body"] == "t1|INFO|a"

    def test_repeated_failures_do_not_duplicate_pending_rows(self):
        batch_no = _setup_batch("retry-dup-src", ["t1|INFO|a"])
        pipeline.parse_batch(batch_no, "ghost")
        pipeline.parse_batch(batch_no, "ghost")
        tasks = [t for t in store.rows("retry_queue") if t.get("batch_no") == batch_no]
        assert len(tasks) == 1


class TestReworkAfterPublish:
    """发布规则后重新提交原批：字段顺序按新规则，原始行号不变，只计量一次。"""

    def test_rework_uses_new_rule_and_keeps_raw_number(self):
        batch_no = _setup_batch("rework-src", ["t1|INFO|a", "t2|WARN|b"])
        raw_ids_before = [r["id"] for r in _raw(batch_no)]
        line_numbers_before = [r["原始行号"] for r in _raw(batch_no)]

        assert pipeline.parse_batch(batch_no, "")["ok"] is True
        assert pipeline.parse_batch(batch_no, "missing-rule")["ok"] is False

        _create_rule("app-log-v2", "ts,sev,msg")
        result = pipeline.parse_batch(batch_no, "app-log-v2")
        assert result["ok"] is True
        assert result["success"] == 2
        assert result["rule_verification"]["proof"] == "explicit_verified"
        assert result["rule_verification"]["field_order"] == ["ts", "sev", "msg"]

        formal = _formal_entities(batch_no)
        assert len(formal) == 2
        fields = [json.loads(e["标准化字段集"]) for e in formal]
        assert fields[0] == {"ts": "t1", "sev": "INFO", "msg": "a"}
        assert fields[1] == {"ts": "t2", "sev": "WARN", "msg": "b"}
        for entity in formal:
            assert entity["rule_field_signature"] == "ts,sev,msg"
            assert entity["rule_proof"] == "explicit_verified"
            assert entity["rule_version"] == 1

        # 原始日志编号和行号完全不变。
        assert [r["id"] for r in _raw(batch_no)] == raw_ids_before
        assert [r["原始行号"] for r in _raw(batch_no)] == line_numbers_before

    def test_rework_resolves_retry_tasks_and_counts_once(self):
        batch_no = _setup_batch("count-src", ["t1|INFO|a"])
        pipeline.parse_batch(batch_no, "ghost")
        tasks_before = [
            t for t in store.rows("retry_queue")
            if t.get("batch_no") == batch_no and t.get("status") == "待重试"
        ]
        assert len(tasks_before) == 1

        _create_rule("count-rule", "x,y,z")
        result = pipeline.parse_batch(batch_no, "count-rule")
        assert result["ok"] is True
        assert result["success"] == 1
        assert result["failure"] == 0
        journal = _journal(batch_no)[-1]
        assert journal["success_count"] == "1"
        assert json.loads(journal["resolved_record_ids"]) == [_raw(batch_no)[0]["id"]]
        assert journal["skipped_parsed_count"] == "0"

        # 再补一次：已解析的行不再重复计量。
        again = pipeline.parse_batch(batch_no, "count-rule")
        # 显式规则重做会重建一份正式明细，但不会在同一日志里重复计量同一原文。
        assert again["success"] == 1
        assert again["skipped_already_parsed"] == []
        journal_again = _journal(batch_no)[-1]
        assert int(journal_again["success_count"]) == 1

    def test_unspecified_reparse_does_not_duplicate_work(self):
        batch_no = _setup_batch("skip-src", ["t1|INFO|a"])
        assert pipeline.parse_batch(batch_no, "")["ok"] is True
        again = pipeline.parse_batch(batch_no, "")
        assert again["success"] == 0
        assert again["skipped_already_parsed"] == [_raw(batch_no)[0]["id"]]
        journal = _journal(batch_no)[-1]
        assert journal["skipped_parsed_count"] == "1"
        assert len(_entities(batch_no)) == 1


class TestPublishDuringParse:
    """发布恰好在解析尝试期间完成：只放行能证明用了指定规则的输出。"""

    def test_reparse_uses_published_rule_after_concurrent_publish(self):
        batch_no = _setup_batch("race-src", ["t1|INFO|a"])
        _create_rule("race-rule", "a,b,c")
        rule = next(r for r in store.rows("field_parser") if r["规则名称"] == "race-rule")

        def publish_during_parse(_pipeline, _batch_no, _requested, _selection):
            service = SERVICES["field_parser"]
            service.run_action(rule["id"], "弃用规则")
            rule["输出字段列表"] = "m,l,t"
            service.run_action(rule["id"], "发布规则")

        type(pipeline).parse_midpoint_hook = publish_during_parse
        try:
            result = pipeline.parse_batch(batch_no, "race-rule")
        finally:
            type(pipeline).parse_midpoint_hook = None

        assert result["ok"] is False
        assert "重新发布" in result["message"]
        assert _formal_entities(batch_no) == []
        assert _raw(batch_no)[0]["status"] == "待解析"
        assert result["superseded_entity_ids"] == []
        journal = _journal(batch_no)[-1]
        assert journal["rule_proof"] == "unproven"
        assert journal["formal_detail"] is False
        assert journal["pending_raw_count"] == "1"

        # 规则发布完成后重新提交原批即可产出可证明的明细。
        redo = pipeline.parse_batch(batch_no, "race-rule")
        assert redo["ok"] is True
        assert redo["rule_verification"]["field_order"] == ["m", "l", "t"]
        assert redo["rule_verification"]["proof"] == "explicit_verified"
        formal = _formal_entities(batch_no)
        assert len(formal) == 1
        assert json.loads(formal[0]["标准化字段集"]) == {"m": "t1", "l": "INFO", "t": "a"}

    def test_concurrent_republish_evicts_prior_default_detail(self):
        """默认映射产物再遇到核验失败的显式重做，也不能残留在正式明细里。"""
        batch_no = _setup_batch("race-evict-src", ["t1|INFO|a"])
        assert pipeline.parse_batch(batch_no, "")["ok"] is True
        assert len(_formal_entities(batch_no)) == 1

        _create_rule("evict-rule", "a,b,c")
        rule = next(r for r in store.rows("field_parser") if r["规则名称"] == "evict-rule")

        def republish(_pipeline, _batch_no, _requested, _selection):
            SERVICES["field_parser"].run_action(rule["id"], "发布规则")

        type(pipeline).parse_midpoint_hook = republish
        try:
            result = pipeline.parse_batch(batch_no, "evict-rule")
        finally:
            type(pipeline).parse_midpoint_hook = None
        assert result["ok"] is False
        assert _formal_entities(batch_no) == []
        assert result["superseded_entity_ids"]

    def test_rule_republished_with_same_fields_is_still_unproven(self):
        """重新发布即使字段相同也是新版本，不能声称用了旧版本。"""
        batch_no = _setup_batch("same-src", ["t1|INFO|a"])
        _create_rule("same-rule", "a,b,c")
        rule = next(r for r in store.rows("field_parser") if r["规则名称"] == "same-rule")

        def republish_same(_pipeline, _batch_no, _requested, _selection):
            SERVICES["field_parser"].run_action(rule["id"], "发布规则")

        type(pipeline).parse_midpoint_hook = republish_same
        try:
            result = pipeline.parse_batch(batch_no, "same-rule")
        finally:
            type(pipeline).parse_midpoint_hook = None
        assert result["ok"] is False
        assert _formal_entities(batch_no) == []


class TestFullScenario:
    """题面要求的完整核对路径：空规则名 → 不存在名称 → 发布 → 不同字段顺序重做。"""

    def test_end_to_end_journal_and_detail_values(self):
        batch_no = _setup_batch(
            "e2e-src",
            ["2026-01-15T10:30:00|INFO|server started",
             "2026-01-15T10:31:00|WARN|memory high"],
        )

        first = pipeline.parse_batch(batch_no, "")
        assert first["ok"] is True
        assert first["rule_verification"]["mode"] == "default"

        second = pipeline.parse_batch(batch_no, "not-a-rule")
        assert second["ok"] is False
        assert _formal_entities(batch_no) == []
        journal = _journal(batch_no)[-1]
        assert journal["rule_reason"] == "not_found"
        pending = json.loads(journal["pending_raw_rows"])
        assert [row["line_no"] for row in pending] == ["1", "2"]

        _create_rule("named-rule", "ts,sev,msg")
        third = pipeline.parse_batch(batch_no, "named-rule")
        assert third["ok"] is True
        assert third["rule_verification"]["field_order"] == ["ts", "sev", "msg"]

        detail = sorted(
            (json.loads(e["标准化字段集"]) for e in _formal_entities(batch_no)),
            key=lambda f: f["ts"],
        )
        assert detail == [
            {"ts": "2026-01-15T10:30:00", "sev": "INFO", "msg": "server started"},
            {"ts": "2026-01-15T10:31:00", "sev": "WARN", "msg": "memory high"},
        ]
        # 原始行号与原始记录 ID 不被补做改写。
        assert [r["原始行号"] for r in _raw(batch_no)] == ["1", "2"]
