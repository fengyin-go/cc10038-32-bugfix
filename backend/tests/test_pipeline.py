"""跨模块数据流业务行为测试：covering version tracking, failure recovery,
batch deduplication, watermark management, schema version evolution, and
cross-service data consumption chains (source_registry → ingest_batch →
raw_records → field_parser → parsed_entity → dedup_engine →
aggregation_view → data_export)."""
from __future__ import annotations

import json
import pytest

from app.store import store


class TestSourceRegistry:
    """数据源登记与验证测试。"""

    def test_register_new_source(self, pipeline):
        result = pipeline.register_source("app-error-log", "http", "beijing")
        assert result["ok"] is True
        assert result["entry"]["来源标识"] == "app-error-log"
        assert result["entry"]["status"] == "待验证"
        assert result["entry"]["source_version"] == 1

    def test_register_duplicate_source(self, pipeline):
        pipeline.register_source("duplicate-src", "http", "shanghai")
        result = pipeline.register_source("duplicate-src", "grpc", "beijing")
        assert result["ok"] is False
        assert "已登记" in result["message"]

    def test_validate_existing_source(self, pipeline):
        pipeline.register_source("to-validate", "http", "default")
        result = pipeline.validate_source("to-validate")
        assert result["ok"] is True
        assert result["entry"]["status"] == "已验证"
        assert result["entry"]["source_version"] == 2

    def test_validate_nonexistent_source(self, pipeline):
        result = pipeline.validate_source("ghost-source")
        assert result["ok"] is False

    def test_validate_increments_version(self, pipeline):
        pipeline.register_source("version-src", "http", "default")
        pipeline.validate_source("version-src")
        entry = pipeline.validate_source("version-src")["entry"]
        assert entry["source_version"] >= 2


class TestIngestBatch:
    """采集批次启动测试。"""

    def test_start_batch_unregistered_source(self, pipeline):
        result = pipeline.start_ingest_batch("missing-src", "2026Q1", [])
        assert result["ok"] is False

    def test_start_batch_unvalidated_source(self, pipeline):
        pipeline.register_source("unvalidated-src", "http", "default")
        result = pipeline.start_ingest_batch("unvalidated-src", "2026Q1", [])
        assert result["ok"] is False
        assert "未通过验证" in result["message"]

    def test_start_batch_success(self, pipeline, sample_raw_logs):
        pipeline.register_source("batch-src", "http", "default")
        pipeline.validate_source("batch-src")
        result = pipeline.start_ingest_batch("batch-src", "2026-01-15", sample_raw_logs)
        assert result["ok"] is True
        assert result["record_count"] == 5
        assert result["batch"]["status"] == "采集中"

    def test_batch_creates_raw_records(self, pipeline, sample_raw_logs):
        pipeline.register_source("record-src", "http", "default")
        pipeline.validate_source("record-src")
        result = pipeline.start_ingest_batch("record-src", "2026-01-15", sample_raw_logs)
        batch_no = result["batch"]["批次编号"]
        raw = store.rows("raw_records")
        batch_raw = [r for r in raw if r.get("批次编号") == batch_no]
        assert len(batch_raw) == 5
        for r in batch_raw:
            assert r["status"] == "待解析"

    def test_no_duplicate_active_batch(self, pipeline, sample_raw_logs):
        pipeline.register_source("single-src", "http", "default")
        pipeline.validate_source("single-src")
        pipeline.start_ingest_batch("single-src", "2026Q1", sample_raw_logs[:2])
        result = pipeline.start_ingest_batch("single-src", "2026Q1", sample_raw_logs[2:])
        assert result["ok"] is False
        assert "进行中的批次" in result["message"]

    def test_batch_records_source_version(self, pipeline, sample_raw_logs):
        pipeline.register_source("ver-track-src", "http", "default")
        pipeline.validate_source("ver-track-src")
        result = pipeline.start_ingest_batch("ver-track-src", "2026Q1", sample_raw_logs[:1])
        assert result["batch"]["source_version"] == 2


class TestFieldParser:
    """字段解析测试。"""

    def test_parse_batch_no_records(self, pipeline):
        result = pipeline.parse_batch("NONEXIST-BATCH")
        assert result["ok"] is False

    def test_parse_batch_no_parser_rule_uses_default(self, pipeline, sample_raw_logs):
        pipeline.register_source("parse-src", "http", "default")
        pipeline.validate_source("parse-src")
        batch = pipeline.start_ingest_batch("parse-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]

        result = pipeline.parse_batch(batch_no)
        assert result["ok"] is True
        assert result["success"] > 0

    def test_parse_batch_sets_raw_status(self, pipeline, sample_raw_logs):
        pipeline.register_source("status-src", "http", "default")
        pipeline.validate_source("status-src")
        batch = pipeline.start_ingest_batch("status-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        pipeline.parse_batch(batch_no)
        raw = store.rows("raw_records")
        batch_raw = [r for r in raw if r.get("批次编号") == batch_no]
        assert all(r["status"] == "已解析" for r in batch_raw)

    def test_parse_creates_parsed_entities(self, pipeline, sample_raw_logs):
        pipeline.register_source("ent-src", "http", "default")
        pipeline.validate_source("ent-src")
        batch = pipeline.start_ingest_batch("ent-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        pipeline.parse_batch(batch_no)
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 2
        for p in parsed:
            assert p["status"] == "待校验"

    def test_parse_journals_created(self, pipeline, sample_raw_logs):
        pipeline.register_source("journal-src", "http", "default")
        pipeline.validate_source("journal-src")
        batch = pipeline.start_ingest_batch("journal-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        before = len(store.rows("parse_journal"))
        pipeline.parse_batch(batch_no)
        after = len(store.rows("parse_journal"))
        assert after > before


class TestValidation:
    """质量校验测试。"""

    def test_validate_entities(self, pipeline, sample_raw_logs):
        pipeline.register_source("valid-src", "http", "default")
        pipeline.validate_source("valid-src")
        batch = pipeline.start_ingest_batch("valid-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)

        result = pipeline.validate_entities(batch_no)
        assert result["ok"] is True
        assert result["passed"] == 2

    def test_validate_sets_status(self, pipeline, sample_raw_logs):
        pipeline.register_source("vstat-src", "http", "default")
        pipeline.validate_source("vstat-src")
        batch = pipeline.start_ingest_batch("vstat-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert all(p["status"] == "校验通过" for p in parsed)


class TestDeduplication:
    """去重引擎测试。"""

    def test_dedup_no_duplicates(self, pipeline, sample_raw_logs):
        pipeline.register_source("nodup-src", "http", "default")
        pipeline.validate_source("nodup-src")
        batch = pipeline.start_ingest_batch("nodup-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["time", "level"], "batch", "keep_first")
        assert result["ok"] is True
        assert result["duplicates_found"] == 0

    def test_dedup_with_duplicates(self, pipeline):
        pipeline.register_source("dup-src", "http", "default")
        pipeline.validate_source("dup-src")
        # 提交相同日志
        same_logs = [
            {"body": "t1|INFO|msg1"},
            {"body": "t1|INFO|msg1"},  # duplicate
            {"body": "t1|WARN|msg2"},
        ]
        batch = pipeline.start_ingest_batch("dup-src", "2026Q1", same_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["level"], "batch", "keep_first")
        assert result["ok"] is True
        assert result["duplicates_found"] > 0

    def test_dedup_keep_last(self, pipeline):
        pipeline.register_source("kl-src", "http", "default")
        pipeline.validate_source("kl-src")
        logs = [
            {"body": "t1|INFO|first"},
            {"body": "t1|INFO|second"},
        ]
        batch = pipeline.start_ingest_batch("kl-src", "2026Q1", logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["level"], "batch", "keep_last")
        assert result["ok"] is True


class TestAggregation:
    """聚合视图测试。"""

    def test_refresh_aggregation(self, pipeline, sample_raw_logs):
        pipeline.register_source("agg-src", "http", "default")
        pipeline.validate_source("agg-src")
        batch = pipeline.start_ingest_batch("agg-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.refresh_aggregation("test-view", "level", "time", "count")
        assert result["ok"] is True
        assert "view" in result
        assert result["dimensions"] > 0

    def test_aggregation_dimensions(self, pipeline, sample_raw_logs):
        pipeline.register_source("dim-src", "http", "default")
        pipeline.validate_source("dim-src")
        batch = pipeline.start_ingest_batch("dim-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.refresh_aggregation("dim-view", "level", "time", "count")
        results = result["view"]["results"]
        assert "INFO" in results
        assert results["INFO"]["count"] == 2  # two INFO logs


class TestDataExport:
    """数据导出与血缘测试。"""

    def test_build_export(self, pipeline, sample_raw_logs):
        pipeline.register_source("export-src", "http", "default")
        pipeline.validate_source("export-src")
        batch = pipeline.start_ingest_batch("export-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")

        result = pipeline.build_export(f"view-{batch_no}", "csv")
        assert result["ok"] is True
        assert result["export"]["status"] == "已完成"

    def test_export_creates_lineage(self, pipeline, sample_raw_logs):
        pipeline.register_source("lin-src", "http", "default")
        pipeline.validate_source("lin-src")
        batch = pipeline.start_ingest_batch("lin-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")

        lineage_before = len(store.rows("data_lineage"))
        pipeline.build_export(f"view-{batch_no}", "csv")
        lineage_after = len(store.rows("data_lineage"))
        assert lineage_after > lineage_before
        last = store.rows("data_lineage")[-1]
        assert last["export_format"] == "csv"


class TestWatermarkManagement:
    """水位管理测试。"""

    def test_advance_watermark_new_source(self, pipeline):
        result = pipeline.advance_watermark("wm-src-1", "INGE-0001", "2026-01-01T00:00:00")
        assert result["ok"] is True
        assert result["watermark"]["数据源"] == "wm-src-1"

    def test_advance_watermark_progress(self, pipeline):
        pipeline.advance_watermark("wm-src-2", "INGE-0001", "2026-01-01T00:00:00")
        result = pipeline.advance_watermark("wm-src-2", "INGE-0002", "2026-01-02T00:00:00")
        assert result["ok"] is True

    def test_watermark_rollback_prevention(self, pipeline):
        pipeline.advance_watermark("wm-src-3", "INGE-0001", "2026-01-05T00:00:00")
        result = pipeline.advance_watermark("wm-src-3", "INGE-0002", "2026-01-01T00:00:00")
        assert result["ok"] is False
        assert "倒退" in result["message"]
        wm = result["message"]
        assert "阻止" in wm

    def test_watermark_rollback_flags_abnormal(self, pipeline):
        pipeline.advance_watermark("wm-src-4", "INGE-0001", "2026-02-01T00:00:00")
        pipeline.advance_watermark("wm-src-4", "INGE-0002", "2026-01-01T00:00:00")
        wms = store.rows("watermark_mgr")
        wm = [w for w in wms if w["数据源"] == "wm-src-4"][0]
        assert wm["status"] == "异常回退"
        assert wm["abnormal"] is True


class TestSchemaEvolution:
    """Schema 版本演进测试。"""

    def test_evolve_schema_new(self, pipeline):
        result = pipeline.evolve_schema("v2.0", "app-logs", ["time", "level", "message", "host"], True)
        assert result["ok"] is True
        assert result["schema"]["版本号"] == "v2.0"
        assert result["schema"]["status"] == "已发布"

    def test_evolve_schema_duplicate(self, pipeline):
        pipeline.evolve_schema("v3.0", "app-logs", ["time", "msg"], True)
        result = pipeline.evolve_schema("v3.0", "app-logs", ["time", "msg"], True)
        assert result["ok"] is False
        assert "已存在" in result["message"]

    def test_evolve_schema_non_backward_compatible(self, pipeline):
        result = pipeline.evolve_schema("v4.0", "db-logs", ["timestamp", "query", "duration"], False)
        assert result["ok"] is True
        assert result["schema"]["向后兼容标记"] == "否"


class TestReplayEngine:
    """回溯重放引擎测试。"""

    def test_replay_batch(self, pipeline, sample_raw_logs):
        pipeline.register_source("replay-src", "http", "default")
        pipeline.validate_source("replay-src")
        batch = pipeline.start_ingest_batch("replay-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        # 重放必须先有可核验的命名规则：找不到就报错，不能只改“解析规则版本”标签。
        from app.services import SERVICES
        service = SERVICES["field_parser"]
        rule, _ = service.create_entry({
            "规则名称": "new-rule-v2",
            "适用数据源": "replay-src",
            "输入格式": "pipe",
            "输出字段列表": "ts,sev,msg",
            "解析引擎": "builtin",
        })
        service.run_action(rule["id"], "发布规则")

        result = pipeline.replay_batch(batch_no, "new-rule-v2")
        assert result["ok"] is True
        assert result["replayed_count"] > 0
        assert result["replay"]["status"] == "已完成"

    def test_replay_batch_unknown_rule_fails(self, pipeline, sample_raw_logs):
        pipeline.register_source("replay-miss-src", "http", "default")
        pipeline.validate_source("replay-miss-src")
        batch = pipeline.start_ingest_batch("replay-miss-src", "2026Q1", sample_raw_logs[:1])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)

        result = pipeline.replay_batch(batch_no, "no-such-rule")
        assert result["ok"] is False
        assert "不存在" in result["message"]


class TestFailureRecovery:
    """失败恢复测试。"""

    def test_recover_batch(self, pipeline):
        pipeline.register_source("recv-src", "http", "default")
        pipeline.validate_source("recv-src")
        logs = [
            {"body": "bad|format"},  # 只有单字段，解析会失败
            {"body": "t1|INFO|msg"},
        ]
        batch = pipeline.start_ingest_batch("recv-src", "2026Q1", logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)

        result = pipeline.recover_batch(batch_no)
        assert result["ok"] is True
        assert isinstance(result["recovered"], int)


class TestFullPipeline:
    """全链路流水线测试。"""

    def test_run_full_pipeline(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "full-pipeline-src",
            "2026-01-15",
            sample_raw_logs[:3],
        )
        assert result["ok"] is True
        steps = result["steps"]
        assert "source_registry" in steps
        assert "ingest_batch" in steps
        assert "field_parser" in steps
        assert "validate" in steps
        assert "dedup" in steps
        assert "aggregation" in steps
        assert "export" in steps
        assert "watermark" in steps

    def test_full_pipeline_data_lineage(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "lineage-check-src",
            "2026-01-15",
            sample_raw_logs[:2],
        )
        assert result["ok"] is True
        lineages = store.rows("data_lineage")
        assert len(lineages) > 0

    def test_full_pipeline_parsed_entities_refer_to_batch(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "ref-check-src",
            "2026-01-15",
            sample_raw_logs[:3],
        )
        batch_no = result["batch_no"]
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 3

    def test_full_pipeline_watermark_advanced(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "wm-check-src",
            "2026-01-15",
            sample_raw_logs[:2],
        )
        assert result["ok"] is True
        wms = store.rows("watermark_mgr")
        wm = [w for w in wms if w["数据源"] == "wm-check-src"]
        assert len(wm) == 1
        assert wm[0]["status"] == "正常"


class TestCrossModuleSnapshot:
    """跨模块数据快照测试。"""

    def test_snapshot_contains_all_modules(self, pipeline, sample_raw_logs):
        pipeline.run_full_pipeline("snap-src", "2026Q1", sample_raw_logs[:3])

        from app.services.base import BaseModuleService
        from app.services import SERVICES
        snapshots = {key: svc.domain_snapshot() for key, svc in SERVICES.items()}

        assert "source_registry" in snapshots
        assert "ingest_batch" in snapshots
        assert "raw_records" in snapshots
        assert "parsed_entity" in snapshots
        assert "dedup_engine" in snapshots
        assert "aggregation_view" in snapshots
        assert "data_export" in snapshots
        assert "data_lineage" in snapshots
        assert "watermark_mgr" in snapshots

    def test_snapshot_data_consistency(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline("consistency-src", "2026Q1", sample_raw_logs[:3])
        batch_no = result["batch_no"]

        from app.services import SERVICES
        parsed_snap = SERVICES["parsed_entity"].domain_snapshot()
        assert parsed_snap["count"] >= 3

        raw_snap = SERVICES["raw_records"].domain_snapshot()
        assert raw_snap["count"] > 0