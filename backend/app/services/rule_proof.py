"""解析规则核验工具：把“用了哪条规则”变成可复核的指纹。

解析链路的静默回退之所以危险，是因为操作日志只记“成功”，却没有任何字段
能证明实际使用的是哪条规则、哪个版本、哪个字段顺序。这里集中生成规则快照
与指纹，解析入口、发布动作和操作日志共用同一套算法，核对时不会各算各的。
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Any

# 未指定规则名时沿用的默认字段映射，保持历史行为不变。
DEFAULT_FIELDS = ["time", "level", "message"]

# 输出字段列表可能用英文/中文逗号或分号分隔。
_FIELD_SEPARATOR = re.compile(r"[,，;；]")


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def normalize_output_fields(raw: Any) -> list[str] | None:
    """把规则里的“输出字段列表”拆成有序字段名。

    返回 None 表示这不是一份可核验的字段列表（空值、占位文本或只有一个
    无法确认顺序的字段）。调用方决定是使用默认映射还是直接报错。
    """
    text = str(raw or "").strip()
    if not text:
        return None
    parts = [part.strip() for part in _FIELD_SEPARATOR.split(text) if part.strip()]
    # 与历史实现保持一致：没有分隔符的占位文本不当作真实字段列表。
    if len(parts) < 2:
        return None
    return parts


def field_signature(fields: list[str]) -> str:
    return ",".join(fields)


def rule_snapshot(rule: dict[str, Any]) -> dict[str, Any]:
    """冻结规则身份。后续用 rule_digest 对比，证明输出是否来自同一版本。"""
    normalized = normalize_output_fields(rule.get("输出字段列表"))
    snapshot = {
        "rule_id": rule.get("id"),
        "rule_name": rule.get("规则名称"),
        "rule_status": rule.get("status"),
        "rule_version": int(rule.get("rule_version", 0) or 0),
        "raw_output_fields": str(rule.get("输出字段列表") or ""),
        "output_fields": list(normalized) if normalized else [],
        "field_signature": field_signature(normalized) if normalized else "",
    }
    snapshot["rule_digest"] = rule_digest(snapshot)
    return snapshot


def rule_digest(snapshot: dict[str, Any]) -> str:
    payload = {
        "rule_id": snapshot.get("rule_id"),
        "rule_name": snapshot.get("rule_name"),
        "rule_status": snapshot.get("rule_status"),
        "rule_version": snapshot.get("rule_version"),
        "field_signature": snapshot.get("field_signature"),
        "raw_output_fields": snapshot.get("raw_output_fields"),
    }
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def parse_body(body: str, field_names: list[str]) -> tuple[dict[str, Any], int, int]:
    """按给定字段顺序把原始正文映射成字段集。

    返回 (字段集, 正文片段数, 字段数)。片段数与字段数是否一致由调用方决定，
    显式规则必须严格一致，默认映射保留历史上的宽松行为。
    """
    parts = body.split("|") if "|" in str(body) else [str(body)]
    fields: dict[str, Any] = {}
    for index, name in enumerate(field_names):
        fields[name] = parts[index].strip() if index < len(parts) else ""
    return fields, len(parts), len(field_names)


def mark_published(rule: dict[str, Any], output_fields: Any = None) -> dict[str, Any]:
    """发布规则：递增版本号并冻结本次发布的指纹。"""
    if output_fields is not None:
        if isinstance(output_fields, (list, tuple)):
            rule["输出字段列表"] = ",".join(str(item).strip() for item in output_fields if str(item).strip())
        else:
            rule["输出字段列表"] = str(output_fields)
    rule["rule_version"] = int(rule.get("rule_version", 0) or 0) + 1
    rule["status"] = "已发布"
    rule["pending"] = False
    rule["abnormal"] = False
    rule["published_at"] = now_iso()
    snapshot = rule_snapshot(rule)
    rule["rule_digest"] = snapshot["rule_digest"]
    return snapshot


def mark_deprecated(rule: dict[str, Any]) -> dict[str, Any]:
    """弃用规则也改变可解析状态，同样递增版本并更新指纹。"""
    rule["rule_version"] = int(rule.get("rule_version", 0) or 0) + 1
    rule["status"] = "已弃用"
    rule["pending"] = True
    rule["abnormal"] = False
    rule["deprecated_at"] = now_iso()
    snapshot = rule_snapshot(rule)
    rule["rule_digest"] = snapshot["rule_digest"]
    return snapshot
