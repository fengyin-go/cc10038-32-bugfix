"""解析规则领域服务：发布/弃用动作要同步递增规则版本并刷新核验指纹。"""
from __future__ import annotations

from typing import Any

from app.services import rule_proof as rp
from app.services.base import BaseModuleService
from app.store import store


class FieldParserService(BaseModuleService):
    MODULE = "field_parser"
    ENTITY = "解析规则"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["规则名称", "适用数据源", "输入格式", "输出字段列表", "解析引擎"]
    REQUIRED_FIELDS = ["规则名称", "适用数据源", "输入格式"]
    STATUS_ORDER = ["草稿", "已发布", "已弃用"]
    ACTION_RULES = {"发布规则": "已发布", "弃用规则": "已弃用"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """规则发布/弃用会改变解析结果的可信身份，因此必须走版本化路径。"""
        with store.lock:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if action == "发布规则":
                rp.mark_published(entry)
                return entry, f"{self.ENTITY}已{action}（版本 v{entry['rule_version']}）"
            if action == "弃用规则":
                rp.mark_deprecated(entry)
                return entry, f"{self.ENTITY}已{action}（版本 v{entry['rule_version']}）"
            return super().run_action(entry_id, action)
