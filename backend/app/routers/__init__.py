"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline_router

# 流水线路由必须先注册：资源路由的 /api/{module}/{entry_id} 只有两段，
# 会把 /api/pipeline/snapshot 之类的固定路径抢先当成“模块 + 记录 ID”。
ROUTERS = [pipeline_router, resource]
