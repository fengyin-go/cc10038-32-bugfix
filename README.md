# 多源日志采集与解析中心

管理多来源日志接入、字段解析、批量去重和水位推进的数据采集平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8013/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5193/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8013`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源登记 | `source_registry` | 数据源 | 来源标识、接入协议、连接凭证 |
| 采集批次 | `ingest_batch` | 采集批次 | 批次编号、所属数据源、采集时间窗口 |
| 字段解析规则 | `field_parser` | 解析规则 | 规则名称、适用数据源、输入格式 |
| 原始日志记录 | `raw_records` | 日志记录 | 批次编号、原始行号、采集时间戳 |
| 标准化明细 | `parsed_entity` | 标准化记录 | 来源批次、原始记录ID、解析规则版本 |
| 去重引擎 | `dedup_engine` | 去重策略 | 策略名称、去重键字段、去重窗口 |
| 水位管理器 | `watermark_mgr` | 采集水位 | 数据源、最后成功批次、当前水位值 |
| 重试队列 | `retry_queue` | 重试任务 | 失败记录ID、失败原因、重试次数 |
| 格式适配层 | `format_adapter` | 格式适配 | 输入格式名、适配器类型、目标格式 |
| 质量校验规则 | `quality_check` | 校验规则 | 规则名称、校验类型、阈值参数 |
| 归档策略 | `data_archive` | 归档策略 | 策略名称、归档周期、保留时长 |
| 限流控制器 | `rate_limiter` | 限流规则 | 数据源、每秒上限、突发并发 |
| 解析操作日志 | `parse_journal` | 解析日志 | 操作类型、批次编号、执行时间 |
| RPA采集流程 | `rpa_workflow` | 采集流程 | 流程名称、目标系统、触发方式 |
| Schema版本 | `schema_version` | Schema版本 | 版本号、适用数据源、字段定义 |
| 采集告警策略 | `alert_policy` | 告警策略 | 策略名称、监控指标、告警阈值 |
| 数据导出 | `data_export` | 导出任务 | 导出范围、目标格式、过滤条件 |
| 回溯重放引擎 | `replay_engine` | 重放任务 | 历史批次范围、新解析规则版本、重放状态 |
| 聚合查询视图 | `aggregation_view` | 聚合视图 | 视图名称、聚合维度、指标定义 |
| 采集血缘 | `data_lineage` | 血缘关系 | 来源批次、解析规则版本、标准化版本 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 解析规则核验

解析入口（`app/services/pipeline.py` 的 `parse_batch`）不再静默回退：

- **空规则名 = 未指定**：沿用已有默认映射（有已发布规则时优先用它），保持历史兼容。
- **非空规则名必须精确命中一条已发布规则**：不存在、未发布、同名多条、没有可核验的
  输出字段列表都会直接报错，不产出任何正式明细，也不会回退到默认字段。
- 规则快照冻结在 `app/services/rule_proof.py`，每条明细都带 `rule_version`、
  `rule_digest`、`rule_field_signature`、`rule_proof`、`formal_detail`，可与解析
  操作日志逐条核对。
- 解析计算在锁外进行、提交前在锁内复核规则指纹。指定规则恰好在解析期间被重新发布时，
  本次输出标记为 `unproven`，不进入正式明细；旧明细一并退出正式集，原文退回待解析。
- 失败/待补做的原始行按规则核验结果登记进处置清单（`retry_queue`），保留可读原文与
  原始行号；补做沿用同一 `raw id`，不会改写原始日志编号，也不会把同一次成功计两遍。
- 发布规则/弃用规则（`field_parser` 服务动作）会递增 `rule_version` 并刷新指纹。

核对接口：

```bash
# 按批次查看每一次解析的规则核验结果与明细字段来源
curl http://127.0.0.1:8013/api/pipeline/batch/INGE-0004/rule-verification

# 补做：发布命名规则后重新提交原批
curl -X POST http://127.0.0.1:8013/api/pipeline/batch/recover?batch_no=INGE-0004&parser_rule_name=app-log-v2
```

领域：数据工程
子方向：数据采集、爬虫与RPA
