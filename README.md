# 制造计划时序数据校正

FastAPI + SQLite + Docker Compose 的分层产线计划校正工具。计划分析员上传：

1. **层级映射**：`node_id, parent_id, level, name, weight`（产线 → 车间 → 工厂，单根树）
2. **产线预测**：`node_id, period, value`（每条产线每个时段一个值）
3. **已发布车间/工厂预测**：`node_id, period, value` 或 `level, period, value`

系统按层级重算总量、找出与已发布值不一致的时段，按节点权重最小化加权调整平方和，
生成各层数量一致的新快照；网页按节点展示每个时段的调整来源，结果可下载为 CSV。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

首次使用：新建数据集 → 分别上传 `sample_data/mapping.csv`、`sample_data/line_forecast.csv`、
`sample_data/published.csv` → 点击「运行校正」→ 检查「调整来源」→「发布此预览为快照」。
发布后在同一页的「交付排产（CSV）」卡片选择时段，生成交付单。

本地不带 Docker 运行：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATABASE_PATH=./app.db uvicorn app.main:create_app --factory --port 8000
pip install -r requirements-dev.txt && pytest -q
```

## 交付取数（给排产员）

数据集页第 4 张卡片或 `/datasets/{id}/deliver` 提供交付入口。选择一组时段后生成 CSV：

```csv
period,line_id,line_name,corrected_value,snapshot_version
2026-01,L1,冲压一线,122.500000,1
2026-01,L2,冲压二线,157.500000,1
```

每行一个「时段 × 产线」，`corrected_value` 取快照里的校正后产线值（不是上传基线），
`snapshot_version` 标出这一行来自哪一版快照。

| 规则 | 行为 |
| --- | --- |
| 已交付时段 | 永远沿用**首次交付**的快照；重新发布、换版、服务重启后都不变 |
| 新时段 | 统一锁定到**本单生效时**的最新发布快照（订单事务开始时读到的最大版本） |
| 制表期间发布新快照 | 不影响本单：本单已解析的快照版本不会后移 |
| 两单同时交付同一新时段 | SQLite `BEGIN IMMEDIATE` 串行化；先提交者写入锁定记录，后提交者读到并复用同一快照，两单 CSV 一致 |
| 应取快照缺少所选时段 | 列出全部缺失时段（含应取版本），**整单退回**，不写订单/明细/锁定记录 |
| 制表失败（缺数、结构异常） | 同样整单退回，只留一条 `delivery_rejected` 操作记录 |

「预览取数」按钮只做只读预演：显示每个时段将沿用的版本或新锁定到的版本、缺哪些时段，
不写任何数据。交付单正文随订单一起持久化，重新下载旧交付单得到的是当时那份字节不变的文件。
单元格以 `=`/`+`/`-`/`@` 开头时会加 `'` 前缀，避免在表格软件里被当成公式。

## 校正模型

对每个时段独立求解。设产线值为决策变量 `x_l`，内部节点值恒等于其产线后代之和，
因此层级一致性 `父 = Σ子` 天然成立。目标函数为

```
Σ_节点 w_node · (x_node − base_node)²
```

其中 `base_node` 为上传的产线预测在节点上的加总。已发布值作为线性等式约束
（节点单一值，或某层全部节点的合计）。两者都是 `x_l` 的线性函数，于是问题化为
**带线性等式约束的加权最小二乘**，用零空间约化求解（numpy SVD + 线性方程组），
不可行时明确报错而不是悄悄违反发布值。

权重语义：权重越大越"不愿被调整"，代价按 `w·Δ²` 计入；因此同等条件下低权重节点承担
更多平移。权重为 0 的节点不参与代价（求解时用极小正则项保证结果唯一、可复现）。

### 校验与拒绝发布

上传映射时即校验并在不合规时**整次拒绝**：

- 映射成环（含自环）
- 同一节点多个父节点
- 父节点不存在
- 缺层：根不是 plant / 车间下没有产线 / 层级为空 / 层级与树深度不符 / 多根
- 使用已发布预测但节点或层级不存在、同一节点同一时段两个不同发布值
- 父节点与其全部子节点的已发布值互相矛盾（预先给出可读错误）
- 约束矩阵不相容（例如工厂合计与车间合计冲突）→ `OptimizationError`

### 版本、预览与失败语义

- 每个数据集有单调递增的 `input_version`；上传内容有变化才 +1。
- 预览记录输入版本 + 三个文件的 sha256。发布时在同一个写事务内重新校验版本与哈希，
  任何一项不匹配 → `StalePreviewError`，**旧预览不得发布**。
- 校正失败不写入 preview 也不写快照；上一版已发布快照原样保留。
- 快照不可变：每次发布新建版本并记录父快照，旧版本可继续查看/下载。
- 上传 / 预览 / 发布在同一 SQLite 写事务中完成（`BEGIN IMMEDIATE`），
  并发上传与预览不会混用版本。

## 页面

- `/` 数据集列表
- `/datasets/{id}` 上传、校正、发布、历史快照、交付排产、操作记录
- `/datasets/{id}/deliver` 独立的交付取数页（也可从数据集页第 4 张卡片进入）
- `/datasets/{id}/snapshots/{v}` 只读快照页
- 每个时段折叠面板显示：不一致时段明细（基线 vs 已发布 vs 差额）、已发布约束、
  按节点的调整来源（`直接发布` / `层级总量` / `层级平衡`）、产线级溯源、校正后各层数值
- 下载：`/datasets/{id}/download/snapshot.csv`、`adjustments.csv`、`conflicts.csv`
  （带 `?version=N` 或 `?preview=N`）；交付单 `/{id}/deliveries/{order}/download`

## JSON API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/datasets` | 新建数据集，body `{"name": "..."}` |
| GET | `/api/datasets/{id}` | 上传状态、预览、快照、事件 |
| POST | `/api/datasets/{id}/upload?kind=mapping\|line_forecast\|published` | multipart `file` |
| POST | `/api/datasets/{id}/preview` | 运行校正 |
| POST | `/api/datasets/{id}/publish` | body `{"preview_id": N}` |
| GET | `/api/datasets/{id}/snapshots` | 快照列表 |
| GET | `/api/datasets/{id}/snapshots/{v}` | 快照详情 |
| POST | `/api/datasets/{id}/deliver/preview` | 只读预演，body `{"periods": ["2026-01"]}` |
| POST | `/api/datasets/{id}/deliver` | 生成交付单，body `{"periods": [...], "note": "..."}` |
| GET | `/api/datasets/{id}/deliveries` | 交付单与逐时段快照版本 |
| GET | `/healthz` | 健康检查 |

错误响应：`{"error": "...", "code": "stale_preview_error"}`，HTTP 400。
缺时段退回额外带 `missing: [{"period", "snapshot_version", "reason"}]`，
`code` 为 `delivery_missing_periods`。

## 测试

```bash
pytest -q
```

覆盖：映射成环/多父/缺层、CSV 解析、约束可行与不可行、权重变化对调整分配的影响、
随机树与独立 KKT 解交叉验证、版本失效与旧预览拒绝发布、失败保留上一版、并发、
HTTP 页面与下载；交付侧覆盖首次交付锁定、换版后重叠时段仍取旧快照、新时段取生效版本、
制表期间发布不影响本单、并发同抢新时段只锁一次且两单同版、缺时段/制表失败整单退回、
重启后锁定仍然有效。

## 目录

```
app/
  main.py        FastAPI 路由（页面 + JSON API）
  service.py     上传/预览/发布编排、版本与失效判定、CSV 导出
  delivery.py    交付单：时段 → 快照解析、首次交付锁定、整单制表/退回
  reconcile.py   加权最小二乘校正与一致性校验
  hierarchy.py   树构建与结构校验
  csv_io.py      CSV 解析
  db.py          SQLite schema 与事务
  templates/     Jinja2 页面
  static/        样式
tests/           96 项自动化测试
sample_data/     可直接上传的示例文件（含一个成环反例）
```

## 限制

- 单进程 SQLite；水平扩展需要换成网络数据库并加分布式锁/乐观版本。
- 已发布 `line` 层级合计会锁定每条产线（合计拆分规则未定义，不擅自猜测）。
- 不保存用户账号与权限；如需多人隔离应在反向代理层加认证。
- 不支持按周/季度等非 ISO 周期的自然排序（按字符串前缀解析年月）。
- 交付锁定是「数据集 + 时段」粒度；同一时段一旦交付就不能换到新快照（需要人工迁移的场景未实现）。
- 当前沙箱内没有 Docker 守护进程与容器运行时，`Dockerfile` / `docker-compose.yml` 已写好但未实际构建镜像；
  应用已在本机 venv 中以 uvicorn 启动（同一 `DATABASE_PATH`），完成上传/校正/发布/交付/缺数退回/
  并发交付/重启后锁定复核的全链路手工验证。
