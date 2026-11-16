# 鲜达冷链 · 温控与损耗关联簿

给鲜达冷链 12 条线路的运营分析组，用运输背景解释温控与货损之间的关联。上游班次和温控记录进入工作簿，分析员检查缺失、执行真实统计，负责人把改善建议签核给线路主管。

原流程是调度员整理工作簿，分析员复制公式，主管在聊天记录里拍板。三个痛点是缺失样本被静默删除、统计口径容易改错、建议与研究证据分离。此项目提供班次清单、样本质检、关联分析、改善建议四个业务页面，贯穿四类实体；并含登录、审计、CSV 之外的 JSON 研究导出。线路主管需要的货损汇总由独立的「货损同步」页面维护：按线路 + 温区汇总样本总数、缺失数与货损合计，推送到单独启动的接收端并对账。

## 启动

```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```

访问 http://localhost:18201 ，后端 http://localhost:19201/docs ，接收端 http://localhost:17201/api/ledger ，MySQL 映射 17701。恰好六个健康检查服务 frontend/backend/worker/db/redis/receiver。backend 与 worker 共享 `/artifacts`；Redis appendonly；数据库与接收端账本各自命名卷。停止与清空演示数据：`docker compose down -v --remove-orphans`。

## 演示账号和权限

| 邮箱 | 密码 | 权限 |
|---|---|---|
| admin@demo.local | Demo123! | 全部读写、负责人签核与审计 |
| analyst@demo.local | Demo123! | 创建和修改班次/样本、提交研究、起草建议；不能签核 |
| viewer@demo.local | Demo123! | 所有业务数据只读与下载；禁止任何写入和计算任务提交 |

JWT 有效期 8 小时，后端查数据库角色，不信任前端传入角色。密码 PBKDF2；演示使用固定盐和可公开的 JWT key，部署时通过环境变量替换 key，并为正式账号改成每用户随机盐。首次启动 SEED_DEMO=true 自动导入；失败记日志。自然键唯一约束、只插入缺失数据，重复执行不覆盖用户修改。

## 一天的业务剧本

1. 08:50 viewer 核对昨日 XD-1115-001 班次。
2. 09:00 analyst 查看 60 班次、60 条样本和缺失数据。
3. 09:10 创建 XD-1115-061，鲜达线路 01 / 冷藏。
4. 09:20 采集暴露 32 分钟、超温 3 次、货损 4.2% 的样本。
5. 09:30 创建冷藏温区暴露时长研究。
6. 09:40 Redis 队列交给 worker 计算；刷新查看真实结果。
7. 10:00 阅读散点、p 值、配对数及混杂因素警示。
8. 10:10 起草“复核线路 01 装卸等待时长”等建议，admin 签核。
9. 10:20 viewer 下载 coldchain-link-<研究id>.json 供线路主管复核。
10. 10:30 线路主管在「货损同步」查看各线路 / 温区的样本总数、缺失数、货损合计与两端差额；有差额可补发。

## 货损汇总与接收端

组键 = 线路 + 温区。汇总口径：样本总数 = 组内班次下的样本条数；缺失数 = 暴露分钟 / 超温次数 / 货损率任一为空的样本数；货损合计 = 非空货损率之和（百分数直接累加）。只有三项都齐的样本既不计缺失，也照常计入总数。

同步路径：样本新增或更正、班次改线路 / 温区时，在同一个数据库事务里保存业务数据并重算受影响组——更正跨组时旧组和新组都校正——然后写一条源版本与待发送记录。事务提交即使接收端离线也成功，录入不被阻塞；发送线程随后按 outbox 推送，失败只标为待确认并按 `SYNC_RETRY_SECONDS` 退避重试。

- 消息是每个组的绝对量快照加单调递增版本；接收端以 (组, 版本) 为水位，只接受更大版本，并按回执号 `receipt_id`（由组键 + 版本确定）去重。因此「已入账却丢回执」的重试不会重复累计；并发发送或乱序抵达时旧版本被拒绝，最终与源端一致。
- 页面区分待发送 / 待确认 / 已对账，并展示两端差额（总数、缺失、货损合计）。补发只把源端当前快照重新排队，不改写、不覆盖接收端已经入账的更新结果。
- 源端重启会把停在「已发出待回执」的记录退回重试；接收端账本在 SQLite 命名卷中，重启不丢。`python -I backend/tests/test_loss_sync.py` 用 SQLite 源库与本仓库接收端在本地演练更正跨组、丢回执、乱序并发与两端重启。接收端演练开关 `POST /api/chaos/drop-receipts {"enabled": true}` 会先入账再丢回执；`POST /api/chaos/reset` 清账本。

接收端是独立进程 / 独立 compose 服务（`receiver/`，映射 17201，账本卷 `receiverdata`），不读源库、不依赖 Redis，源端通过 `RECEIVER_URL` 推送。部署时接收端账本应单独备份；源库重建后首次写入会取更高版本自动收敛，若接收端版本反而更高，页面显式提示差额待确认。

## 算法与边界

Python SciPy 计算 Pearson r = sum((x-xmean)(y-ymean))/sqrt(sum((x-xmean)^2)sum((y-ymean)^2))；Spearman 使用平均秩处理并列值，再对秩求 Pearson。返回双侧 p 值、有效配对数、排除数、散点和原始快照。输入为同温区样本，x 为暴露分钟或超温次数，y 固定为货损率%。只有 x/y 均非空的配对进入计算；不做插补。少于3对、任一变量为常量，任务明确 failed。相关性不等于因果，不自动决定线路处罚。60条 seed 使用 Random(3102)；冷藏和冷冻各有一条缺失暴露，10条常温对照的暴露均20分钟，用于失败诊断。研究创建时冻结输入，后续修改样本不改变已有结果。任务失败可修复原始记录再新建研究。worker 启动恢复 queued/running 任务；单 worker 模式，不承诺多副本 exactly-once。

## 技术和目录

Python3.11 / FastAPI / SQLAlchemy / SciPy / PyMySQL / PyJWT；MySQL8.0；Redis7；Nginx1.27；原生 HTML、CSS、JS ES modules。前端无构建步骤。

- backend/src/models、schemas、services、controllers、routes 分层承载实体与业务。
- backend/src/middlewares 为 JWT 角色鉴权、请求 ID、安全响应头；异常返回可恢复错误。
- backend/scripts/seed_demo.py 导入数据（含为历史线路 / 温区回填货损汇总）；migrations/001_initial.sql 说明建表策略，当前由 SQLAlchemy create_all 建表，尚未采用版本迁移工具。
- backend/src/services/sync.py 为货损汇总与 outbox 发送；receiver/app.py 为独立接收端与演练开关。
- frontend/src/api、stores、types、components/common、hooks、pages、router、utils、constants 为请求、状态、契约、共享表格/标签/字段、表单、五页面（含货损同步）与路由。
- 共享枚举 ROLES、STATES、ZONES：backend/src/constants/enums.py 与 frontend/src/constants/enums.js；校验在 backend/src/schemas/business.py，任务状态在 worker.py，UI 标签在 components/common/status.js。

本地开发：安装 backend/requirements.lock（真实运行环境完整依赖锁定；requirements.txt 为直接依赖）；启动本地 MySQL/Redis，设置 DATABASE_URL/REDIS_URL/JWT_SECRET 后 `uvicorn src.main:app --reload`。前端可由 Nginx 提供，代理 `/api/` 去除前缀转发给后端。

环境变量：COMPOSE_PROJECT_NAME（容器前缀）、FRONTEND_PORT/BACKEND_PORT/DB_PORT/RECEIVER_PORT（映射）、DB_PASSWORD（演示数据库密码）、JWT_SECRET、SEED_DEMO、RECEIVER_URL、SYNC_RETRY_SECONDS。仅内部服务接触 Redis，不映射宿主端口。编辑班次与样本 API 支持 PUT；UI 当前以新增和浏览为主，无批量 CSV 导入、删除、分页或邮件通知。Google Fonts 需联网，无法联网则浏览器回退 serif/sans-serif。证据和测试在批次根 test-artifacts/gb-3102（本地演练脚本在 backend/tests/test_loss_sync.py），不写入源码。

License: MIT。

NETWORK_SUBNET 默认 10.253.102.0/24，适配本机默认地址池耗尽的情况；迁移到其他环境可修改为空闲子网。
