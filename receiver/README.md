# 货损汇总接收端（独立服务）

只接收源端推送的「线路 + 温区」组快照，落 SQLite 账本，供对账与补发演练。

```sh
# 本地直接跑
RECEIVER_DB=/tmp/receiver.sqlite uvicorn app:app --port 7000
# 或随 compose 启动
docker compose up -d receiver
```

接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/ingest` | 接收组快照；按 (组, 版本) 与 receipt_id 幂等 |
| GET | `/api/ledger` | 当前账本（每组的版本与快照） |
| GET | `/api/receipts` | 最近回执 |
| GET | `/api/log` | 接收日志（含被拒的旧版本） |
| POST | `/api/chaos/drop-receipts` | `{"enabled": true}` 先入账再丢回执，演练已入账丢回执 |
| POST | `/api/chaos/reset` | 清空账本 |

演练脚本：`python -I backend/tests/test_loss_sync.py`（SQLite 源库 + 本接收端，真实 HTTP，覆盖更正跨组、丢回执、乱序并发、重启续跑）。
