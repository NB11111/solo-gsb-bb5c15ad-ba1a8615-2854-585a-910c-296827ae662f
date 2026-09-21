# Webhook Inbox

基于 FastAPI + SQLite 的 Webhook 事件收件箱。接收事件、幂等去重、按时间游标分页查询、单条确认，数据持久化到 SQLite，启动时自动建表。

## 快速开始（Docker 一键启动）

```bash
docker compose up --build
```

启动后访问：

- API 地址： http://localhost:8000
- 交互式文档（Swagger UI）： http://localhost:8000/docs
- 健康检查： http://localhost:8000/healthz

Compose 将宿主机 **8000 端口**映射到容器 8000 端口，并挂载命名卷 `inbox-data` 持久化 SQLite 数据文件（`/data/events.db`），容器重启数据不丢。

也可以不用 Compose：

```bash
docker build -t webhook-inbox .
docker run -d -p 8000:8000 -v inbox-data:/data webhook-inbox
```

## API 说明

### 提交事件 `POST /events`

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id": "evt_001", "type": "payment.succeeded", "payload": {"order_id": "o_123", "amount": 9900}}'
```

响应（`201 Created`）：

```json
{
  "event_id": "evt_001",
  "type": "payment.succeeded",
  "status": "pending",
  "payload": {"amount": 9900, "order_id": "o_123"},
  "received_at": "2026-09-20T12:34:56.789012Z",
  "acked_at": null
}
```

幂等语义（以 `event_id` 为幂等键）：

| 情况 | 状态码 | 说明 |
|---|---|---|
| 首次提交 | `201` | 事件入库 |
| 相同 `event_id` + 相同内容（含并发重复） | `200` | 只保留首个版本，返回与首次**完全一致**的响应体，并带响应头 `X-Idempotent-Replay: true` |
| 相同 `event_id` + 不同内容 | `409` | 冲突，已存数据不变 |

内容是否相同由 `type` 与规范化后的 `payload`（键排序后的 JSON）的 SHA-256 指纹判定，因此 `payload` 键顺序不同不算冲突。

### 分页查询 `GET /events`

```bash
curl -s 'http://localhost:8000/events?limit=2&status=pending'
# => {"items": [...], "next_cursor": "eyJyZWNlaXZlZF9hdCI6..."}
curl -s 'http://localhost:8000/events?limit=2&cursor=eyJyZWNlaXZlZF9hdCI6...'
```

- 按接收时间 `(`received_at`, `event_id`) 升序返回；
- `limit`：每页条数，默认 50，最大 200；
- `cursor`：上一页响应中的 `next_cursor`，为 `null` 表示没有更多数据；
- `status`（可选）：`pending` / `acknowledged` 过滤。

### 查询单条 `GET /events/{event_id}`

存在返回 `200`，不存在返回 `404`。

### 确认单条 `POST /events/{event_id}/ack`

```bash
curl -s -X POST http://localhost:8000/events/evt_001/ack
```

将状态置为 `acknowledged` 并记录 `acked_at`。重复确认是幂等的（返回相同结果），事件不存在返回 `404`。

## 设计要点

- **并发去重**：`event_id` 是主键，写入用单条 `INSERT OR IGNORE`（SQLite 层面原子），进程内再以锁串行化"插入/回读"临界区。并发重复请求中只有一个成功插入，其余全部读到已存的首个版本。
- **一致性**：重复请求返回的是数据库中存储的记录，因此响应体与首次响应逐字节一致。
- **持久化**：SQLite WAL 模式 + `busy_timeout`，事件与处理状态（`status` / `acked_at`）同表存储，应用启动时自动执行 `CREATE TABLE IF NOT EXISTS` 建表建索引。
- **时间游标**：`received_at` 为定长微秒级 UTC 时间戳（字典序即时间序），游标是 `(received_at, event_id)` 的不透明 base64 编码，翻页不重不漏。

## 配置

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `DATABASE_PATH` | 容器内 `/data/events.db`，本地 `./data/events.db` | SQLite 文件路径，`:memory:` 表示内存库 |

## 本地开发与测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -q          # 含并发重复/冲突的集成测试
uvicorn app.main:app --reload --port 8000
```

## 项目结构

```
app/
  main.py        # FastAPI 应用与路由（启动时建表）
  db.py          # SQLite 访问层（schema、幂等插入、游标查询、确认）
  schemas.py     # Pydantic 请求/响应模型
tests/test_api.py
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```
