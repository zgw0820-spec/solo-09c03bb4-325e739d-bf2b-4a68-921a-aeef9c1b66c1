# 固定内网区域主备 DNS（FastAPI + SQLite + dnslib）

为固定内网区域 `internal.local`（可用 `DNS_ZONE` 修改）提供主备两台权威 DNS，
并在同一区域内支持**受管 A 记录**：一个服务名绑定两个 IPv4 地址和一个 TCP
探测端口，由每台 DNS 各自探测、自动在主地址与备地址之间切换。

- **主机 (primary)**：REST API 一次提交、最多 **8 条 A 记录**（普通 A 与受管
  A 合计）的**完整区域**；整份区域校验并落库成功后才把 serial 加 1。发布不
  依赖备用机，也不等待任何探测。
- **备用机 (backup)**：从主机 HTTP 拉取**整份版本**（手动 / 后台自动 / 启动
  即拉），独立 SQLite 数据库。拉取失败或缓慢时继续用旧版应答；恢复后追到新版。
- 两机都通过 **UDP（默认 5053）** 回答区域内 A 查询。每个回答只取自**同一份
  不可变版本快照**，探测状态也按该版本的 serial 隔离，绝不混版。

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 受管 A 记录：服务名 + 两个 IPv4 + 同一 TCP 端口 | `app/models.py` `RecordInput`：`name/primary_ip/backup_ip/port`；同一记录不得同时给普通 `ip` |
| 与普通 A 合计最多 8 条，普通 A 提交仍可用 | `ZonePayload` 对混合列表统一计数与查重；只含 `name/ip` 的旧请求体继续工作 |
| 两台 DNS 分别探测 | `app/failover.py`：每进程一个后台探测线程 + 有界线程池，主/备地址各一路探测 |
| 主连续两次失败且备可达才切备 | `_decide_locked()`：`primary.fail_streak >= 2 and backup.latest_ok is True` |
| 主连续两次成功才切回 | `backup` 状态下 `primary.ok_streak >= 2` |
| 两址均不可达 / 无有效探测结果 → SERVFAIL | `decide()` 返回空地址，DNS 层置 `RCODE.SERVFAIL`（不是 NXDOMAIN） |
| 探测超时不阻塞 UDP / 发布 / 同步 | 探测在独立守护线程、connect 超时 `PROBE_TIMEOUT` 秒、锁内不做网络 IO；发布/同步只更新内存版本指针后唤醒探测线程 |
| 换版后旧探测结果不影响新版 | 探测状态按 serial 分桶；看到新 serial 立即新建空状态，旧桶仅保留一份供旧快照回答，绝不被新 serial 读取 |
| 每次回答的地址与探测状态同版本 | 解析器先读快照 `(serial, records)`，再用**同一 serial** 查 `registry.decide(serial, name)` |
| 备用断流仍按旧版探测，恢复后追随新版 | 同步失败不触碰 active 版本，探测线程持续读 active 版本；`commit_version` 成功后立即切到新 serial 的探测状态 |
| 可启动的双服务 Compose + Swagger | `docker-compose.yml`（primary/backup）；`/docs`、`/openapi.json` |

### 探测与切换状态机（每台 DNS、每条受管记录独立）

```
                 primary 连续失败 >= 2
                且 backup 最近一次探测成功
   [primary] ------------------------------> [backup]
       ^                                        |
       |         primary 连续成功 >= 2          |
       +----------------------------------------+

   初始：primary（尚无探测结果时 SERVFAIL）
   两址最近结果均为失败          -> SERVFAIL
   当前选中地址的最近结果为失败，
   但尚未满足切换条件            -> 继续回答当前选中地址
```

- “连续”指相邻两次探测结果，任一次相反结果即清零。
- 单次失败不切换、也不 SERVFAIL，避免抖动。若主址只失败一次且备址不可达，
  仍回答主址（选中状态未变）；两址都已知失败才 SERVFAIL。
- 探测结果只写入与该探测任务相同的 serial；执行中的探测在换版后返回也会被丢弃
  （serial 不在注册表中）。
- 备用断流期间，备用机继续按它当前 active 的旧版探测和应答；恢复同步到新版后，
  探测目标随新版变化。

### 数据模型（SQLite，WAL 模式）

```
zone_versions(serial INTEGER PRIMARY KEY, records_json TEXT,
              record_count INTEGER, published_at TEXT)   -- 不可变整份版本
state(key TEXT PRIMARY KEY, value TEXT)
        active_serial      当前生效版本指针
        last_sync_*        备用机最近一次同步状态（at/status/error）
        upstream_serial    最近看到的上游序号
```

`records_json` 中每项要么是 `{"name","ip"}`（普通 A），要么是
`{"name","primary_ip","backup_ip","port"}`（受管 A）。版本切换 = 插入新版本行
+ 更新 `active_serial`，同一事务提交。探测状态只存在内存中，按 serial 分桶，
不落库。

## 端口与 API

| 角色 | API/Swagger | UDP DNS |
| --- | --- | --- |
| primary | 主机 8080 → 容器 8000 | 主机 5053 → 容器 5053/udp |
| backup  | 主机 8081 → 容器 8000 | 主机 5054 → 容器 5053/udp |

- `GET  /health` 健康检查
- `GET  /api/version` 角色、serial、记录数、同步状态、受管记录的探测状态与当前答案
- `GET  /api/zone` 当前完整区域
- `POST /api/zone` **仅主机**，整份替换区域，成功 201
- `POST /api/sync` **仅备用机**，立即拉取一次，返回 `synced` / `ignored_stale` / `failed`
- `POST /api/failover/switch` 运维强制切换：`{"name": "...", "primary_ip": "..."}`；
  仅作用于当前 active 版本的受管记录，`primary_ip` 必须与当前版本一致（防止按旧
  版本强制），主址恢复连续两次成功后自动切回
- `GET  /api/probe` 当前版本的每条受管记录、两址探测结果/连续计数与答案

发布请求体示例（普通 A 与受管 A 混用）：

```json
{"records": [
  {"name": "web.internal.local", "ip": "10.0.0.21"},
  {"name": "svc.internal.local",
   "primary_ip": "10.0.0.11", "backup_ip": "10.0.0.12", "port": 5432}
]}
```

`primary_ip`/`backup_ip`/`port` 也接受别名 `primary`/`backup`/`secondary_ip`/
`probe_port` 等（见 Swagger schema）。同一记录只能二选一：普通 A 或受管 A。

DNS 行为：区域内普通 A 名返回 NOERROR+A；受管记录按上述状态机返回主/备地址，
不可用时 SERVFAIL；区域内不存在的名称 NXDOMAIN；区域内非 A 查询 NOERROR NODATA；
区域外 REFUSED。

## 运行方式

### Docker Compose（推荐）

```bash
docker compose up --build
# 主机 Swagger:  http://localhost:8080/docs
# 备用 Swagger:  http://localhost:8081/docs

curl -sX POST http://localhost:8080/api/zone -H 'content-type: application/json' \
  -d '{"records":[{"name":"web.internal.local","ip":"10.0.0.21"},
                  {"name":"svc.internal.local","primary_ip":"10.0.0.11",
                   "backup_ip":"10.0.0.12","port":5432}]}'

curl -sX POST http://localhost:8081/api/sync          # 后台也会自动同步
curl -s http://localhost:8080/api/probe               # 查看探测状态

dig @127.0.0.1 -p 5053 svc.internal.local A
dig @127.0.0.1 -p 5054 svc.internal.local A           # 备用机独立探测
```

注意：Compose 中的 `10.0.0.11/12` 只是示例地址，需替换为从容器可达的真实内网
地址与端口；受管记录的探测从容器网络发起。

### 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
DNS_ROLE=primary DB_PATH=./data/primary.db DNS_PORT=15053 API_PORT=18000 \
  .venv/bin/python -m uvicorn app.main:app --port 18000
DNS_ROLE=backup  DB_PATH=./data/backup.db  DNS_PORT=15054 API_PORT=18001 \
  PRIMARY_API_URL=http://127.0.0.1:18000 \
  .venv/bin/python -m uvicorn app.main:app --port 18001
```

## 测试

```bash
.venv/bin/python -m pytest tests/test_unit.py -q   # 校验/事务/防回退/状态机单元测试
.venv/bin/python -I tests/e2e_test.py              # 原双进程端到端 30 项（回归）
.venv/bin/python -I tests/auto_sync_test.py        # 自动同步/幂等/Swagger
.venv/bin/python -I tests/slow_transfer_test.py    # 慢传输期间旧版持续应答
.venv/bin/python -I tests/failover_e2e.py          # 主备切换/换版隔离/非阻塞（39 项）
```

`failover_e2e.py` 在 `127.0.0.x` 上起真实 TCP 服务并启停它们，覆盖：受管记录
初答主址、主址连续失败切备、双址全挂 SERVFAIL、主址恢复两次成功切回、新版本
尚无探测结果时 SERVFAIL、换版后不泄露旧版地址、强制切换及地址校验、探测超时
期间发布与 API 延迟、普通 A 与受管混用、备用断流继续旧版以及恢复后追随新版。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DNS_ROLE` | `primary` | `primary` 或 `backup` |
| `DNS_ZONE` | `internal.local` | 固定内网区域名 |
| `DB_PATH` | `/data/dns.db` | 本机独立 SQLite 文件 |
| `DNS_PORT` / `API_PORT` | `5053` / `8000` | UDP DNS / HTTP 端口 |
| `PRIMARY_API_URL` | `http://primary:8000` | 备用机指向主机 API |
| `SYNC_INTERVAL` | `5` | 备用机后台同步间隔秒 |
| `HTTP_TIMEOUT` | `10` | 拉取区域 HTTP 超时秒 |
| `MAX_RECORDS` | `8` | 单次区域最大 A 记录数（普通+受管合计） |
| `PROBE_INTERVAL` | `1.0` | 每地址 TCP 探测间隔秒 |
| `PROBE_TIMEOUT` | `0.5` | 单次 TCP connect 超时秒 |
| `PROBE_WORKERS` | `32` | 探测线程池大小（16 条地址并发足够） |
| `DNS_TTL` | `30` | A 记录 TTL |

## 已知边界

- 仅支持 A 记录与 IPv4；区域固定为一个（`DNS_ZONE`）。
- 探测只做 TCP connect 可达性，不做协议级健康检查；端口必须两址一致。
- 探测结果在内存中，进程重启后从零开始：重启后先 SERVFAIL，直到两次探测完成
  （通常 1–2 个 `PROBE_INTERVAL`）。
- 切换判定按“每台 DNS 自己的探测”，因此两机可能短暂处于不同状态；这是题目
  要求的“分别探测”。
- 传输为整份区域拉取（无增量/AXFR），区域上限 8 条，报文很小。
- 单机内嵌 UDP 线程 + 探测线程 + 一个 uvicorn 进程；未做 TSIG 鉴权，内网部署时
  应限制 `/api/zone` 与 `/api/failover/switch` 的调用来源。
- 强制切换接口是运维逃生口，不是自动故障转移的一部分。
