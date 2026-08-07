# MeterGate 问题发现与解决记录

> 本文件记录项目开发与压测过程中发现的全部问题：现象、排查思路、根因、
> 解决方案、验证方式。分为三类：**资金正确性 bug**（最严重）、**性能瓶颈**、
> **测试方法问题**（非代码 bug，但同样值得记录）。
>
> 方法论核心：**计费系统不是写对的，是审出来的** —— 21 万+ 请求的真实
> 账单审计 + 高并发压测，比单元测试更能暴露资金问题。

---

## 目录

- [A. 资金正确性 bug（🔴 最严重）](#a-资金正确性-bug)
- [B. 性能瓶颈（🟡）](#b-性能瓶颈)
- [C. 正确性缺陷（非资金）（🟡）](#c-正确性缺陷非资金)
- [D. 测试方法问题（⚪ 非代码 bug）](#d-测试方法问题非代码-bug)
- [E. 方法论总结](#e-方法论总结)

---

## A. 资金正确性 bug

### A1. 结算不 clawback 超扣（流式响应用户少付钱）

**发现场景**：21 万请求压测后，余额比期望值多出 1,080,300 微元。

**现象**：固定 1,000 串行请求余额精确，500 流式请求余额精确，但**混合并发压测后余额恒有偏差**（~5 微元/请求）。

**排查思路**：
1. 先怀疑查询时机（异步结算未消化）→ 等 30s 后仍有偏差，排除
2. 固定请求精确 vs 并发不精确 → 怀疑与并发/流式相关
3. 按状态分组审计订单：发现 NO_CHARGE 订单金额为 782,725 ≠ 0（→ 引向 A2）
4. 余额偏差 782,725 与 NO_CHARGE 金额完全吻合 → 确认零完成保险问题
5. **但还有一个独立偏差**：流式请求的 clawback 缺失

**根因**：预扣按估算（prompt + max_tokens 封顶 ≈ 237 微元），流式响应实际消费可能 3,000 微元。结算脚本只释放预扣的 237，**超出的 2,763 微元从未从余额补扣**——用户少付 92%。

```lua
-- 修复前：charged > pre 时差额丢失
local refund = pre - charged
if refund < 0 then refund = 0 end   -- ← 差额被静默丢弃

-- 修复后：从余额 clawback
if refund < 0 then
  redis.call('DECRBY', KEYS[1], -refund)  -- 补扣差额
  refund = 0
end
```

**验证**：新增 `TestPreChargeClawback` 单测（pre=200, charged=5000 → 余额精确扣 5000）+ 500 流式请求端到端余额精确。

**教训**：预授权估算必须考虑"实际消费 > 估算"的路径；**资金脚本的边界情况要用专门的单测覆盖**，不能只测正常路径。

---

### A2. 零完成保险仍收 prompt 费用

**发现场景**：A1 排查过程中，按状态分组审计订单。

**现象**：failed 请求（上游 5xx）标记为 NO_CHARGE，但**订单金额仍包含 prompt token 费用**（31,309 个失败请求 = 782,725 微元）。

**根因**：`orderFromEvent` 对 failed 事件只把 completion 置 0，金额仍按 prompt 计算。OpenRouter 的 zero-completion insurance 语义是**失败请求完全免费**（prompt 也不收）。

**修复**：
```go
if ev.Status == metering.StatusFailed {
    status = StatusNoCharge
    completion = 0
    amount = 0 // 失败请求免费
}
```
（Settler 与 DetailSink 两处同修，保证 Layer A 一致）

**验证**：压测后 Layer A 金额完全一致 + 余额精确。

**教训**："不计费"的语义边界要写清楚：**是"不收 completion"还是"完全免费"**。实现时两处（订单+明细）必须同步。

---

### A3. 对账重跑重复打钱（refund 幂等执行缺失）

**发现场景**：M4 开发时写幂等测试。

**现象**：第二次运行 `RunDay(autoRefund=true)` 时，`RefundsAuto` 应为 0 但实际为 1——**用户被重复补退**。

**根因**：`InsertRefund` 幂等（重复 idempotency_key 返回已有 ID），但**执行逻辑（TopUp + MarkExecuted）没有跳过已存在的记录**——补退单没重复建，但钱重复打了。

**修复**：`InsertRefund` 返回 `(id, created, err)`，只有 `created==true` 才执行打钱。PostgreSQL 用 `xmax = 0` 技巧检测"本语句新插入"。

**验证**：`TestAutoRefundNegativeAmounts` 断言第二次运行 0 补退 + 余额不变。

**教训**：**幂等插入 ≠ 幂等执行**。所有"先插入后执行"的模式都要区分"新建"和"已存在"。

---

## B. 性能瓶颈

### B1. Settler 全局锁（所有事件争一把锁）

**发现场景**：压测 5,511 req/s 时，单请求平均 18ms（纯转发只要 3.3ms）。

**排查思路**：控制变量实验——关掉计费链路（纯转发模式）测出 30,165 req/s，证明 5.5 倍差距全在计费链路。再用微基准（10 万事件灌 Settler）测出 4,117 events/s。

**根因**：Settler 所有事件的 buffer append 用**一个全局 mutex**，100 并发下锁竞争严重。微基准进一步暴露：测试桩 `memStore` 的 O(n²) 线性扫描（修复测试桩后 Settler 实测 131 万 events/s）。

**修复**：16 分片 buffer（按 request_id 哈希路由），每分片独立 mutex + 独立 flusher goroutine。

**验证**：微基准 131 万 events/s + 全链路压测 22K req/s。

---

### B2. 每行 INSERT（500 次 fsync/批）

**发现场景**：Kafka 消费者批量提交修复后，PG 落账仍只 ~1K/s。

**排查思路**：直接测 Settler 微基准（内存 store 快）→ 怀疑 PG 侧。手动验证 multi-row SQL 正常 → 检查代码发现 `pgx.SendBatch` 发的是 **500 条独立 INSERT**（每条一次 fsync）。

**根因**：`pgx.Batch` 是 500 条独立语句的管线化，不是一条 multi-row 语句——**每条 INSERT 都要 fsync**，docker PG 上每条 2-5ms，一批 1-2.5 秒。

**修复**：真正的 multi-row INSERT（一条语句 `VALUES (...),(...)` + `ON CONFLICT DO NOTHING`），每批 ≤1000 行（12,000 参数 < PG 65,535 上限）。

**验证**：压测 PG 落账速率大幅提升，Layer A 完全一致。

---

### B3. Kafka 每条消息一次 CommitMessages（489 events/s 的元凶）

**发现场景**：Kafka 消费模式压测，消费者 lag 450 万且只以 ~934/s 消化。

**排查思路**：
1. Settler 微基准 131 万/s（排除 Settler）
2. BatchSettle 微基准 23 万/s（排除 Redis）
3. 写 Kafka 消费链路集成测试：**稳定复现 489 events/s**
4. 读 kafka-go 源码：`FetchMessage` 有预取，但**每条消息一次 `CommitMessages` 是同步 RPC（~1ms）**

**根因**：per-message commit 是 kafka-go 公认的反模式——每条消息一次 broker 往返。

**修复**：批量提交——攒 500 条或 200ms，一次 `CommitMessages(batch...)`（group 语义：最高 offset 提交即提交全部之前的）。

**验证**：集成测试 489 → **85,397 events/s（174 倍）**。

---

### B4. 计量日志在热路径（一次压测写 77MB JSON）

**发现场景**：分析单请求 18ms 构成时，发现 slog 同步写文件 + JSON marshal 占大头。

**根因**：`gateway.emit` 每次请求 `slog.Info("metering", event)` —— marshal + 文件写同步执行。审计事件已通过 Kafka/ClickHouse/PostgreSQL 持久化，日志只是兜底。

**修复**：metering 日志降为 Debug 级别（`METERGATE_LOG_LEVEL=debug` 开启），审计可靠性由事件总线承担。

**验证**：压测吞吐提升 + 完整性不变（Layer A 一致）。

---

### B5. 连接池过小（100 并发下频繁重建上游连接）

**发现场景**：压测初期分析连接行为。

**根因**：`http.Transport` 的 `MaxIdleConnsPerHost=32`，100 并发下 68 个请求需新建 TCP 连接。

**修复**：`MaxIdleConns=1024, MaxIdleConnsPerHost=512`。

---

## C. 正确性缺陷（非资金）

### C1. 健康位初始 false 导致路由选择锁死（富者愈富）

**发现场景**：M3 路由引擎压测，$1 渠道 150 次全部命中、$3 渠道 0 次（预期 9:1）。

**排查思路**：单测分布正常（9:1）→ 集成测试正常 → 真实运行异常 → 差异在运行时状态。检查发现 `HealthTracker.Healthy()` 对**无数据的渠道返回 false**——从未被选中的渠道永远不健康，永远无法被选中。

**根因**：无数据 = 不健康 的默认值导致选择锁死（rich-get-richer）。

**修复**：无数据渠道视为健康 + 构建时预热所有渠道为健康。

**验证**：200 请求实测 181:19 ≈ 9:1。

**教训**：**默认值方向决定系统是否自锁**。无数据时"健康"比"不健康"安全（宁可用坏渠道，不可饿死好渠道）。

---

### C2. 熔断器 Allow() 副作用泄漏

**发现场景**：M3 代码审查时发现 `Route()` 调用 `breaker.Allow()` 会消耗 HalfOpen 探针配额——**每次路由决策都消耗探针**，与"转发时才消耗"的语义不符。

**修复**：拆出只读的 `IsOpen()` 用于路由决策；`Allow()`（有副作用）只在真正转发时调用。

---

### C3. 异步 Settler 的 flush 时机 bug（残余 batch 永不 flush）

**发现场景**：压测结束后 Kafka lag 停在 101 不消化（消费者看起来"卡死"）。

**根因**：`KafkaConsumer.Run` 的 maxWait flush 检查**只在 `FetchMessage` 返回后执行**——topic 静默（无新消息）时 `FetchMessage` 阻塞，残余 batch 永不 flush。

**修复**：`FetchMessage` 用 `context.WithTimeout(maxWait)`——超时即排水 flush。

**验证**：压测后 lag 归零。

---

### C4. commit 先于落盘（丢账窗口）

**发现场景**：设计评审时发现 KafkaConsumer 提交 offset 时，Settler 的异步 buffer 可能还没落 PG——**崩溃即丢账**（at-least-once 变成 at-most-once）。

**修复**：`Settler.FlushSync()`（强制所有分片落账）在每次 commit 前调用——**先落盘，再提交**。

**验证**：压测后 PG=CH 完全一致 + lag=0。

---

### C5. 非流式失败路径不产生计量事件

**发现场景**：压测统计事件数 < 请求数（差 1.8 万）。

**根因**：上游 5xx 时 handler 直接 `writeError` 返回，**不 emit 事件**——失败请求对账不可见。

**修复**：失败也 emit failed 事件（审计 + NO_CHARGE 记录）。

---

### C6. 明细层金额为 0

**发现场景**：ClickHouse 明细 20 条但金额全 0。

**根因**：`metering.Event` 没有金额字段，`DetailSink` 未定价。

**修复**：DetailSink 复用与 Settler 相同的定价路径（保证 Layer A 一致）。

---

## D. 测试方法问题（非代码 bug）

> 这些不是代码缺陷，但**每次排查都消耗了大量时间**——记录它们，避免重蹈覆辙。

### D1. Kafka group offset 残留（topic 重建后消费者停滞）

**现象**：重建 topic 后消费者"永不消费"（PG 零增长），反复误判为代码 bug。

**根因**：consumer group 保留了旧 topic 的 offset（数百万），新 topic 的 end offset 远小于 committed offset——消费者认为"无新消息"。

**解决**：删除 group（`rpk group delete`，需等 session 过期）或换 group id。生产上 topic 不会随便重建，但**运维手册要注明**。

### D2. 查询时机过早（异步结算未消化）

**现象**：压测后立即查 PG，余额/订单数对不上——实际是异步 Settler 还在消化。

**解决**：验证前等 30-60s 或确认 lag=0。

### D3. 跨轮次数据污染

**现象**：多轮压测共享 PG/Redis/Kafka，历史数据混入导致"Layer A 不一致"误判。

**解决**：每轮开始前完整清理（PG DELETE + CH DROP TABLE + Redis FLUSHDB + Kafka topic 重建 + group 重置）。

### D4. 日志文件累积（grep 统计失真）

**现象**：metering 事件日志跨轮次累积，用 grep 计数得到错误的事件数。

**解决**：每轮启动前 `rm` 日志文件；或改用 PG/CH 计数。

### D5. pkill 自匹配（bash 命令被杀）

**现象**：`pkill -f one-api-server` 会匹配**包含该字符串的 bash 命令行自身**，把 shell 杀掉，后续命令不执行，表现为"服务没起来"。

**解决**：pkill 用精确模式（`pkill -x`）或先取 PID 再 kill；启动与清理分开命令。

### D6. Docker 环境细节

- ClickHouse 官方镜像 default 用户**仅允许回环 IP**（docker 端口映射访问被拒，伪装成密码错误）→ 挂载 users.d 配置放行
- ClickHouse 配置 XML **不支持 `#` 注释**（解析报错 line 1 column 1）→ 用 XML 注释
- Redpanda 默认 512M 内存会 OOM 被杀 → 至少 768M-1G
- kafka-go v0.4 的异步错误通过 `Completion` 回调（不是 `Errors()`）

---

## E. 方法论总结

### 发现问题的三板斧

```
① 控制变量实验：纯转发 vs 全链路 → 定位差距归属
② 微基准隔离：Settler/BatchSettle/Kafka 分别测 → 定位组件
③ 数据审计：按状态分组统计订单/明细/余额 → 定位资金问题
```

### 资金系统的测试层次

| 层次 | 覆盖 | 本次发现 |
|------|------|----------|
| 单元测试 | 脚本边界（clawback、幂等） | A1, A3 |
| 端到端小量 | 固定请求精确性 | 验证基线 |
| **高并发压测 + 账单审计** | 并发×流式×失败混合 | A1, A2, B1-B4, C1, C5 |
| 静默期/故障注入 | 边界状态 | C3（topic 静默）, C4（崩溃窗口） |

### 十条工程经验

1. **钱用整数算**——float 是金融原罪
2. **幂等插入 ≠ 幂等执行**——`created` 标志必须显式
3. **预授权必须处理超扣路径**——估算永远可能不够
4. **语义边界写清楚**——"不计费"是免费还是部分免费
5. **默认值方向决定自锁**——无数据时选安全侧
6. **commit 必须晚于落盘**——at-least-once 是承诺不是口号
7. **批量提交是 Kafka 消费者的第一性能原则**——per-message commit 是反模式
8. **multi-row INSERT 才是批量**——pgx.Batch 是管线化不是合并
9. **审计日志不占热路径**——事件总线负责持久化，日志只做兜底
10. **区分系统 bug 与测试污染**——清理方法要标准化（D1-D4）

---

---

## F. 准确性路线（2026-08 追加：比性能更致命的五个缺口）

> 性能战场收尾后（22K req/s 且每核效率恒定），准确性成为主战场。
> 这五个缺口单测/压测都测不出来，只在"真实上游 + 价格变更 + 重平衡 +
> 用户申诉"的组合场景下暴露。

| # | 缺口 | 风险 | 修复 | 验证 |
|---|------|------|------|------|
| F1 | **价格快照缺失**：结算用当前价格表，价格变更会重定价在途请求 | 对账系统性偏差 | `PricingSnapshot` 随事件冻结请求开始价格；原子 PriceTable | 单测：$10→$8 变更后按 $10 结算 |
| F2 | **真实上游 usage 语义**：reasoning/cached tokens、中断无 usage 被 mock 掩盖 | 供应商账单对不上 | `internal/upstreamsim` 模拟 5 种上游形态 | 套件测试全过 |
| F3 | **供应商对账未实现**：毛利不可信 | 平台亏钱不知道 | `ProviderReconciler`（万分之五容差，账单源可插拔） | 容差内/20% 差异/缺账单三场景 |
| F4 | **明细追溯缺失**：用户申诉不可查 | 信任危机 | `GET /admin/billing/request?request_id=` | CH 明细查询 |
| F5 | **重平衡结算一致性未验证**：at-least-once 语义下重复投递 | 重复扣费 | 故障注入测试（5 次重复投递 + 双 Settler 重平衡） | 恰好 1 单、零重复扣费 |

**方法论更新**：mock 掩盖真实性 → **上游行为模拟器**（upstreamsim）是准确性验证的基础设施；
资金系统的第四个测试层次（故障注入/重平衡）补上了。

*维护：MeterGate 项目组 | 持续更新中*
