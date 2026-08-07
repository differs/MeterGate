# PostgreSQL 分库配置示例（阶段 3：hash(user_id) % 4）
# 事务天然在单用户内 → 零跨库事务；对账按库并行，结果合并。

# 1. 建 4 个库（或 4 个 schema）
CREATE DATABASE metergate_shard_0;
CREATE DATABASE metergate_shard_1;
CREATE DATABASE metergate_shard_2;
CREATE DATABASE metergate_shard_3;

# 2. 每个分片相同的表结构（由网关启动时自动建表）

# 3. 路由规则（应用层）：shard = hash(user_id) % 4
#    Go: 分库适配器待实现（当前 OrderStore 单库）；
#    分库后每库独立 OrderStore 实例，按 user_id 路由。

# 4. 网关分库配置示例（未来 M6 支持时）：
#    METERGATE_PG_SHARDS=4
#    METERGATE_PG_DSN_0=postgres://...@pg-0:5432/metergate_shard_0
#    METERGATE_PG_DSN_1=postgres://...@pg-1:5432/metergate_shard_1
#    METERGATE_PG_DSN_2=postgres://...@pg-2:5432/metergate_shard_2
#    METERGATE_PG_DSN_3=postgres://...@pg-3:5432/metergate_shard_3

# 5. 对账并行：每库独立跑 reconcile --pg-dsn <shard>，结果汇总

# 6. 迁移五步（可回滚）
#    ① 双写：新分片开启写入（旧库继续）
#    ② 回填：历史订单按 shard_id 导入
#    ③ 校验：行数 + checksum 100% 一致
#    ④ 切读：1% → 10% → 50% → 100% 灰度
#    ⑤ 切写：旧库转只读，保留 90 天回滚窗口

# 7. 大客户独立分片（热点隔离）
#    shard(user_id) 覆盖常规用户；大客户单独实例 + 专属 Kafka 分区
