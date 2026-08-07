# Kafka topic sizing & provisioning (Redpanda/Kafka)
# 分区数 = 消费者成员数（网关实例数）× 2，留重平衡余量
# 分区键 = request_id（哈希）→ 同一请求始终同一分区（保序）

# --- Redpanda topic create (4 网关实例 → 12 分区) ---
rpk topic create metering.events \
  --partitions 12 \
  --replicas 3

# --- 扩容到 16 网关实例时 → 32 分区 ---
rpk topic alter metering.events --set partitions=32

# --- 检查消费者组（MEMBERS 应 = 网关实例数）---
rpk group describe metergate-settle

# expected:
#   STATE    Stable
#   MEMBERS  4
#   TOTAL-LAG 0   (after drain)
