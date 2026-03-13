# Kafka 使用与配置技术文档

本文档详细记录了 Kafka 的环境配置、使用指南、遇到的问题及解决方案，以及相关的最佳实践建议。

---

## 目录

1. [Kafka 核心概念](#1-kafka-核心概念)
2. [Kafka 环境配置](#2-kafka-环境配置)
3. [Kafka 使用指南](#3-kafka-使用指南)
4. [遇到的问题及解决方案](#4-遇到的问题及解决方案)
5. [Kafka 消费者暂停/恢复机制原理](#5-kafka-消费者暂停恢复机制原理)
6. [注意事项与建议](#6-注意事项与建议)

---

## 1. Kafka 核心概念

### 1.1 什么是 Kafka？

Kafka 是一个**分布式流处理平台**，由 LinkedIn 开发，现为 Apache 开源项目。它的核心特性：

| 特性 | 说明 |
|------|------|
| **高吞吐** | 支持每秒百万级消息传输 |
| **持久化** | 消息持久化到磁盘，可配置保留时间 |
| **分布式** | 支持集群部署，自动副本同步 |
| **可扩展** | 支持动态扩容 |
| **容错** | 副本机制保证数据不丢失 |

### 1.2 Kafka 核心架构

```
                         ┌─────────────────────────────────────────┐
                         │           Kafka Cluster                │
┌────────────┐          │  ┌─────────┐  ┌─────────┐  ┌─────────┐│
│  Producer  │──────────│──│Broker 1 │──│Broker 2 │──│Broker 3 ││
└────────────┘          │  │(Leader) │  │(Follower)│  │(Follower)││
                       │  └────┬────┘  └────┬────┘  └────┬────┘│
┌────────────┐          │       │            │            │       │
│  Consumer │──────────│───────┴────────────┴────────────┘       │
└────────────┘          │              │                         │
                       │  ┌────────────▼────────────┐            │
                       │  │      Zookeeper         │            │
                       │  │   (集群元数据管理)      │            │
                       │  └────────────────────────┘            │
                       └─────────────────────────────────────────┘
```

### 1.3 核心术语

| 术语 | 说明 | 比喻 |
|------|------|------|
| **Broker** | Kafka 服务节点 | 邮局 |
| **Topic** | 消息主题/分类 | 邮箱 |
| **Partition** | 分区，Topic 的物理分片 | 邮箱格子 |
| **Replica** | 副本，数据备份 | 复印件 |
| **Offset** | 消息偏移量 | 信件编号 |
| **Producer** | 消息生产者 | 发信人 |
| **Consumer** | 消息消费者 | 收信人 |
| **Consumer Group** | 消费者组 | 收信组 |
| **Leader/Follower** | 主副本/从副本 | 负责人/备份 |

### 1.4 消息处理流程

```
1. Producer 发送消息
       ↓
2. Broker 接收消息，写入 Partition
       ↓
3. Leader Partition 同步到 Follower Partition
       ↓
4. Consumer 从 Partition 拉取消息
       ↓
5. Consumer 提交 Offset 表示已处理
```

---

## 2. Kafka 环境配置

### 2.1 Broker 配置详解

#### 基本配置原理

Broker 是 Kafka 的核心服务进程，负责接收消息、存储消息、提供消息。

```properties
# ========== 基本配置 ==========

# Kafka 集群地址列表，客户端通过此地址连接
# 建议至少配置 2 个，以防单点故障
bootstrap.servers=192.168.116.5:9092,192.168.116.6:9092

# 监听地址
# PLAINTEXT: 明文通信
# SSL: 加密通信
listeners=PLAINTEXT://0.0.0.0:9092

# 对外公布的地址，客户端实际连接的地址
# 生产环境需配置外网可访问的地址
advertised.listeners=PLAINTEXT://192.168.116.5:9092
```

**原理说明**：
- `listeners`：Broker 绑定的 IP 和端口
- `advertised.listeners`：告诉客户端连接哪个地址
- 内网环境可相同，外网环境需要不同

#### 日志存储配置

```properties
# 日志存储目录，可以配置多个目录（用逗号分隔）
# 生产环境建议使用 SSD 或高速磁盘
log.dirs=/tmp/kafka-logs

# 消息保留时间
# 默认 7 天（168 小时）
# 根据业务需求调整：
# - 业务数据：7-30 天
# - 日志数据：1-7 天
# - 临时数据：几小时
log.retention.hours=168

# 保留检查间隔，定期检查过期日志并删除
log.retention.check.interval.ms=300000

# 单个日志文件大小，达到此大小会创建新文件
# 默认 1GB，有助于日志轮转
log.segment.bytes=1073741824
```

**原理说明**：
- Kafka 使用**日志分段**存储，每个分段文件大小有限
- 超过保留时间或大小的日志会被删除
- 分段有助于快速查找和删除历史数据

#### 线程与网络配置

```properties
# 网络处理线程数，负责接收请求和返回响应
# 建议值：CPU 核心数 + 1
# 3 核 CPU → 4 个线程
num.network.threads=3

# IO 处理线程数，负责实际的消息读写
# 建议值：CPU 核心数 × 2
# 3 核 CPU → 6 个线程
num.io.threads=8

# Socket 发送缓冲区大小
socket.send.buffer.bytes=102400

# Socket 接收缓冲区大小
socket.receive.buffer.bytes=102400

# 单次请求最大大小，防止大请求占用内存
socket.request.max.bytes=104857600
```

**原理说明**：
- Kafka 采用**多线程架构**
- 网络线程：处理网络请求（接收/发送）
- IO 线程：执行实际的磁盘读写
- 缓冲区：减少网络/磁盘 IO 次数

#### 副本与一致性配置

```properties
# 默认副本数
# 生产环境建议 3
default.replication.factor=3

# 最少同步副本数
# 写入数据时，必须有至少 2 个副本同步成功
min.insync.replicas=2
```

**原理说明**：
```
场景：replication.factor=3, min.insync.replicas=2

消息写入流程：
1. Producer 发送给 Leader
2. Leader 同步到 Follower 1 ✓
3. Leader 同步到 Follower 2 ✓
4. 确认写入成功

如果只有 1 个 Follower 同步成功：
- 会返回错误给 Producer
- 消息写入失败
```

| 配置值 | 可靠性 | 可用性 | 说明 |
|--------|--------|--------|------|
| acks=0 | 低 | 高 | 发完就忘，不等待确认 |
| acks=1 | 中 | 高 | 等待 Leader 确认 |
| acks=all | 高 | 低 | 等待全部副本确认 |

### 1.2 Producer 配置

#### Producer 工作原理

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Producer   │────▶│   Broker    │────▶│   Partition │
│ (发件人)     │     │   (邮局)     │     │   (邮箱)     │
└─────────────┘     └─────────────┘     └─────────────┘
      │                   │                   │
      │ 1. 收集消息        │ 2. 批量发送        │ 3. 追加到日志
      │                   │                   │
      ▼                   ▼                   ▼
  内存缓冲区          请求队列            物理文件
 (batch.size)        (request.timeout)
```

#### Producer 配置参数

```properties
# ========== 核心配置 ==========

# Kafka 集群地址（必填）
bootstrap.servers=192.168.116.5:9092

# 键序列化器，将 Key 转为字节数组
key.serializer=org.apache.kafka.common.serialization.StringSerializer

# 值序列化器，将 Value 转为字节数组
value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

**原理说明**：
- Kafka 只传输字节数组
- 需要将 Java 对象**序列化**为字节数组
- 常见序列化方式：String、JSON、Avro、Protobuf

```properties
# ========== 可靠性配置 ==========

# 确认机制，控制写入成功标准
# 推荐值：all（高可靠）
acks=all

# 发送失败重试次数
# 推荐值：3
retries=3
```

**原理说明**：
```
acks=all 工作流程：

1. Producer 发送消息到 Broker
           ↓
2. Leader 收到消息，写入本地日志
           ↓
3. Leader 复制到 Follower
           ↓
4. 所有 Follower 确认后
           ↓
5. Leader 返回确认给 Producer
           ↓
6. 只有此时才认为写入成功
```

```properties
# ========== 性能配置 ==========

# 批量大小，当达到此大小时发送批次
# 默认 16KB
# 消息较大时增加此值
batch.size=16384

# 批次等待时间，即使未达到 batch.size 也发送
# 默认 0ms（立即发送）
# 增加此值可提高吞吐量，但会增加延迟
linger.ms=10

# 发送缓冲区大小
# 默认 32MB
buffer.memory=33554432

# 压缩类型，可选：none, gzip, snappy, lz4, zstd
# 推荐值：snappy（平衡压缩率和速度）
compression.type=snappy
```

**原理说明**：
```
批量发送原理：

单个发送（效率低）：
[msg1] → [Broker]
[msg2] → [Broker]
[msg3] → [Broker]
发送 3 次，3 次网络往返

批量发送（效率高）：
[msg1, msg2, msg3] → [Broker]
发送 1 次，1 次网络往返
```

### 1.3 Consumer 配置

#### Consumer 工作原理

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Broker    │────▶│  Consumer   │────▶│  业务处理    │
│   (邮局)     │     │   (收件人)   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
      │                   │                   │
      │ 1. Pull 拉取消息    │ 2. 处理消息        │
      │                   │                   │
      ▼                   ▼                   ▼
   Partition         内存缓冲区           业务逻辑
 (offset 管理)       (poll 批量)
```

#### Consumer 配置参数

```properties
# ========== 核心配置 ==========

# Kafka 集群地址
bootstrap.servers=192.168.116.5:9092

# 消费者组 ID，同一组内消息只会消费一次
group.id=vector-sync-group

# 键反序列化器
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer

# 值反序列化器
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

**原理说明**：
```
Consumer Group 消费模型：

Topic: [P0, P1, P2]
          │     │     │
          ▼     ▼     ▼
Group A: [C0]  [C1]  [C2]    ← 每个分区一个消费者
          (3 个消费者)

消息流向：
P0 → C0
P1 → C1
P2 → C2

特点：
- 消息只会被消费一次
- 消费者数 ≤ 分区数
- 消费者数 = 分区数 时效率最高
```

```properties
# ========== 偏移量管理 ==========

# 当没有初始偏移量时，从哪里开始消费
# earliest: 从最早的消息开始
# latest: 从最新的消息开始
auto.offset.reset=earliest

# 是否自动提交偏移量
# 推荐值：false（手动控制）
enable.auto.commit=false
```

**原理说明**：
```
偏移量（Offset）概念：

Partition 日志：
[0]msg1 → [1]msg2 → [2]msg3 → [3]msg4 → [4]msg5
            ↑                        ↑
         offset=1                 offset=4

消费进度：
- 已消费：offset 0, 1, 2, 3
- 下次消费：offset 4
- 提交 offset=4 后，即使重启也会从 4 开始

自动提交 vs 手动提交：
- 自动提交：后台定时提交，可能丢消息
- 手动提交：业务处理后提交，确保不丢
```

```properties
# ========== 消费控制 ==========

# 每次 poll 的最大记录数
# 推荐值：500
max.poll.records=500

# 两次 poll 之间的最大间隔
# 超过此时间未 poll，视为消费者离线
# 推荐值：5 分钟（300000ms）
max.poll.interval.ms=300000

# 会话超时时间
# 超过此时间未收到心跳，踢出消费者组
session.timeout.ms=100000

# 心跳间隔
# 建议值：session.timeout.ms / 3
heartbeat.interval.ms=30000
```

**原理说明**：
```
poll 机制原理：

while (running) {
    // 1. 从 Kafka 拉取消息
    records = consumer.poll(Duration.ofMillis(1000));
    
    // 2. 处理消息
    for (record : records) {
        process(record);
    }
    
    // 3. 手动提交偏移量
    consumer.commitSync();
}
```

```properties
# ========== 并发消费 ==========

# 并发消费者数量
# 建议值：分区数 / 消费者实例数
# 例如：10 个分区，2 个实例 → concurrency = 5
concurrency=10
```

**原理说明**：
```
Spring Kafka 并发消费原理：

Spring 创建多个 ListenerContainer：
                   
Topic [P0, P1, P2, P3]
        │   │   │   │
        ▼   ▼   ▼   ▼
    ┌─────────────────┐
    │ ConsumerThread1 │ → 处理 P0, P1
    ├─────────────────┤
    │ ConsumerThread2 │ → 处理 P2, P3
    └─────────────────┘
```

---

## 3. Kafka 使用指南

#### Topic 核心概念

```
Topic: 数据的分类/主题
├── Partition 1 (分区 1)
│   ├── Partition 2 (分区 2)
│   └── Partition N (分区 N)
│
每个 Partition:
- 有序（消息按顺序存储）
- 持久化（磁盘存储）
- 可副本（高可用）
```

#### 创建 Topic

```bash
# 基本创建命令
kafka-topics.sh \
  --create \
  --topic data-sync-topic \      # Topic 名称
  --partitions 10 \              # 分区数
  --replication-factor 1 \       # 副本数
  --bootstrap-server 192.168.116.5:9092
```

**参数选择建议**：

| 参数 | 开发环境 | 生产环境 |
|------|----------|----------|
| partitions | 6-10 | 20-50+ |
| replication-factor | 1 | 3 |

# 查看 Topic
``` bash
kafka-topics.sh --list --bootstrap-server 192.168.116.5:9092
```

# 查看消费组状态
```bash
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.116.5:9092 \
  --group vector-sync-group \
  --describe
```

### 3.2 生产者实现

```java
@Service
public class DataProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // 发送消息
    public void send(String topic, String key, String value) {
        kafkaTemplate.send(topic, key, value)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    // 发送失败
                    log.error("发送失败: {}", ex.getMessage());
                } else {
                    // 发送成功
                    log.debug("发送成功, partition: {}, offset: {}", 
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });
    }

    // 同步发送
    public void sendSync(String topic, String key, String value) {
        try {
            kafkaTemplate.send(topic, key, value).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("发送失败", e);
        }
    }
}
```

### 3.3 消费者实现

#### 批量消费模式

```java
@Service
public class MessageConsumerService {

    @KafkaListener(
        topics = "${spring.kafka.topic}",
        groupId = "${spring.kafka.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
        List<ConsumerRecord<String, String>> records,
        Acknowledgment ack
    ) {
        for (ConsumerRecord<String, String> record : records) {
            try {
                // 业务处理
                process(record.value());
                // 手动提交
                ack.acknowledge();
            } catch (Exception e) {
                log.error("处理失败: {}", e.getMessage());
            }
        }
    }
}
```

### 3.4 序列化方案

#### JSON 序列化

```java
@Configuration
public class KafkaConfig {

    // Producer 使用 JSON
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    // Consumer 使用 JSON
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.model");
        return new DefaultKafkaConsumerFactory<>(props);
    }
}
```

---

## 4. 遇到的问题及解决方案

### 问题 1：消费失败时偏移量仍被提交

**现象**：消息处理失败，但 offset 仍然前进

**环境信息**：
- Kafka 版本：2.8+
- Spring Kafka 版本：3.0+
- 消费模式：批量消费

**排查过程**：
1. 检查消费者日志，发现消息处理报错但 offset 仍前进
2. 检查配置，发现使用了 `AckMode.MANUAL_IMMEDIATE`
3. 检查 `bulkIndex` 方法，发现即使 ES 返回错误也不抛异常

**根本原因**：
1. 使用了 `AckMode.MANUAL_IMMEDIATE`，每次 poll 后自动提交
2. `bulkIndex` 方法即使返回错误也不会抛出异常

**解决方案**：
```java
// 1. 改为手动提交
factory.setAckMode(ContainerProperties.AckMode.MANUAL);

// 2. 只有成功才提交
if (success) {
    ack.acknowledge();
}
```

**验证结果**：消息处理失败后，offset 不再自动前进。

---

### 问题 2：循环消费导致资源浪费

**现象**：下游服务持续报错，消费者反复消费相同消息

**环境信息**：同问题1

**排查过程**：
1. 检查日志，发现同一批消息反复消费
2. 检查下游服务（ES），发现连接超时
3. 分析代码，发现失败消息不 ack，会被重新消费

**根本原因**：
1. 失败后不 ack，消息保留在队列
2. 没有失败阈值检测机制
3. 没有暂停/恢复机制

**解决方案**：
```java
// 添加连续失败计数
private AtomicInteger consecutiveFailures = new AtomicInteger(0);

// 超过阈值自动暂停
if (consecutiveFailures.get() >= maxConsecutiveFailures) {
    container.pause();
}
```

**验证结果**：连续失败达到阈值后，消费者自动暂停。

---

### 问题 3：Kafka 不支持部分 offset 提交

**现象**：批量消费时，部分成功部分失败，不知道如何处理

**根本原因**：
Kafka 的 offset 提交是"截至式"的 - 提交 offset=3 意味着已处理 0,1,2,3，不支持跳跃式提交。

**解决方案**：
```java
// 方案1：整批处理
if (failCount == 0) {
    ack.acknowledge(); // 全部成功才提交
} else {
    // 全部失败或部分失败，整批重试
}

// 方案2：按成功率判断
double successRate = (double) successCount / totalCount;
if (successRate >= threshold) {
    ack.acknowledge(); // 成功率足够，提交
    sendFailedToRetry(failedRecords); // 失败的发到重试队列
}
```

**验证结果**：根据成功率决定是否提交，避免重复消费。

---

### 问题 4：部分成功时的重试机制优化

**现象**：
- 全部失败 → 下游故障，应该暂停
- 部分失败 → 临时故障，重试才有意义

**解决方案**：
```java
boolean allFailed = failCount == totalCount;
boolean hasSuccess = successCount > 0;
double successRate = (double) successCount / totalCount;

if (allFailed) {
    // 全部失败：下游故障，暂停消费者
    consecutiveBatchFailures.incrementAndGet();
    if (consecutiveBatchFailures >= maxConsecutive) {
        pause();
    }
} else if (hasSuccess && successRate >= threshold) {
    // 部分成功：临时故障，发送到重试队列
    sendFailedToRetry(failedRecords);
}
```

**验证结果**：下游故障时及时暂停，临时故障时允许重试。

---

### 问题 5：重试队列的简化

**现象**：单独的重试队列增加系统复杂度

**分析**：
- 重试队列需要单独的消费者
- 增加了维护成本

**解决方案**：
```java
// 失败消息发回原队列末尾
ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
    record.topic(),
    record.partition(),
    record.timestamp(),
    record.key(),
    record.value()
);
kafkaTemplate.send(producerRecord);
```

**验证结果**：简化了架构，失败消息在原队列重试。

---

### 问题 6：死信消息处理

**现象**：超过重试次数的消息需要保存供后续排查

**解决方案**：
```java
// 发送到死信队列
@Value("${sync.dlq.topic:data-sync-dlq}")
private String dlqTopic;

private void sendToDlq(SyncMessage message, String originalValue, String reason) {
    Map<String, Object> dlqData = new HashMap<>();
    dlqData.put("original_message", message);
    dlqData.put("original_value", originalValue);
    dlqData.put("dlq_reason", reason);
    dlqData.put("dlq_timestamp", System.currentTimeMillis());
    
    ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
        dlqTopic, message.getId(), new ObjectMapper().writeValueAsString(dlqData));
    kafkaTemplate.send(dlqRecord);
}
```

**验证结果**：失败消息保存到 DLQ，可供后续人工排查。

---

## 5. Kafka 消费者暂停/恢复机制原理

### 5.1 概述

Kafka 消费者的 `pause()` 和 `resume()` 是 Spring Kafka 提供的功能，用于临时停止/恢复消费者从 Broker 拉取消息。本节深入分析其内部实现原理。

### 5.2 暂停状态的实现方式

#### 5.2.1 忙等（Busy Waiting）机制

```
┌─────────────────────────────────────────────────────────┐
│              消费者线程执行流程                          │
├─────────────────────────────────────────────────────────┤
│  1. consumer.poll()                                     │
│       ↓                                                 │
│  2. 检查 paused 标志                                     │
│       ↓                                                 │
│  3. 如果 paused = true:                                 │
│       ↓                                                 │
│       ┌─────────────────────────────────────────────┐  │
│       │  while (paused) {                           │  │
│       │      Thread.sleep(100);  // 忙等             │  │
│       │      // 继续检查 paused 标志                │  │
│       │      // 不调用 poll，返回空集合              │  │
│       │  }                                           │  │
│       └─────────────────────────────────────────────┘  │
│       ↓                                                 │
│  4. 如果 paused = false:                                 │
│       ↓                                                 │
│       正常调用 consumer.poll() 获取消息                   │
└─────────────────────────────────────────────────────────┘
```

**关键点**：
- `pause()` 不会真正停止消费者线程
- 只是设置一个标志位
- 消费者线程在 `poll()` 时检查该标志
- 如果暂停，返回空集合而不是真正拉取消息

#### 5.2.2 Spring Kafka 源码分析

```java
// Spring Kafka 中的实现
public class ConcurrentKafkaListenerContainerFactory {
    
    public void pause() {
        for (MessageListenerContainer container : this containers) {
            container.pause();
        }
    }
    
    public void resume() {
        for (MessageListenerContainer container : this.containers) {
            container.resume();
        }
    }
}

// MessageListenerContainer 中的 paused 标志
public abstract class AbstractMessageListenerContainer {
    
    private final AtomicBoolean paused = new AtomicBoolean(false);
    
    public void pause() {
        this.paused.set(true);
    }
    
    public void resume() {
        this.paused.set(false);
    }
    
    protected boolean isPaused() {
        return this.paused.get();
    }
}
```

### 5.3 暂停期间的心跳维持

#### 5.3.1 心跳机制原理

```
┌─────────────────────────────────────────────────────────────┐
│                    Kafka 心跳机制                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   Consumer                          Coordinator             │
│      │                                  │                   │
│      │────── Heartbeat Request ────────▶│                   │
│      │◀───── Heartbeat Response ───────│                   │
│      │                                  │                   │
│      │────── Heartbeat Request ────────▶│                   │
│      │◀───── Heartbeat Response ───────│                   │
│      │                                  │                   │
│   关键：心跳在后台线程执行，不受 poll 影响                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

#### 5.3.2 暂停不会影响心跳的原因

**核心原因**：Kafka 的心跳是由**独立的后台线程**执行的，与 `poll()` 方法无关。

```java
// Kafka Consumer 内部结构
public class KafkaConsumer<K, V> {
    
    // 心跳线程（独立于主消费者线程）
    private final HeartbeatThread heartbeatThread;
    
    // 主消费者线程
    private class ConsumerLoop implements Runnable {
        public void run() {
            while (running) {
                // 这里检查 pause 标志
                if (!paused) {
                    consumer.poll(pollTimeout);
                } else {
                    // 条件阻塞，线程挂起
                }
            }
        }
    }
    
    // 心跳线程（始终运行，不受 pause 影响）
    private class HeartbeatThread implements Runnable {
        public void run() {
            while (running) {
                sendHeartbeat();
                Thread.sleep(heartbeatInterval);
            }
        }
    }
}
```

**关键配置参数**：

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| `session.timeout.ms` | 会话超时时间，默认 10 秒 | 100000 (100秒) |
| `heartbeat.interval.ms` | 心跳间隔，默认 3 秒 | 30000 (30秒) |
| `max.poll.interval.ms` | 两次 poll 最大间隔，默认 5 分钟 | 300000 (5分钟) |

#### 5.3.3 为什么暂停不会触发 Rebalance

```
┌─────────────────────────────────────────────────────────┐
│                  Rebalance 触发条件                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  1. 消费者心跳超时                                       │
│     └── 暂停期间心跳仍在继续 → 不会超时                   │
│                                                         │
│  2. 消费者主动离开组                                     │
│     └── pause() 不主动离开组 → 不会触发                   │
│                                                         │
│  3. 消费者被强制下线                                     │
│     └── pause() 不改变组状态 → 不会触发                   │
│                                                         │
│  结论：pause() 只是暂停消费，不改变消费者组状态            │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 5.4 暂停/恢复对消费者组的影响

#### 5.4.1 消费者组状态

```
                    消费者组状态转换
                          
    ┌──────────┐      join       ┌──────────┐
    │  Empty   │ ──────────────▶│ Stable   │
    └──────────┘                └────┬─────┘
                                     │
                                     │ rebalance
                                     ▼
                              ┌──────────┐
                              │ Rebalancing │
                              └──────────┘
```

**暂停/恢复对状态的影响**：

| 操作 | 消费者组状态 | 分区分配 | 说明 |
|------|-------------|---------|------|
| `pause()` | Stable | 不变 | 消费者暂停拉取，但仍是组成员 |
| `resume()` | Stable | 不变 | 消费者恢复拉取 |

#### 5.4.2 分区分配保持不变

```
暂停前:
Topic: data-sync-topic [P0, P1, P2]
                    │
                    ▼
Consumer Group: [C1 → P0], [C2 → P1], [C3 → P2]

暂停后:
Topic: data-sync-topic [P0, P1, P2]
                    │
                    ▼
Consumer Group: [C1 → P0 (paused)], [C2 → P1], [C3 → P2]
                    │
                    ▼
分区分配不变，只是 C1 暂停消费

恢复后:
Topic: data-sync-topic [P0, P1, P2]
                    │
                    ▼
Consumer Group: [C1 → P0], [C2 → P1], [C3 → P2]
                    │
                    ▼
分区分配完全恢复
```

### 5.5 暂停期间的消息处理

#### 5.5.1 消息状态

```
时间线:

T1: poll() 获取消息 [msg1, msg2, msg3]
         ↓
T2: 消息处理中...
         ↓
T3: 调用 pause()
         ↓
T4: 已获取的消息继续处理完成
         ↓
T5: 下次 poll() → 返回空（暂停状态）
         ↓
T6: 条件阻塞，直到调用 resume()
         ↓
T7: resume() → 恢复正常 poll()
```

**关键点**：
- `pause()` 不会中断正在处理的消息
- 已获取的消息会继续处理完成
- 暂停后、下次 poll 返回空

#### 5.5.2 偏移量提交

```
场景：处理消息时调用 pause()

1. poll() 获取消息 [msg1, msg2, msg3]
         ↓
2. 开始处理 msg1
         ↓
3. msg1 处理成功 → commit offset 1
         ↓
4. 调用 pause()
         ↓
5. msg2, msg3 继续处理完成（如果业务代码没有检查 pause）
         ↓
6. 如果没有手动 commit，offset 停留在 msg1
         ↓
7. resume() 后，会重新消费 msg2, msg3
```

**风险提示**：如果在处理过程中调用 `pause()`，可能导致部分消息被重复消费。

### 5.6 配置参数的作用

#### 5.6.1 session.timeout.ms

```properties
# 默认值：100000 (10秒)
# 作用：消费者必须在该时间内发送心跳，否则被认为已死

# 如果暂停时间超过此值，会发生什么？
# 答：不会！因为心跳线程独立运行，不受 pause() 影响
```

#### 5.6.2 heartbeat.interval.ms

```properties
# 默认值：30000 (3秒)
# 作用：心跳发送间隔

# 建议值：session.timeout.ms / 3
# 例如：session.timeout=100000 → heartbeat.interval=30000
```

#### 5.6.3 max.poll.interval.ms

```properties
# 默认值：300000 (5分钟)
# 作用：两次 poll 之间的最大间隔

# 重要：如果暂停时间过长，可能触发此超时！
# 因为 pause() 返回空，poll() 返回空 = 没有处理新消息
```

**风险提示**：
```
如果暂停时间 > max.poll.interval.ms:
    ↓
Consumer 被认为已死
    ↓
触发 Rebalance
    ↓
分区重新分配给其他消费者
    ↓
resume() 后，消费者重新加入，可能从之前位置继续消费
```

### 5.7 暂停/恢复的最佳实践

#### 5.7.1 配置建议

```properties
# 延长 max.poll.interval.ms 以支持长时间暂停
max.poll.interval.ms=600000  # 10分钟

# 确保心跳频率足够高
session.timeout.ms=100000     # 10秒
heartbeat.interval.ms=30000  # 3秒
```

#### 5.7.2 代码示例

```java
@Service
public class MessageConsumerService {

    private final AtomicBoolean paused = new AtomicBoolean(false);

    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void consume(List<ConsumerRecord<String, String>> records,
                        Acknowledgment ack) {
        
        // 检查暂停状态
        if (paused.get()) {
            return; // 不处理，返回
        }
        
        for (ConsumerRecord<String, String> record : records) {
            // 处理中也可以检查暂停状态
            if (paused.get()) {
                break;
            }
            // 处理消息...
        }
        
        // 处理完成后提交
        ack.acknowledge();
    }

    public void pause() {
        paused.set(true);
        container.pause();
    }

    public void resume() {
        paused.set(false);
        container.resume();
    }
}
```

#### 5.7.3 注意事项

| 注意事项 | 说明 |
|----------|------|
| 暂停时间 | 不要超过 `max.poll.interval.ms`，否则会触发 Rebalance |
| 心跳 | 暂停期间心跳继续，不会超时 |
| 偏移量 | 暂停前的偏移量需要手动管理 |
| 重试 | 暂停期间失败的消息，resume 后会重新消费 |

---

## 6. 注意事项与建议

### 6.1 性能优化

| 优化项 | 操作 | 效果 |
|--------|------|------|
| 批量消费 | `setBatchListener(true)` | 减少网络往返 |
| 并发消费 | `setConcurrency(N)` | 提高并行度 |
| 批量发送 | `batch.size` | 减少网络开销 |
| 消息压缩 | `compression.type=snappy` | 减少网络传输 |

### 6.2 安全性配置

| 配置 | 说明 | 场景 |
|------|------|------|
| SSL | 加密通信 | 生产环境 |
| SASL | 身份认证 | 生产环境 |
| ACL | 权限控制 | 多租户环境 |

### 6.3 监控指标

| 指标 | 告警阈值 | 说明 |
|------|----------|------|
| consumer_lag | > 10000 | 消费积压 |
| records_consumed_rate | < 100 | 消费速率异常 |
| failed_records_rate | > 10% | 失败率过高 |

---

## 附录

### 配置速查表

| 分类 | 参数 | 推荐值 | 说明 |
|------|------|--------|------|
| Broker | `num.io.threads` | CPU×2 | IO 线程数 |
| Broker | `log.retention.hours` | 168 | 保留时间 |
| Producer | `acks` | all | 可靠性 |
| Producer | `compression.type` | snappy | 压缩 |
| Consumer | `max.poll.records` | 500 | 批量大小 |
| Consumer | `enable.auto.commit` | false | 手动提交 |
| Consumer | `session.timeout.ms` | 100000 | 会话超时 |
| Consumer | `heartbeat.interval.ms` | 30000 | 心跳间隔 |

### 相关代码

- `MessageConsumerService.java` - 消费者服务实现
- `SyncController.java` - 暂停/恢复 API
- `SyncMessage.java` - 消息模型

---

**文档版本**: 1.2  
**更新日期**: 2026-03-13  
**更新内容**: 
- 增加 Kafka 问题及解决方案
- 增加消费者暂停/恢复机制原理详解
