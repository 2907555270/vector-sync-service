# Kafka 使用与配置技术文档

本文档详细记录了 Kafka 的环境配置、使用指南、遇到的问题及解决方案，以及相关的最佳实践建议。

---

## 目录

1. [Kafka 环境配置](#1-kafka-环境配置)
2. [Kafka 使用指南](#2-kafka-使用指南)
3. [遇到的问题及解决方案](#3-遇到的问题及解决方案)
4. [注意事项与建议](#4-注意事项与建议)

---

## 1. Kafka 环境配置

### 1.1 开发环境配置

#### Broker 配置

```properties
# broker 基本配置
bootstrap.servers=192.168.116.5:9092
listeners=PLAINTEXT://192.168.116.5:9092
advertised.listeners=PLAINTEXT://192.168.116.5:9092

# 日志存储
log.dirs=/tmp/kafka-logs
log.retention.hours=168
log.retention.check.interval.ms=300000

# 网络与线程
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Zookeeper 连接
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
```

#### 开发环境简化配置

```yaml
spring:
  kafka:
    bootstrap-servers: 192.168.116.5:9092
    consumer:
      group-id: vector-sync-group
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 500
      concurrency: 10
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
```

### 1.2 生产环境配置

#### Broker 生产级配置

```properties
# 集群配置
bootstrap.servers=192.168.116.5:9092,192.168.116.6:9092,192.168.116.7:9092

# 副本配置
default.replication.factor=3
min.insync.replicas=2

# 日志配置
log.dirs=/data/kafka-logs
log.retention.hours=168
log.retention.check.interval.ms=300000
log.segment.bytes=1073741824

# 性能配置
num.network.threads=9
num.io.threads=32
queued.max.requests=500
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400

# 安全配置
security.inter.broker.protocol=PLAINTEXT
```

### 1.3 Producer 配置

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| `bootstrap.servers` | Kafka 集群地址 | `host1:9092,host2:9092` |
| `key.serializer` | 键序列化方式 | `StringSerializer` |
| `value.serializer` | 值序列化方式 | `StringSerializer` |
| `acks` | 确认机制 | `all` (高可靠) / `1` (高性能) |
| `retries` | 重试次数 | `3` |
| `batch.size` | 批量大小 | `16384` |
| `linger.ms` | 批次等待时间 | `10` |
| `buffer.memory` | 发送缓冲区 | `33554432` |
| `compression.type` | 压缩方式 | `snappy` |

### 1.4 Consumer 配置

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| `bootstrap.servers` | Kafka 集群地址 | 同 Producer |
| `group.id` | 消费者组 ID | `vector-sync-group` |
| `key.deserializer` | 键反序列化方式 | `StringDeserializer` |
| `value.deserializer` | 值反序列化方式 | `StringDeserializer` |
| `auto.offset.reset` | 初始消费位置 | `earliest` |
| `enable.auto.commit` | 自动提交偏移量 | `false` (手动控制) |
| `max.poll.records` | 每次 poll 最大记录数 | `500` |
| `max.poll.interval.ms` | 两次 poll 最大间隔 | `300000` |
| `session.timeout.ms` | 会话超时时间 | `100000` |
| `heartbeat.interval.ms` | 心跳间隔 | `30000` |

### 1.5 网络与安全配置

```properties
# 网络配置
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://192.168.116.5:9092

# 连接配置
max.connections.per.ip=100
max.connections=1000

# SSL 配置（生产环境）
security.protocol=SSL
ssl.keystore.location=/path/to/keystore.jks
ssl.keystore.password=password
ssl.truststore.location=/path/to/truststore.jks
ssl.truststore.password=password

# SASL 配置
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
```

---

## 2. Kafka 使用指南

### 2.1 Topic 创建与管理

#### 创建 Topic

```bash
# 创建 topic
kafka-topics.sh \
  --create \
  --topic data-sync-topic \
  --partitions 10 \
  --replication-factor 1 \
  --bootstrap-server 192.168.116.5:9092

# 查看 topic 列表
kafka-topics.sh \
  --list \
  --bootstrap-server 192.168.116.5:9092

# 查看 topic 详情
kafka-topics.sh \
  --describe \
  --topic data-sync-topic \
  --bootstrap-server 192.168.116.5:9092
```

#### Topic 配置建议

| 配置项 | 推荐值 | 说明 |
|--------|--------|------|
| `partitions` | 消费者数量 × 2 | 分区数 |
| `replication-factor` | 3 | 副本数 |
| `retention.hours` | 168 | 保留时间 7 天 |

### 2.2 Producer 实现最佳实践

#### 基本 Producer 实现

```java
@Service
public class DataPreparationService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String topic, String key, String value) {
        kafkaTemplate.send(topic, key, value)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send message: {}", ex.getMessage());
                } else {
                    log.debug("Message sent successfully");
                }
            });
    }
}
```

#### 批量发送实现

```java
public void sendBatch(String topic, List<Message> messages) {
    ExecutorService executor = Executors.newFixedThreadPool(10);
    CountDownLatch latch = new CountDownLatch(messages.size());

    for (Message msg : messages) {
        executor.submit(() -> {
            try {
                kafkaTemplate.send(topic, msg.getKey(), msg.getValue())
                    .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Failed to send: {}", e.getMessage());
            } finally {
                latch.countDown();
            }
        });
    }

    latch.await(10, TimeUnit.MINUTES);
    executor.shutdown();
}
```

### 2.3 Consumer 实现最佳实践

#### 批量消费实现

```java
@Service
public class MessageConsumerService {

    @KafkaListener(
        topics = "${sync.kafka.topic}",
        groupId = "${sync.kafka.consumer-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeBatch(
        List<ConsumerRecord<String, String>> records,
        Acknowledgment acknowledgment
    ) {
        for (ConsumerRecord<String, String> record : records) {
            try {
                processMessage(record.value());
                acknowledgment.acknowledge();
            } catch (Exception e) {
                log.error("Failed to process: {}", e.getMessage());
            }
        }
    }
}
```

#### 消费者配置

```java
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
            kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);  // 启用批量消费
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(10);  // 并发数
        
        return factory;
    }
}
```

### 2.4 数据序列化/反序列化

#### JSON 序列化

```java
@Configuration
public class KafkaConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.116.5:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.116.5:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.vectorsync.model");
        return new DefaultKafkaConsumerFactory<>(props);
    }
}
```

### 2.5 监控与维护

#### 消费者组监控

```bash
# 查看消费者组状态
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.116.5:9092 \
  --group vector-sync-group \
  --describe

# 查看消费 lag
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.116.5:9092 \
  --group vector-sync-group \
  --describe \
  --members \
  --offsets
```

#### 偏移量管理

```bash
# 重置到最新
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.116.5:9092 \
  --group vector-sync-group \
  --reset-offsets \
  --topic data-sync-topic \
  --to-latest \
  --execute

# 重置到指定偏移量
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.116.5:9092 \
  --group vector-sync-group \
  --reset-offsets \
  --topic data-sync-topic \
  --to-offset 10000 \
  --execute

# 按时间重置
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.116.5:9092 \
  --group vector-sync-group \
  --reset-offsets \
  --topic data-sync-topic \
  --by-duration 1h \
  --execute
```

---

## 3. 遇到的问题及解决方案

### 问题 1：消费失败时偏移量仍被提交

#### 问题描述

在批量消费模式下，即使部分消息处理失败，偏移量仍然会被提交，导致消息丢失。

#### 根本原因

1. 使用了 `AckMode.MANUAL_IMMEDIATE`，每次 poll 后自动提交
2. `bulkIndex` 方法即使返回错误也不会抛出异常

#### 解决方案

1. 改为 `AckMode.MANUAL`，手动控制提交时机
2. 修改 `bulkIndex` 方法，当有错误时抛出异常
3. 改为逐条处理，只有全部成功才提交 ack

```java
// 正确的处理逻辑
for (ConsumerRecord<String, String> record : records) {
    boolean success = processWithRetry(message);
    if (success) {
        successRecords.add(record);
    } else {
        failedRecords.add(record);
    }
}

if (failedRecords.isEmpty()) {
    acknowledgment.acknowledge();
}
```

### 问题 2：循环消费导致资源浪费

#### 问题描述

当下游服务持续报错时，消费者无法 ack，导致每次 poll 都获取到相同消息，形成循环消费。

#### 根本原因

1. 失败后不 ack，消息保留在队列
2. 没有失败阈值检测机制
3. 没有暂停/恢复机制

#### 解决方案

1. 添加连续失败计数器
2. 达到阈值时自动暂停消费
3. 支持手动暂停/恢复

```java
// 连续失败检查
private boolean checkAndTriggerPause() {
    int currentFailures = consecutiveFailures.get();
    int maxConsecutive = syncProperties.getRetry().getMaxConsecutiveFailures();
    
    if (currentFailures >= maxConsecutive) {
        pause();
        return true;
    }
    return false;
}

// 暂停消费者
public synchronized void pause() {
    MessageListenerContainer container = 
        listenerRegistry.getListenerContainer(LISTENER_ID);
    container.pause();
    paused.set(true);
}
```

### 问题 3：多线程消费线程安全问题

#### 问题描述

需要多个消费者实例实现并行消费，但存在线程安全隐患。

#### 根本原因

1. Spring Kafka 的 `concurrency` 配置不正确
2. `AckMode` 配置为 `MANUAL_IMMEDIATE` 导致自动提交

#### 解决方案

1. 增加 `concurrency` 配置
2. 改为 `AckMode.MANUAL`
3. 确保统计变量使用线程安全类

```yaml
spring:
  kafka:
    consumer:
      concurrency: 10  # 配置并发数
      max-poll-records: 500
```

### 问题 4：ES 批量写入错误不抛异常

#### 问题描述

`bulkIndex` 方法即使 ES 返回错误也不会抛出异常，导致消费者认为成功。

#### 根本原因

Elasticsearch Java Client 的 `bulk` 方法即使有错误也会返回 200 状态码，需要检查 `response.errors()`。

#### 解决方案

```java
BulkResponse response = esClient.bulk(bulkRequest);

if (response.errors()) {
    List<String> errors = new ArrayList<>();
    for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
            errors.add(item.error().reason());
        }
    }
    throw new BulkIndexException(
        String.format("Bulk index failed: %d errors", errors.size()),
        result
    );
}
```

---

## 4. 注意事项与建议

### 4.1 性能优化策略

| 优化项 | 建议 | 说明 |
|--------|------|------|
| 批量处理 | 启用批量消费 | `setBatchListener(true)` |
| 并发消费 | 设置合适的 concurrency | 不超过分区数 |
| 批量大小 | `max.poll.records=500` | 根据实际调整 |
| 压缩 | 启用 `snappy` 压缩 | 减少网络传输 |
| 批量发送 | Producer 批量发送 | 减少请求次数 |

### 4.2 可扩展性考虑

```
扩展策略:
1. 增加分区数 → 提高并行度
2. 增加消费者数 → 提高消费速度（不超过分区数）
3. 升级 Broker 配置 → 提高吞吐量
4. 集群扩展 → 提高可用性
```

#### 分区规划公式

```
分区数 = 目标吞吐量 / 单分区吞吐量
消费者数 = 分区数（最佳）
```

| 场景 | 建议分区数 | 说明 |
|------|------------|------|
| 小数据量 | 6-10 | 简单场景 |
| 中等数据量 | 20-30 | 标准场景 |
| 大数据量 | 50+ | 高吞吐场景 |

### 4.3 灾难恢复计划

#### 数据备份

1. **Topic 备份**
   - 保留足够副本数（建议 3）
   - 定期检查副本同步状态

2. **偏移量备份**
   - 记录关键时间点的偏移量
   - 支持快速回滚

#### 故障恢复流程

```
1. 服务故障
   ↓
2. 自动检测（监控告警）
   ↓
3. 暂停消费（防止数据丢失）
   ↓
4. 排查问题
   ↓
5. 修复/重启服务
   ↓
6. 恢复消费
   ↓
7. 数据校验
```

### 4.4 安全最佳实践

| 安全措施 | 说明 |
|----------|------|
| **SSL/TLS 加密** | 生产环境必须启用 |
| **SASL 认证** | 身份验证 |
| **ACL 权限控制** | 限制 topic 访问 |
| **网络隔离** | 使用内网地址 |
| **敏感数据脱敏** | 日志中避免打印敏感信息 |

### 4.5 版本升级注意事项

| 升级类型 | 注意事项 |
|----------|----------|
| **Broker 升级** | 从低版本逐级升级 |
| **客户端升级** | 保持与 Broker 兼容 |
| **Spring Kafka 版本** | 与 Spring Boot 版本匹配 |

#### 兼容性矩阵

| Kafka Broker | Spring Kafka | 推荐程度 |
|--------------|--------------|----------|
| 2.8.x | 2.7.x | 推荐 |
| 2.8.x | 2.8.x | 推荐 |
| 3.0.x | 3.0.x | 推荐 |

### 4.6 监控指标建议

| 指标 | 告警阈值 | 说明 |
|------|----------|------|
| consumer_lag | > 10000 | 消费积压 |
| records_consumed_rate | < 100 | 消费速率异常 |
| failed_records_rate | > 10% | 失败率过高 |
| reconnect_rate | > 5/min | 连接异常 |

---

## 附录

### 相关配置文件位置

- `application.yml` - Spring Boot Kafka 配置
- `KafkaConsumerConfig.java` - 消费者配置类
- `MessageConsumerService.java` - 消费服务实现
- `DataPreparationService.java` - 生产者服务实现

### 参考资料

- [Apache Kafka 官方文档](https://kafka.apache.org/documentation/)
- [Spring Kafka 官方文档](https://docs.spring.io/spring-kafka/reference/)
- [Elasticsearch Java API Client](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html)

---

**文档版本**: 1.0  
**创建日期**: 2026-03-12  
**最后更新**: 2026-03-12
