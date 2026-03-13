# Vector Sync Service

大规模数据同步服务，通过 Kafka 将表数据同步至 Elasticsearch 向量数据库。

## 技术栈

- **Spring Boot 3.2.0** - 应用程序框架
- **Java 17** - 开发语言
- **Apache Kafka** - 消息队列
- **Elasticsearch 8.11.0** - 向量数据库
- **Spring Kafka** - Kafka 集成

## 功能特性

- **Kafka 消费**: 从 Kafka 主题消费数据同步消息，支持批量处理（详见 [Kafka 技术文档](technical_issues/kafka.md)）
- **向量转换**: 将业务数据转换为带向量字段的文档，支持稠密向量和稀疏向量
- **批量索引**: 高效批量写入 Elasticsearch
- **重试机制**: 指数退避重试策略，失败消息自动进入死信队列
- **流量控制**: 支持消费者暂停/恢复，防止故障扩散
- **监控指标**: 提供同步状态、健康检查、文档管理等 REST API

## Kafka 核心难点

本项目在 Kafka 使用过程中遇到并解决了多个核心难点，详细分析见 [Kafka 技术文档](technical_issues/kafka.md)：

### 消费可靠性

- **偏移量提交策略**: 从自动提交改为手动提交，确保消息处理成功后才提交
- **部分成功处理**: 根据成功率阈值决定是否提交，避免丢消息或重复消费
- **连续失败检测**: 累计连续失败次数，自动暂停消费者防止故障扩散

### 重试与死信

- **重试队列简化**: 失败消息发回原队列末尾，而非创建独立重试队列
- **死信队列 (DLQ)**: 超过最大重试次数的消息进入死信队列，供后续排查

### 消费者暂停/恢复

- **暂停机制原理**: 通过设置 paused 标志，poll() 返回空集合但不停止消费者线程
- **心跳维持**: 暂停期间心跳线程独立运行，不会触发 Rebalance
- **配置优化**: 延长 max.poll.interval.ms 支持长时间暂停

## 消息格式

同步消息 `SyncMessage` 包含以下字段：

```json
{
  "message_key": "唯一键",
  "id": "文档ID",
  "type": "数据类型",
  "action": "操作类型 (create/update/delete)",
  "data": { "业务数据" },
  "timestamp": "时间戳",
  "version": "版本号"
}
```

## API 接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/sync/status` | GET | 获取同步状态和统计 |
| `/api/sync/health` | GET | 健康检查 |
| `/api/sync/pause` | POST | 暂停消费者 |
| `/api/sync/resume` | POST | 恢复消费者 |
| `/api/sync/reindex/{id}` | POST | 重建指定文档 |
| `/api/sync/delete/{id}` | DELETE | 删除指定文档 |
| `/api/sync/clear-retry-count` | POST | 清空重试计数 |

## 配置说明

主要配置项位于 `src/main/resources/application.yml`：

### Kafka 配置
```yaml
spring:
  kafka:
    bootstrap-servers: 192.168.116.5:9092
    consumer:
      group-id: vector-sync-group
      concurrency: 5
```

### Elasticsearch 配置
```yaml
spring:
  elasticsearch:
    uris: http://192.168.116.5:9200

sync:
  elasticsearch:
    index: vectors
    bulk-size: 100
    flush-interval-seconds: 5
```

### 向量配置
```yaml
sync:
  vector:
    provider: local
    dimension: 384
    model-name: sentence-transformers/all-MiniLM-L6-v2
```

### 重试配置
```yaml
sync:
  retry:
    max-attempts: 3
    initial-interval-ms: 1000
    multiplier: 2.0
    max-interval-ms: 10000
    max-consecutive-failures: 10
    success-threshold: 0.2
```

## 快速开始

### 1. 环境要求

- Java 17+
- Maven 3.8+
- Kafka (运行在 192.168.116.5:9092)
- Elasticsearch (运行在 192.168.116.5:9200)

### 2. 构建项目

```bash
mvn clean package -DskipTests
```

### 3. 运行服务

```bash
java -jar target/vector-sync-service-1.0.0.jar
```

服务默认运行在端口 9001。

### 4. 验证服务

```bash
# 健康检查
curl http://localhost:9001/api/sync/health

# 查看同步状态
curl http://localhost:9001/api/sync/status
```

## 监控

服务提供 Spring Boot Actuator 端点：

- `/actuator/health` - 健康状态
- `/actuator/info` - 应用信息
- `/actuator/metrics` - 性能指标

## 架构说明

```
[数据源] → [Kafka: data-sync-topic] → [MessageConsumerService]
                                          ↓
                                    [VectorTransformService]
                                          ↓
                                    [ElasticsearchService]
                                          ↓
                                    [Elasticsearch: vectors]
```

## 扩展

如需使用外部向量服务，可实现 `VectorApiService` 接口并配置：

```yaml
vector:
  api:
    base-url: http://localhost:9003
    timeout: 30000
    enabled: true
```
