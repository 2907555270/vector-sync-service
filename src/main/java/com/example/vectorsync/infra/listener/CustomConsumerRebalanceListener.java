package com.example.vectorsync.infra.listener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class CustomConsumerRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(CustomConsumerRebalanceListener.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (!partitions.isEmpty()) {
            log.warn("Rebalance 开始！被收回的分区数: {}", partitions.size());
            log.warn("被收回的分区: {}", partitions);
            // 这里可以做：提交 pending offset、清理状态、暂停业务处理等
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (!partitions.isEmpty()) {
            log.info("Rebalance 完成！新分配的分区数: {}", partitions.size());
            log.info("新分配的分区: {}", partitions);
            // 这里可以做：seek 到指定 offset、恢复业务处理、记录时间戳等
        }
    }

    // Kafka 2.4+ 可选重写（分区丢失时调用，通常与 revoked 类似）
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        log.error("分区丢失（可能消费者被踢出组）: {}", partitions);
    }
}
