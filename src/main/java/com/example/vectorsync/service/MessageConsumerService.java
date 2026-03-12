package com.example.vectorsync.service;

import com.example.vectorsync.config.SyncProperties;
import com.example.vectorsync.model.SyncMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageConsumerService {

    private final ElasticsearchService elasticsearchService;
    private final RetryService retryService;
    private final SyncProperties syncProperties;
    private final ObjectMapper objectMapper;
    private final KafkaListenerEndpointRegistry listenerRegistry;

    @Getter
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    private final LongAdder totalProcessed = new LongAdder();
    private final LongAdder totalSuccess = new LongAdder();
    private final LongAdder totalFailed = new LongAdder();
    private final LongAdder totalRetried = new LongAdder();
    private final LongAdder totalDlq = new LongAdder();
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    public static final String LISTENER_ID = "messageConsumerService";

    @KafkaListener(
            id = LISTENER_ID,
            topics = "${sync.kafka.topic}",
            groupId = "${sync.kafka.consumer-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeBatch(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment acknowledgment
    ) {
        if (paused.get()) {
            log.warn("Consumer is paused, not processing records. Records count: {}", records.size());
            return;
        }

        if (records == null || records.isEmpty()) {
            return;
        }

        log.info("Received batch of {} messages", records.size());

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        int batchFailedCount = 0;

        for (ConsumerRecord<String, String> record : records) {
            if (paused.get()) {
                log.warn("Consumer paused during batch processing, stopping at offset {}", record.offset());
                break;
            }

            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    log.warn("Message missing ID, skipping offset: {}", record.offset());
                    addOffsetToCommit(record, offsetsToCommit);
                    continue;
                }

                boolean success = processWithRetry(message);
                
                if (success) {
                    addOffsetToCommit(record, offsetsToCommit);
                    totalSuccess.increment();
                    retryService.clearRetryCount(message.getId());
                } else {
                    batchFailedCount++;
                    handleFailedMessage(message, record.value());
                }

            } catch (JsonProcessingException e) {
                log.error("Failed to parse message at offset {}: {}", record.offset(), e.getMessage());
                addOffsetToCommit(record, offsetsToCommit);
            } catch (Exception e) {
                log.error("Failed to process message at offset {}: {}", record.offset(), e.getMessage());
                batchFailedCount++;
            }
        }

        totalProcessed.add(records.size() - batchFailedCount);

        if (!offsetsToCommit.isEmpty()) {
            acknowledgment.addOffsets(offsetsToCommit);
            log.info("Acknowledged {} records", offsetsToCommit.size());
        }

        if (batchFailedCount > 0) {
            int currentFailures = consecutiveFailures.addAndGet(batchFailedCount);
            log.warn("Batch had {} failed records, consecutive failures: {}", batchFailedCount, currentFailures);

            if (currentFailures >= syncProperties.getRetry().getMaxConsecutiveFailures()) {
                log.error("Max consecutive failures reached, pausing consumer");
                pause();
            }
        } else {
            consecutiveFailures.set(0);
        }

        lastFlushTime.set(System.currentTimeMillis());
    }

    private void handleFailedMessage(SyncMessage message, String originalValue) {
        int currentRetryCount = retryService.getRetryCount(message.getId());
        
        if (currentRetryCount >= syncProperties.getRetry().getMaxAttempts()) {
            log.error("Message {} exceeded max retries ({}) , sending to DLQ", 
                message.getId(), syncProperties.getRetry().getMaxAttempts());
            retryService.sendToDlq(message.getId(), originalValue, 
                "Max retries exceeded", currentRetryCount);
            totalDlq.increment();
            retryService.clearRetryCount(message.getId());
        } else {
            retryService.incrementRetryCount(message.getId());
            retryService.sendToRetry(message.getId(), originalValue, currentRetryCount);
            totalRetried.increment();
        }
        
        totalFailed.increment();
    }

    private void addOffsetToCommit(ConsumerRecord<String, String> record, 
                                   Map<TopicPartition, OffsetAndMetadata> offsets) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        offsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
    }

    private boolean processWithRetry(SyncMessage message) {
        int maxRetries = syncProperties.getRetry().getMaxAttempts();
        long initialDelay = syncProperties.getRetry().getInitialIntervalMs();
        double multiplier = syncProperties.getRetry().getMultiplier();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            if (paused.get()) {
                log.warn("Consumer paused during retry, abandoning message {}", message.getId());
                return false;
            }

            try {
                elasticsearchService.bulkIndex(List.of(message));
                return true;
            } catch (Exception e) {
                log.warn("Attempt {}/{} failed for message id: {}, error: {}", 
                        attempt, maxRetries, message.getId(), e.getMessage());
                
                if (attempt < maxRetries) {
                    try {
                        long delay = (long) (initialDelay * Math.pow(multiplier, attempt - 1));
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        log.error("All retries exhausted for message id: {}", message.getId());
        return false;
    }

    public synchronized void pause() {
        if (!paused.get()) {
            try {
                MessageListenerContainer container = listenerRegistry.getListenerContainer(LISTENER_ID);
                if (container != null) {
                    container.pause();
                    paused.set(true);
                    log.error("Consumer PAUSED due to consecutive failures");
                }
            } catch (Exception e) {
                log.error("Failed to pause consumer: {}", e.getMessage());
            }
        }
    }

    public synchronized void resume() {
        if (paused.get()) {
            try {
                MessageListenerContainer container = listenerRegistry.getListenerContainer(LISTENER_ID);
                if (container != null) {
                    container.resume();
                    paused.set(false);
                    consecutiveFailures.set(0);
                    log.info("Consumer RESUMED successfully");
                }
            } catch (Exception e) {
                log.error("Failed to resume consumer: {}", e.getMessage());
            }
        }
    }

    public boolean isPaused() {
        return paused.get();
    }

    public SyncStatus getStatus() {
        return SyncStatus.builder()
                .paused(paused.get())
                .totalProcessed(totalProcessed.sum())
                .totalSuccess(totalSuccess.sum())
                .totalFailed(totalFailed.sum())
                .totalRetried(totalRetried.sum())
                .totalDlq(totalDlq.sum())
                .consecutiveFailures(consecutiveFailures.get())
                .retryTopic(retryService.getRetryTopic())
                .dlqTopic(retryService.getDlqTopic())
                .lastFlushTime(lastFlushTime.get())
                .build();
    }

    @lombok.Data
    @lombok.Builder
    public static class SyncStatus {
        private boolean paused;
        private long totalProcessed;
        private long totalSuccess;
        private long totalFailed;
        private long totalRetried;
        private long totalDlq;
        private int consecutiveFailures;
        private String retryTopic;
        private String dlqTopic;
        private long lastFlushTime;
    }
}
