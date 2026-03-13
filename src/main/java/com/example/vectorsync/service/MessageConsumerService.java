package com.example.vectorsync.service;

import com.example.vectorsync.config.SyncProperties;
import com.example.vectorsync.model.SyncMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.*;
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
    
    private final AtomicInteger consecutiveBatchFailures = new AtomicInteger(0);
    private final AtomicInteger retryConsecutiveBatchFailures = new AtomicInteger(0);

    public static final String LISTENER_ID = "messageConsumerService";
    public static final String RETRY_LISTENER_ID = "retryMessageConsumerService";

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
            acknowledgment.acknowledge();
            return;
        }

        log.info("Received batch of {} messages", records.size());

        int successCount = 0;
        int failCount = 0;

        for (ConsumerRecord<String, String> record : records) {
            if (paused.get()) {
                log.warn("Consumer paused during batch processing");
                break;
            }

            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    successCount++;
                    continue;
                }

                boolean success = processWithRetry(message);
                
                if (success) {
                    successCount++;
                    retryService.clearRetryCount(message.getId());
                } else {
                    failCount++;
                }

            } catch (JsonProcessingException e) {
                log.error("Failed to parse message at offset {}: {}", record.offset(), e.getMessage());
                successCount++;
            } catch (Exception e) {
                log.error("Failed to process message at offset {}: {}", record.offset(), e.getMessage());
                failCount++;
            }
        }

        totalProcessed.add(records.size());

        if (failCount == 0) {
            acknowledgment.acknowledge();
            totalSuccess.add(successCount);
            consecutiveBatchFailures.set(0);
            log.info("Batch all success, acknowledged {} records", successCount);
        } else if (successCount == 0) {
            acknowledgment.acknowledge();
            totalFailed.add(failCount);
            int batchFailures = consecutiveBatchFailures.incrementAndGet();
            log.error("Batch all failed ({}), consecutive batch failures: {}", failCount, batchFailures);
            
            if (batchFailures >= syncProperties.getRetry().getMaxConsecutiveFailures()) {
                pause();
            }
        } else {
            acknowledgment.acknowledge();
            totalSuccess.add(successCount);
            totalFailed.add(failCount);
            consecutiveBatchFailures.set(0);
            
            log.warn("Batch partially failed ({} success, {} failed), sending failed to retry queue", 
                successCount, failCount);
            
            for (ConsumerRecord<String, String> record : records) {
                try {
                    SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                    if (message.getId() != null && !message.getId().isEmpty()) {
                        handleFailedMessage(message, record.value());
                    }
                } catch (Exception e) {
                    log.error("Failed to handle failed record: {}", e.getMessage());
                }
            }
        }

        lastFlushTime.set(System.currentTimeMillis());
    }

    @KafkaListener(
            id = RETRY_LISTENER_ID,
            topics = "${sync.retry.topic:data-sync-retry}",
            groupId = "${spring.kafka.consumer.group-id:vector-sync-group}-retry",
            containerFactory = "retryKafkaListenerContainerFactory"
    )
    public void consumeRetryBatch(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment acknowledgment
    ) {
        if (paused.get()) {
            log.warn("Consumer is paused, not processing retry records");
            return;
        }

        if (records == null || records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        log.info("Received retry batch of {} messages", records.size());

        int successCount = 0;
        int failCount = 0;

        for (ConsumerRecord<String, String> record : records) {
            if (paused.get()) {
                break;
            }

            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    successCount++;
                    continue;
                }

                boolean success = processWithRetry(message);
                
                if (success) {
                    successCount++;
                    retryService.clearRetryCount(message.getId());
                } else {
                    failCount++;
                }

            } catch (Exception e) {
                log.error("Failed to process retry message: {}", e.getMessage());
                failCount++;
            }
        }

        if (failCount == 0) {
            acknowledgment.acknowledge();
            totalSuccess.add(successCount);
            retryConsecutiveBatchFailures.set(0);
            log.info("Retry batch all success, acknowledged");
        } else if (successCount == 0) {
            acknowledgment.acknowledge();
            totalFailed.add(failCount);
            int batchFailures = retryConsecutiveBatchFailures.incrementAndGet();
            log.error("Retry batch all failed, consecutive batch failures: {}", batchFailures);
            
            for (ConsumerRecord<String, String> record : records) {
                try {
                    SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                    if (message.getId() != null) {
                        handleRetryFailedMessage(message, record.value(), "Retry batch all failed");
                    }
                } catch (Exception e) {
                    log.error("Failed to send to DLQ: {}", e.getMessage());
                }
            }
            
            if (batchFailures >= syncProperties.getRetry().getMaxConsecutiveFailures()) {
                pause();
            }
        } else {
            acknowledgment.acknowledge();
            totalSuccess.add(successCount);
            totalFailed.add(failCount);
            retryConsecutiveBatchFailures.set(0);
            
            for (ConsumerRecord<String, String> record : records) {
                try {
                    SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                    if (message.getId() != null) {
                        handleRetryFailedMessage(message, record.value(), "Retry partially failed");
                    }
                } catch (Exception e) {
                    log.error("Failed to handle retry failed message: {}", e.getMessage());
                }
            }
        }
    }

    private void handleFailedMessage(SyncMessage message, String originalValue) {
        int currentRetryCount = retryService.getRetryCount(message.getId());
        
        if (currentRetryCount >= syncProperties.getRetry().getMaxAttempts()) {
            log.error("Message {} exceeded max retries, sending to DLQ", message.getId());
            retryService.sendToDlq(message.getId(), originalValue, "Max retries exceeded", currentRetryCount);
            totalDlq.increment();
            retryService.clearRetryCount(message.getId());
        } else {
            retryService.incrementRetryCount(message.getId());
            retryService.sendToRetry(message.getId(), originalValue, currentRetryCount);
            totalRetried.increment();
        }
    }

    private void handleRetryFailedMessage(SyncMessage message, String originalValue, String reason) {
        int currentRetryCount = retryService.getRetryCount(message.getId());
        
        if (currentRetryCount >= syncProperties.getRetry().getMaxAttempts()) {
            log.error("Retry message {} exceeded max retries, sending to DLQ", message.getId());
            retryService.sendToDlq(message.getId(), originalValue, reason, currentRetryCount);
            totalDlq.increment();
            retryService.clearRetryCount(message.getId());
        } else {
            retryService.incrementRetryCount(message.getId());
            retryService.sendToRetry(message.getId(), originalValue, currentRetryCount);
            totalRetried.increment();
        }
    }

    private boolean processWithRetry(SyncMessage message) {
        int maxRetries = syncProperties.getRetry().getMaxAttempts();
        long initialDelay = syncProperties.getRetry().getInitialIntervalMs();
        double multiplier = syncProperties.getRetry().getMultiplier();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            if (paused.get()) {
                return false;
            }

            try {
                elasticsearchService.bulkIndex(List.of(message));
                return true;
            } catch (Exception e) {
                log.warn("Attempt {}/{} failed for message id: {}", attempt, maxRetries, message.getId());
                
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep((long) (initialDelay * Math.pow(multiplier, attempt - 1)));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        return false;
    }

    public synchronized void pause() {
        if (!paused.get()) {
            try {
                MessageListenerContainer container = listenerRegistry.getListenerContainer(LISTENER_ID);
                if (container != null) {
                    container.pause();
                }
                MessageListenerContainer retryContainer = listenerRegistry.getListenerContainer(RETRY_LISTENER_ID);
                if (retryContainer != null) {
                    retryContainer.pause();
                }
                paused.set(true);
                log.error("Consumer PAUSED due to consecutive batch failures");
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
                }
                MessageListenerContainer retryContainer = listenerRegistry.getListenerContainer(RETRY_LISTENER_ID);
                if (retryContainer != null) {
                    retryContainer.resume();
                }
                paused.set(false);
                consecutiveBatchFailures.set(0);
                retryConsecutiveBatchFailures.set(0);
                log.info("Consumer RESUMED successfully");
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
                .consecutiveBatchFailures(consecutiveBatchFailures.get())
                .retryConsecutiveBatchFailures(retryConsecutiveBatchFailures.get())
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
        private int consecutiveBatchFailures;
        private int retryConsecutiveBatchFailures;
        private String retryTopic;
        private String dlqTopic;
        private long lastFlushTime;
    }
}
