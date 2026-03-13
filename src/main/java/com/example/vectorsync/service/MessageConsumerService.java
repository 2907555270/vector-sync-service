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
            log.warn("Consumer is paused, not processing records");
            return;
        }

        if (records == null || records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        log.info("Received batch of {} messages", records.size());

        int processedCount = 0;
        boolean pausedDuringProcessing = false;

        for (ConsumerRecord<String, String> record : records) {
            if (paused.get()) {
                log.warn("Consumer paused during batch processing at offset {}, stopping", record.offset());
                pausedDuringProcessing = true;
                break;
            }

            processedCount++;

            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    continue;
                }

                boolean success = processWithRetry(message);
                
                if (success) {
                    totalSuccess.increment();
                    retryService.clearRetryCount(message.getId());
                } else {
                    totalFailed.increment();
                }

            } catch (JsonProcessingException e) {
                log.error("Failed to parse message at offset {}: {}", record.offset(), e.getMessage());
                totalSuccess.increment();
            } catch (Exception e) {
                log.error("Failed to process message at offset {}: {}", record.offset(), e.getMessage());
                totalFailed.increment();
            }
        }

        totalProcessed.add(processedCount);

        if (pausedDuringProcessing) {
            log.warn("Paused during processing, not acknowledging. Processed {} messages", processedCount);
            return;
        }

        boolean allProcessed = processedCount == records.size();
        boolean hasFailure = totalFailed.sum() > 0;

        if (allProcessed) {
            acknowledgment.acknowledge();
            
            if (!hasFailure) {
                consecutiveBatchFailures.set(0);
                log.info("Batch all success, acknowledged {} records", processedCount);
            } else {
                int currentFailures = consecutiveBatchFailures.incrementAndGet();
                log.error("Batch completed with failures, consecutive batch failures: {}", currentFailures);
                
                if (currentFailures >= syncProperties.getRetry().getMaxConsecutiveFailures()) {
                    pause();
                }
            }
        } else {
            acknowledgment.acknowledge();
            log.warn("Batch partially processed ({} / {}), acknowledged", processedCount, records.size());
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

        int processedCount = 0;
        boolean pausedDuringProcessing = false;
        int successCount = 0;
        int failCount = 0;

        for (ConsumerRecord<String, String> record : records) {
            if (paused.get()) {
                pausedDuringProcessing = true;
                break;
            }

            processedCount++;

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

        if (pausedDuringProcessing) {
            log.warn("Paused during retry processing, not acknowledging");
            return;
        }

        boolean allProcessed = processedCount == records.size();

        if (allProcessed) {
            acknowledgment.acknowledge();
            
            if (failCount == 0) {
                retryConsecutiveBatchFailures.set(0);
                totalSuccess.add(successCount);
                log.info("Retry batch all success");
            } else if (successCount == 0) {
                totalFailed.add(failCount);
                int batchFailures = retryConsecutiveBatchFailures.incrementAndGet();
                log.error("Retry batch all failed, consecutive batch failures: {}", batchFailures);
                
                for (ConsumerRecord<String, String> record : records) {
                    trySendToRetryOrDlq(record, "Retry batch all failed");
                }
                
                if (batchFailures >= syncProperties.getRetry().getMaxConsecutiveFailures()) {
                    pause();
                }
            } else {
                totalSuccess.add(successCount);
                totalFailed.add(failCount);
                retryConsecutiveBatchFailures.set(0);
                
                for (ConsumerRecord<String, String> record : records) {
                    trySendToRetryOrDlq(record, "Retry partially failed");
                }
            }
        }
    }

    private void trySendToRetryOrDlq(ConsumerRecord<String, String> record, String reason) {
        try {
            SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
            if (message.getId() == null) return;
            
            int currentRetryCount = retryService.getRetryCount(message.getId());
            
            if (currentRetryCount >= syncProperties.getRetry().getMaxAttempts()) {
                retryService.sendToDlq(message.getId(), record.value(), reason, currentRetryCount);
                totalDlq.increment();
                retryService.clearRetryCount(message.getId());
            } else {
                retryService.incrementRetryCount(message.getId());
                retryService.sendToRetry(message.getId(), record.value(), currentRetryCount);
                totalRetried.increment();
            }
        } catch (Exception e) {
            log.error("Failed to handle retry failed message: {}", e.getMessage());
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
