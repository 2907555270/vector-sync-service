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

        List<ConsumerRecord<String, String>> processedRecords = new ArrayList<>();
        List<ConsumerRecord<String, String>> successRecords = new ArrayList<>();
        List<ConsumerRecord<String, String>> failedRecords = new ArrayList<>();
        boolean pausedDuringProcessing = false;

        for (ConsumerRecord<String, String> record : records) {
            if (paused.get()) {
                log.warn("Consumer paused during batch processing at offset {}, stopping", record.offset());
                pausedDuringProcessing = true;
                break;
            }

            processedRecords.add(record);

            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    successRecords.add(record);
                    continue;
                }

                boolean success = processWithRetry(message);
                
                if (success) {
                    successRecords.add(record);
                    retryService.clearRetryCount(getMessageKey(record));
                } else {
                    failedRecords.add(record);
                }

            } catch (JsonProcessingException e) {
                log.error("Failed to parse message at offset {}: {}", record.offset(), e.getMessage());
                successRecords.add(record);
            } catch (Exception e) {
                log.error("Failed to process message at offset {}: {}", record.offset(), e.getMessage());
                failedRecords.add(record);
            }
        }

        totalProcessed.add(processedRecords.size());
        totalSuccess.add(successRecords.size());
        totalFailed.add(failedRecords.size());

        int processedCount = processedRecords.size();
        double successRate = processedCount > 0 ? (double) successRecords.size() / processedCount : 0;
        double threshold = syncProperties.getRetry().getSuccessThreshold();

        boolean shouldAck = successRecords.size() > 0 && successRate >= threshold;
        
        if (shouldAck) {
            acknowledgment.acknowledge();
            
            if (!failedRecords.isEmpty()) {
                log.warn("Batch acknowledged with failures. Success: {}, Failed: {}, Rate: {}", 
                    successRecords.size(), failedRecords.size(), successRate);
                sendFailedToRetryOrDlq(failedRecords);
            } else {
                consecutiveBatchFailures.set(0);
                log.info("Batch all success, acknowledged {} records", processedCount);
            }
        } else if (processedRecords.isEmpty() || pausedDuringProcessing) {
            log.warn("Paused or empty batch, not acknowledging. Processed: {}", processedCount);
        } else {
            int currentFailures = consecutiveBatchFailures.incrementAndGet();
            log.error("Batch success rate {} < {}, consecutive batch failures: {}", 
                successRate, threshold, currentFailures);
            
            if (currentFailures >= syncProperties.getRetry().getMaxConsecutiveFailures()) {
                pause();
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

        List<ConsumerRecord<String, String>> successRecords = new ArrayList<>();
        List<ConsumerRecord<String, String>> failedRecords = new ArrayList<>();
        boolean pausedDuringProcessing = false;

        for (ConsumerRecord<String, String> record : records) {
            if (paused.get()) {
                pausedDuringProcessing = true;
                break;
            }

            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    successRecords.add(record);
                    continue;
                }

                boolean success = processWithRetry(message);
                
                if (success) {
                    successRecords.add(record);
                    retryService.clearRetryCount(getMessageKey(record));
                } else {
                    failedRecords.add(record);
                }

            } catch (Exception e) {
                log.error("Failed to process retry message: {}", e.getMessage());
                failedRecords.add(record);
            }
        }

        int processedCount = successRecords.size() + failedRecords.size();
        double successRate = processedCount > 0 ? (double) successRecords.size() / processedCount : 0;
        double threshold = syncProperties.getRetry().getSuccessThreshold();

        boolean shouldAck = successRecords.size() > 0 && successRate >= threshold;
        
        if (shouldAck) {
            acknowledgment.acknowledge();
            
            if (!failedRecords.isEmpty()) {
                totalSuccess.add(successRecords.size());
                totalFailed.add(failedRecords.size());
                log.warn("Retry batch acknowledged with failures. Success: {}, Failed: {}", 
                    successRecords.size(), failedRecords.size());
                sendFailedToRetryOrDlq(failedRecords);
            } else {
                retryConsecutiveBatchFailures.set(0);
                totalSuccess.add(successRecords.size());
                log.info("Retry batch all success, acknowledged");
            }
        } else if (processedCount == 0 || pausedDuringProcessing) {
            log.warn("Paused or empty retry batch, not acknowledging");
        } else {
            totalSuccess.add(successRecords.size());
            totalFailed.add(failedRecords.size());
            
            int batchFailures = retryConsecutiveBatchFailures.incrementAndGet();
            log.error("Retry batch success rate {} < {}, consecutive batch failures: {}", 
                successRate, threshold, batchFailures);
            
            sendFailedToRetryOrDlq(failedRecords);
            
            if (batchFailures >= syncProperties.getRetry().getMaxConsecutiveFailures()) {
                pause();
            }
        }
    }

    private String getMessageKey(ConsumerRecord<String, String> record) {
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }

    private void sendFailedToRetryOrDlq(List<ConsumerRecord<String, String>> failedRecords) {
        for (ConsumerRecord<String, String> record : failedRecords) {
            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                if (message.getId() == null) continue;
                
                String messageKey = getMessageKey(record);
                int currentRetryCount = retryService.getRetryCount(messageKey);
                
                if (currentRetryCount >= syncProperties.getRetry().getMaxAttempts()) {
                    retryService.sendToDlq(message.getId(), record.value(), "Max retries exceeded", currentRetryCount);
                    totalDlq.increment();
                    retryService.clearRetryCount(messageKey);
                } else {
                    retryService.incrementRetryCount(messageKey);
                    retryService.sendToRetry(message.getId(), record.value(), currentRetryCount);
                    totalRetried.increment();
                }
            } catch (Exception e) {
                log.error("Failed to send to retry: {}", e.getMessage());
            }
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
                .successThreshold(syncProperties.getRetry().getSuccessThreshold())
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
        private double successThreshold;
        private String retryTopic;
        private String dlqTopic;
        private long lastFlushTime;
    }
}
