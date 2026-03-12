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
    private final SyncProperties syncProperties;
    private final ObjectMapper objectMapper;
    private final KafkaListenerEndpointRegistry listenerRegistry;

    @Getter
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean manuallyPaused = new AtomicBoolean(false);
    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    private final LongAdder totalProcessed = new LongAdder();
    private final LongAdder totalSuccess = new LongAdder();
    private final LongAdder totalFailed = new LongAdder();
    private final LongAdder totalSkipped = new LongAdder();
    private final LongAdder totalRetried = new LongAdder();
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final List<String> failedMessageIds = Collections.synchronizedList(new ArrayList<>());

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

        long batchStartOffset = records.get(0).offset();
        long batchEndOffset = records.get(records.size() - 1).offset();

        List<ConsumerRecord<String, String>> successRecords = new ArrayList<>();
        List<ConsumerRecord<String, String>> failedRecords = new ArrayList<>();

        for (ConsumerRecord<String, String> record : records) {
            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    log.warn("Message missing ID, skipping offset: {}", record.offset());
                    successRecords.add(record);
                    totalSkipped.increment();
                    continue;
                }

                boolean success = processWithRetry(message);
                
                if (success) {
                    successRecords.add(record);
                    totalSuccess.increment();
                } else {
                    failedRecords.add(record);
                    totalFailed.increment();
                    failedMessageIds.add(message.getId());
                }

            } catch (JsonProcessingException e) {
                log.error("Failed to parse message at offset {}: {}", record.offset(), e.getMessage());
                successRecords.add(record);
                totalSkipped.increment();
            } catch (Exception e) {
                log.error("Failed to process message at offset {}: {}", record.offset(), e.getMessage());
                failedRecords.add(record);
                totalFailed.increment();
            }
        }

        totalProcessed.add(records.size());

        if (!failedRecords.isEmpty()) {
            int currentFailures = consecutiveFailures.addAndGet(failedRecords.size());
            log.warn("Batch {}-{} had {} failed records, consecutive failures: {}", 
                    batchStartOffset, batchEndOffset, failedRecords.size(), currentFailures);

            if (currentFailures >= syncProperties.getRetry().getMaxConsecutiveFailures()) {
                log.error("Max consecutive failures reached ({}), pausing consumer", 
                        syncProperties.getRetry().getMaxConsecutiveFailures());
                pause();
            }
        } else {
            consecutiveFailures.set(0);
            failedMessageIds.clear();
        }

        if (!successRecords.isEmpty()) {
            acknowledgment.acknowledge();
            log.info("Acknowledged {} records", successRecords.size());
        }

        lastFlushTime.set(System.currentTimeMillis());
    }

    private boolean processWithRetry(SyncMessage message) {
        int maxRetries = syncProperties.getRetry().getMaxAttempts();
        long initialDelay = syncProperties.getRetry().getInitialIntervalMs();
        double multiplier = syncProperties.getRetry().getMultiplier();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                elasticsearchService.bulkIndex(List.of(message));
                return true;
            } catch (Exception e) {
                log.warn("Attempt {}/{} failed for message id: {}, error: {}", 
                        attempt, maxRetries, message.getId(), e.getMessage());
                
                if (attempt < maxRetries) {
                    totalRetried.increment();
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
                    log.warn("Consumer PAUSED due to consecutive failures. Failed message IDs: {}", 
                            failedMessageIds);
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

    public void pauseManually() {
        manuallyPaused.set(true);
        pause();
        log.info("Consumer manually paused");
    }

    public void resumeManually() {
        manuallyPaused.set(false);
        resume();
        log.info("Consumer manually resumed");
    }

    public boolean isPaused() {
        return paused.get();
    }

    public SyncStatus getStatus() {
        return SyncStatus.builder()
                .paused(paused.get())
                .manuallyPaused(manuallyPaused.get())
                .totalProcessed(totalProcessed.sum())
                .totalSuccess(totalSuccess.sum())
                .totalFailed(totalFailed.sum())
                .totalSkipped(totalSkipped.sum())
                .totalRetried(totalRetried.sum())
                .consecutiveFailures(consecutiveFailures.get())
                .failedMessageIds(new ArrayList<>(failedMessageIds))
                .lastFlushTime(lastFlushTime.get())
                .build();
    }

    @lombok.Data
    @lombok.Builder
    public static class SyncStatus {
        private boolean paused;
        private boolean manuallyPaused;
        private long totalProcessed;
        private long totalSuccess;
        private long totalFailed;
        private long totalSkipped;
        private long totalRetried;
        private int consecutiveFailures;
        private List<String> failedMessageIds;
        private long lastFlushTime;
    }
}
