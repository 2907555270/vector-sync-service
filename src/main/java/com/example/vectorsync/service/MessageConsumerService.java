package com.example.vectorsync.service;

import com.example.vectorsync.config.SyncProperties;
import com.example.vectorsync.model.SyncMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    private final LongAdder totalProcessed = new LongAdder();
    private final LongAdder totalSuccess = new LongAdder();
    private final LongAdder totalFailed = new LongAdder();
    private final LongAdder totalSkipped = new LongAdder();
    private final LongAdder totalRetried = new LongAdder();

    private final Set<String> skipMessageIds = Collections.synchronizedSet(new HashSet<>());
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final Map<String, Integer> messageRetryCount = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = "${sync.kafka.topic}",
            groupId = "${sync.kafka.consumer-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeBatch(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment acknowledgment
    ) {
        if (records == null || records.isEmpty()) {
            return;
        }

        log.info("Received batch of {} messages", records.size());

        long batchStartOffset = records.get(0).offset();
        long batchEndOffset = records.get(records.size() - 1).offset();

        List<ConsumerRecord<String, String>> successRecords = new ArrayList<>();
        List<ConsumerRecord<String, String>> failedRecords = new ArrayList<>();
        List<ConsumerRecord<String, String>> skipRecords = new ArrayList<>();

        for (ConsumerRecord<String, String> record : records) {
            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    log.warn("Message missing ID, skipping offset: {}", record.offset());
                    successRecords.add(record);
                    totalSkipped.increment();
                    continue;
                }

                if (skipMessageIds.contains(message.getId())) {
                    log.info("Message {} in skip list, acknowledging", message.getId());
                    successRecords.add(record);
                    totalSkipped.increment();
                    continue;
                }

                int retryCount = messageRetryCount.getOrDefault(message.getId(), 0);
                if (retryCount >= syncProperties.getRetry().getMaxAttempts()) {
                    log.warn("Message {} exceeded max retries, will be skipped", message.getId());
                    skipMessageIds.add(message.getId());
                    successRecords.add(record);
                    totalSkipped.increment();
                    continue;
                }

                boolean success = processWithRetry(message);
                
                if (success) {
                    successRecords.add(record);
                    totalSuccess.increment();
                    messageRetryCount.remove(message.getId());
                } else {
                    failedRecords.add(record);
                    totalFailed.increment();
                    messageRetryCount.merge(message.getId(), 1, Integer::sum);
                    consecutiveFailures.incrementAndGet();
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

        if (consecutiveFailures.get() >= 10) {
            log.error("Too many consecutive failures ({}), triggering emergency skip", 
                    consecutiveFailures.get());
        }

        if (!failedRecords.isEmpty()) {
            consecutiveFailures.addAndGet(failedRecords.size());
            log.warn("Batch {}-{} had {} failed records", batchStartOffset, batchEndOffset, failedRecords.size());
        } else {
            consecutiveFailures.set(0);
        }

        if (!successRecords.isEmpty()) {
            acknowledgment.acknowledge();
            long maxSuccessOffset = successRecords.stream()
                    .mapToLong(ConsumerRecord::offset)
                    .max()
                    .orElse(batchStartOffset);
            log.info("Acknowledged up to offset {}", maxSuccessOffset);
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

    public void skipMessage(String messageId) {
        skipMessageIds.add(messageId);
        log.info("Added message {} to skip list", messageId);
    }

    public void clearSkipList() {
        skipMessageIds.clear();
        log.info("Cleared skip list");
    }

    public void resetConsecutiveFailures() {
        consecutiveFailures.set(0);
        log.info("Reset consecutive failures counter");
    }

    public int getSkipListSize() {
        return skipMessageIds.size();
    }

    public SyncStatus getStatus() {
        return SyncStatus.builder()
                .totalProcessed(totalProcessed.sum())
                .totalSuccess(totalSuccess.sum())
                .totalFailed(totalFailed.sum())
                .totalSkipped(totalSkipped.sum())
                .totalRetried(totalRetried.sum())
                .consecutiveFailures(consecutiveFailures.get())
                .skipListSize(skipMessageIds.size())
                .lastFlushTime(lastFlushTime.get())
                .build();
    }

    @lombok.Data
    @lombok.Builder
    public static class SyncStatus {
        private long totalProcessed;
        private long totalSuccess;
        private long totalFailed;
        private long totalSkipped;
        private long totalRetried;
        private int consecutiveFailures;
        private int skipListSize;
        private long lastFlushTime;
    }
}
