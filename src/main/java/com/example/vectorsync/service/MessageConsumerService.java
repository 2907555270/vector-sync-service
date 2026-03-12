package com.example.vectorsync.service;

import com.example.vectorsync.config.SyncProperties;
import com.example.vectorsync.model.SyncMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.*;
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
    private final LongAdder totalRetried = new LongAdder();
    private final LongAdder totalSkipped = new LongAdder();

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

        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        long batchStartOffset = records.get(0).offset();
        long batchEndOffset = records.get(records.size() - 1).offset();

        Map<String, SyncMessage> successMessages = new LinkedHashMap<>();
        List<ConsumerRecord<String, String>> failedRecords = new ArrayList<>();

        for (ConsumerRecord<String, String> record : records) {
            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    log.warn("Message missing ID, skipping offset: {}", record.offset());
                    successMessages.put(String.valueOf(record.offset()), message);
                    totalSkipped.increment();
                    continue;
                }

                boolean success = processWithRetry(message);
                
                if (success) {
                    successMessages.put(message.getId(), message);
                    totalSuccess.increment();
                } else {
                    failedRecords.add(record);
                    totalFailed.increment();
                }

            } catch (JsonProcessingException e) {
                log.error("Failed to parse message at offset {}: {}", record.offset(), e.getMessage());
                successMessages.put(String.valueOf(record.offset()), null);
                totalSkipped.increment();
            } catch (Exception e) {
                log.error("Failed to process message at offset {}: {}", record.offset(), e.getMessage());
                failedRecords.add(record);
                totalFailed.increment();
            }
        }

        totalProcessed.add(records.size());

        if (failedRecords.isEmpty()) {
            acknowledgment.acknowledge();
            log.info("Batch {}-{} all success, acknowledged", batchStartOffset, batchEndOffset);
        } else {
            log.warn("Batch {}-{} had {} failed records, NOT acknowledging. " +
                    "Failed offsets: {}, will be retried",
                    batchStartOffset, batchEndOffset, failedRecords.size(),
                    failedRecords.stream().map(r -> r.offset()).toList());
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

    public SyncStatus getStatus() {
        return SyncStatus.builder()
                .totalProcessed(totalProcessed.sum())
                .totalSuccess(totalSuccess.sum())
                .totalFailed(totalFailed.sum())
                .totalRetried(totalRetried.sum())
                .totalSkipped(totalSkipped.sum())
                .lastFlushTime(lastFlushTime.get())
                .build();
    }

    @lombok.Data
    @lombok.Builder
    public static class SyncStatus {
        private long totalProcessed;
        private long totalSuccess;
        private long totalFailed;
        private long totalRetried;
        private long totalSkipped;
        private long lastFlushTime;
    }
}
