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
import java.util.concurrent.ConcurrentHashMap;
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

    private final Map<TopicPartition, Long> pendingOffsets = new ConcurrentHashMap<>();

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

        boolean allSuccess = true;
        List<ConsumerRecord<String, String>> failedRecords = new ArrayList<>();

        for (ConsumerRecord<String, String> record : records) {
            boolean success = processSingleRecord(record);
            if (success) {
                totalSuccess.increment();
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                pendingOffsets.put(tp, record.offset() + 1);
            } else {
                totalFailed.increment();
                failedRecords.add(record);
                allSuccess = false;
            }
        }

        totalProcessed.add(records.size());

        if (allSuccess) {
            acknowledgment.acknowledge();
            pendingOffsets.clear();
            log.debug("All records processed successfully, acknowledged");
        } else {
            log.warn("Batch completed with {} failed records, not acknowledging", failedRecords.size());
        }

        lastFlushTime.set(System.currentTimeMillis());
    }

    private boolean processSingleRecord(ConsumerRecord<String, String> record) {
        try {
            SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
            
            if (message.getId() == null || message.getId().isEmpty()) {
                log.warn("Message missing ID, skipping offset: {}", record.offset());
                return true;
            }

            return processWithRetry(message);

        } catch (JsonProcessingException e) {
            log.error("Failed to parse message at offset {}: {}", record.offset(), e.getMessage());
            totalSkipped.increment();
            return true;
        } catch (Exception e) {
            log.error("Failed to process message at offset {}: {}", record.offset(), e.getMessage());
            return false;
        }
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
                .pendingOffsets(pendingOffsets.size())
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
        private int pendingOffsets;
        private long lastFlushTime;
    }
}
