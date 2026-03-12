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
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageConsumerService {

    private final ElasticsearchService elasticsearchService;
    private final SyncProperties syncProperties;
    private final ObjectMapper objectMapper;

    private final BlockingQueue<List<String>> messageBuffer = new LinkedBlockingQueue<>(100);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    private final LongAdder totalProcessed = new LongAdder();
    private final LongAdder totalSuccess = new LongAdder();
    private final LongAdder totalFailed = new LongAdder();

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

        try {
            List<String> rawMessages = records.stream()
                    .map(ConsumerRecord::value)
                    .collect(Collectors.toList());
            
            List<SyncMessage> syncMessages = parseMessages(rawMessages);
            
            if (syncMessages.isEmpty()) {
                acknowledgment.acknowledge();
                return;
            }

            boolean success = processBatch(syncMessages);

            if (success) {
                totalProcessed.add(records.size());
                totalSuccess.add(records.size());
                acknowledgment.acknowledge();
                log.debug("Batch processed successfully, acknowledged");
            } else {
                totalFailed.add(records.size());
                log.error("Batch processing failed");
            }

        } catch (Exception e) {
            totalFailed.add(records.size());
            log.error("Error processing batch: {}", e.getMessage(), e);
        }

        lastFlushTime.set(System.currentTimeMillis());
    }

    private List<SyncMessage> parseMessages(List<String> rawMessages) {
        List<SyncMessage> syncMessages = new ArrayList<>();
        
        for (String rawMessage : rawMessages) {
            try {
                SyncMessage message = objectMapper.readValue(rawMessage, SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    log.warn("Message missing ID, skipping: {}", rawMessage);
                    continue;
                }
                
                syncMessages.add(message);
            } catch (JsonProcessingException e) {
                log.error("Failed to parse message: {}", e.getMessage());
            }
        }
        
        return syncMessages;
    }

    private boolean processBatch(List<SyncMessage> messages) {
        int batchSize = syncProperties.getBatch().getSize();
        
        if (messages.size() <= batchSize) {
            return processDirect(messages);
        }
        
        List<List<SyncMessage>> batches = partitionList(messages, batchSize);
        boolean allSuccess = true;
        
        for (List<SyncMessage> batch : batches) {
            if (!processDirect(batch)) {
                allSuccess = false;
            }
        }
        
        return allSuccess;
    }

    private boolean processDirect(List<SyncMessage> messages) {
        try {
            elasticsearchService.bulkIndex(messages);
            return true;
        } catch (Exception e) {
            log.error("Failed to bulk index: {}", e.getMessage(), e);
            handleFailure(messages);
            return false;
        }
    }

    private void handleFailure(List<SyncMessage> messages) {
        log.warn("Handling failure for {} messages", messages.size());
        for (SyncMessage message : messages) {
            retryWithBackoff(message, 1);
        }
    }

    private void retryWithBackoff(SyncMessage message, int attempt) {
        int maxRetries = 3;
        long initialDelay = 1000L;
        
        if (attempt > maxRetries) {
            log.error("Max retries exceeded for message id: {}", message.getId());
            return;
        }
        
        try {
            Thread.sleep(initialDelay * attempt);
            elasticsearchService.bulkIndex(List.of(message));
            log.info("Retry successful for message id: {}", message.getId());
        } catch (Exception e) {
            log.warn("Retry {} failed for message id: {}, error: {}", 
                    attempt, message.getId(), e.getMessage());
            retryWithBackoff(message, attempt + 1);
        }
    }

    private List<List<SyncMessage>> partitionList(List<SyncMessage> list, int size) {
        List<List<SyncMessage>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

    public SyncStatus getStatus() {
        return SyncStatus.builder()
                .totalProcessed(totalProcessed.sum())
                .totalSuccess(totalSuccess.sum())
                .totalFailed(totalFailed.sum())
                .lastFlushTime(lastFlushTime.get())
                .build();
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @lombok.Data
    @lombok.Builder
    public static class SyncStatus {
        private long totalProcessed;
        private long totalSuccess;
        private long totalFailed;
        private long lastFlushTime;
    }
}
