package com.example.vectorsync.service;

import com.example.vectorsync.config.SyncProperties;
import com.example.vectorsync.model.SyncMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final SyncProperties syncProperties;
    private final ObjectMapper objectMapper;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Getter
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    
    private final LongAdder totalProcessed = new LongAdder();
    private final LongAdder totalSuccess = new LongAdder();
    private final LongAdder totalFailed = new LongAdder();
    private final LongAdder totalRetried = new LongAdder();
    private final LongAdder totalDlq = new LongAdder();
    
    private final AtomicInteger consecutiveBatchFailures = new AtomicInteger(0);

    private final Map<String, Integer> retryCountMap = new ConcurrentHashMap<>();

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
            log.warn("Consumer is paused, not processing records");
            return;
        }

        if (records == null || records.isEmpty()) {
            log.warn("Received batch of 0 messages, not processing records");
            acknowledgment.acknowledge();
            return;
        }

        log.info("Received batch of {} messages", records.size());

        List<ConsumerRecord<String, String>> processedRecords = new ArrayList<>();
        List<ConsumerRecord<String, String>> successRecords = new ArrayList<>();
        List<ConsumerRecord<String, String>> failedRecords = new ArrayList<>();

        for (ConsumerRecord<String, String> record : records) {
            if (paused.get()) {
                log.warn("Consumer paused during batch processing at offset {}, stopping", record.offset());
                break;
            }

            processedRecords.add(record);

            try {
                SyncMessage message = objectMapper.readValue(record.value(), SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    successRecords.add(record);
                    continue;
                }

                boolean success = processWithRetry(message, record);
                
                if (success) {
                    successRecords.add(record);
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
        double successRate = processedCount > 0 ? (double) successRecords.size() / records.size() : 0;
        double threshold = syncProperties.getRetry().getSuccessThreshold();

        boolean shouldAck = !successRecords.isEmpty() && successRate >= threshold;
        
        if (shouldAck) {
            acknowledgment.acknowledge();
            
            if (!failedRecords.isEmpty()) {
                log.warn("Batch acknowledged with failures. Success: {}, Failed: {}, Rate: {}", 
                    successRecords.size(), failedRecords.size(), successRate);
                sendFailedToQueue(failedRecords);
            } else {
                consecutiveBatchFailures.set(0);
                log.info("Batch all success, acknowledged {} records", processedCount);
            }
        } else if (processedRecords.isEmpty()) {
            log.warn("Empty batch, not acknowledging");
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

    private String getMessageKey(ConsumerRecord<String, String> record) {
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }

    private void sendFailedToQueue(List<ConsumerRecord<String, String>> failedRecords) {
        for (ConsumerRecord<String, String> record : failedRecords) {
            try {
                String messageKey = getMessageKey(record);
                int currentRetryCount = retryCountMap.getOrDefault(messageKey, 0);
                
                if (currentRetryCount >= syncProperties.getRetry().getMaxAttempts()) {
                    log.warn("Message {} exceeded max retries ({}), discarding", 
                        messageKey, syncProperties.getRetry().getMaxAttempts());
                    totalDlq.increment();
                    retryCountMap.remove(messageKey);
                } else {
                    retryCountMap.put(messageKey, currentRetryCount + 1);
                    
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                        record.topic(),
                        record.partition(),
                        record.timestamp(),
                        record.key(),
                        record.value()
                    );
                    
                    kafkaTemplate.send(producerRecord);
                    totalRetried.increment();
                    log.info("Message {} sent back to queue, retry count: {}/{}", 
                        messageKey, currentRetryCount + 1, syncProperties.getRetry().getMaxAttempts());
                }
            } catch (Exception e) {
                log.error("Failed to send message back to queue: {}", e.getMessage());
            }
        }
    }

    private boolean processWithRetry(SyncMessage message, ConsumerRecord<String, String> record) {
        int maxRetries = syncProperties.getRetry().getMaxAttempts();
        long initialDelay = syncProperties.getRetry().getInitialIntervalMs();
        double multiplier = syncProperties.getRetry().getMultiplier();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            if (paused.get()) {
                return false;
            }

            try {
                elasticsearchService.bulkIndex(List.of(message));
                
                String messageKey = getMessageKey(record);
                retryCountMap.remove(messageKey);
                
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
                    paused.set(true);
                    log.error("Consumer PAUSED due to consecutive batch failures");
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
                    consecutiveBatchFailures.set(0);
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

    public void clearRetryCountMap() {
        retryCountMap.clear();
        log.info("Retry count map cleared");
    }

    public int getRetryCountMapSize() {
        return retryCountMap.size();
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
                .successThreshold(syncProperties.getRetry().getSuccessThreshold())
                .retryCountMapSize(retryCountMap.size())
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
        private double successThreshold;
        private int retryCountMapSize;
        private long lastFlushTime;
    }
}
