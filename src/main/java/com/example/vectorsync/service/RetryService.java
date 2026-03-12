package com.example.vectorsync.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class RetryService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${sync.retry.topic:data-sync-retry}")
    private String retryTopic;

    @Value("${sync.dlq.topic:data-sync-dlq}")
    private String dlqTopic;

    @Value("${sync.retry.max-attempts:3}")
    private int maxRetryAttempts;

    private final Map<String, AtomicInteger> retryCountMap = new ConcurrentHashMap<>();

    public void sendToRetry(String messageKey, String messageValue, int currentRetryCount) {
        try {
            Map<String, Object> retryMessage = objectMapper.readValue(messageValue, Map.class);
            retryMessage.put("retry_count", currentRetryCount + 1);
            retryMessage.put("original_key", messageKey);

            String retryValue = objectMapper.writeValueAsString(retryMessage);

            CompletableFuture<SendResult<String, String>> future = 
                kafkaTemplate.send(retryTopic, messageKey, retryValue);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send to retry topic: key={}, error={}", messageKey, ex.getMessage());
                } else {
                    log.info("Sent to retry topic: key={}, partition={}, offset={}", 
                        messageKey, 
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });

            log.info("Message {} sent to retry queue, attempt {}/{}", 
                messageKey, currentRetryCount + 1, maxRetryAttempts);

        } catch (Exception e) {
            log.error("Failed to prepare retry message: key={}, error={}", messageKey, e.getMessage());
        }
    }

    public void sendToDlq(String messageKey, String messageValue, String errorReason, int retryCount) {
        try {
            Map<String, Object> dlqMessage = objectMapper.readValue(messageValue, Map.class);
            dlqMessage.put("dlq_reason", errorReason);
            dlqMessage.put("total_retries", retryCount);
            dlqMessage.put("original_key", messageKey);
            dlqMessage.put("dlq_timestamp", System.currentTimeMillis());

            String dlqValue = objectMapper.writeValueAsString(dlqMessage);

            CompletableFuture<SendResult<String, String>> future = 
                kafkaTemplate.send(dlqTopic, messageKey, dlqValue);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send to DLQ: key={}, error={}", messageKey, ex.getMessage());
                } else {
                    log.warn("Message {} sent to DLQ after {} retries: {}", 
                        messageKey, retryCount, errorReason);
                }
            });

        } catch (Exception e) {
            log.error("Failed to prepare DLQ message: key={}, error={}", messageKey, e.getMessage());
        }
    }

    public int getRetryCount(String messageKey) {
        AtomicInteger count = retryCountMap.get(messageKey);
        return count != null ? count.get() : 0;
    }

    public void incrementRetryCount(String messageKey) {
        retryCountMap.compute(messageKey, (k, v) -> {
            if (v == null) {
                return new AtomicInteger(1);
            }
            v.incrementAndGet();
            return v;
        });
    }

    public void clearRetryCount(String messageKey) {
        retryCountMap.remove(messageKey);
    }

    public boolean shouldSendToDlq(String messageKey) {
        int count = getRetryCount(messageKey);
        return count >= maxRetryAttempts;
    }

    public String getRetryTopic() {
        return retryTopic;
    }

    public String getDlqTopic() {
        return dlqTopic;
    }
}
