package com.example.vectorsync.benchmark;

import com.example.vectorsync.model.SyncMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class DataPreparationService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${sync.kafka.topic}")
    private String topic;

    public DataPreparationService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public PrepareResult prepareTestData(int count, String dataType) {
        log.info("Starting to prepare {} test records", count);
        long startTime = System.currentTimeMillis();

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        List<String> errors = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    SyncMessage message = generateMessage(index, dataType);
                    String json = objectMapper.writeValueAsString(message);

                    SendResult<String, String> result = kafkaTemplate.send(topic, message.getId(), json).get(30, TimeUnit.SECONDS);

                    if (result != null) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failedCount.incrementAndGet();
                    errors.add("Index " + index + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;

        PrepareResult result = new PrepareResult();
        result.setTotalRequested(count);
        result.setSuccessCount(successCount.get());
        result.setFailedCount(failedCount.get());
        result.setDurationMs(duration);
        result.setTps(count * 1000L / Math.max(duration, 1));
        result.setErrors(errors);
        result.setEndOffset(getTopicEndOffset());

        log.info("Data preparation completed. Success: {}, Failed: {}, Duration: {}ms",
                successCount.get(), failedCount.get(), duration);

        return result;
    }

    private SyncMessage generateMessage(int index, String dataType) {
        Map<String, Object> data = new HashMap<>();
        data.put("id", index);
        data.put("index", index);
        data.put("type", dataType);

        switch (dataType) {
            case "article":
                data.put("title", "文章标题 " + index);
                data.put("content", "这是文章内容，用于生成向量测试。包含足够的文本内容来模拟真实场景。" + index);
                data.put("author", "作者 " + (index % 100));
                data.put("category", "分类 " + (index % 10));
                data.put("tags", Arrays.asList("tag1", "tag2", "tag3"));
                break;
            case "product":
                data.put("name", "产品 " + index);
                data.put("description", "产品描述信息 " + index);
                data.put("price", Math.random() * 1000);
                data.put("category", "类别 " + (index % 20));
                break;
            default:
                data.put("content", "测试数据 " + index);
                data.put("extra", "额外字段 " + index);
        }

        data.put("timestamp", System.currentTimeMillis());
        data.put("uuid", UUID.randomUUID().toString());
        data.put("random", Math.random());

        return SyncMessage.builder()
                .id(String.valueOf(index))
                .type(dataType)
                .action("create")
                .data(data)
                .timestamp(System.currentTimeMillis())
                .version(1)
                .build();
    }

    private long getTopicEndOffset() {
        try {
            return kafkaTemplate.getDefaultTopic();
        } catch (Exception e) {
            return -1;
        }
    }

    public void clearTopic() {
        log.warn("Clearing topic {} - this is a dangerous operation", topic);
    }

    @lombok.Data
    public static class PrepareResult {
        private int totalRequested;
        private int successCount;
        private int failedCount;
        private long durationMs;
        private long tps;
        private List<String> errors;
        private long endOffset;
    }
}
