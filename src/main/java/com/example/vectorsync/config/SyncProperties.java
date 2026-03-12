package com.example.vectorsync.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "sync")
public class SyncProperties {

    private Kafka kafka = new Kafka();
    private Elasticsearch elasticsearch = new Elasticsearch();
    private Batch batch = new Batch();
    private Retry retry = new Retry();
    private Vector vector = new Vector();

    @Data
    public static class Kafka {
        private String topic = "data-sync-topic";
        private String consumerGroup = "vector-sync-group";
    }

    @Data
    public static class Elasticsearch {
        private String index = "vectors";
        private String indexMapping;
        private int bulkSize = 100;
        private int flushIntervalSeconds = 5;
    }

    @Data
    public static class Batch {
        private int size = 100;
        private long timeoutMs = 3000;
    }

    @Data
    public static class Retry {
        private int maxAttempts = 3;
        private long initialIntervalMs = 1000;
        private double multiplier = 2.0;
        private long maxIntervalMs = 10000;
        private int maxConsecutiveFailures = 10;
    }

    @Data
    public static class Vector {
        private String provider = "local";
        private int dimension = 384;
        private String modelName;
        private String apiKey;
        private String endpoint;
    }
}
