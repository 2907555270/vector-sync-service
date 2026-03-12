package com.example.vectorsync.benchmark;

import com.example.vectorsync.model.SyncMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.Reader;
import java.util.*;

public class MetadataImporter {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "192.168.116.5:9092";
    private static final String DEFAULT_TOPIC = "data-sync-topic";
    private static final int DEFAULT_BATCH_SIZE = 100;

    public static void main(String[] args) throws Exception {
        String filePath = args.length > 0 ? args[0] : "metadatas.json";
        String bootstrapServers = args.length > 1 ? args[1] : DEFAULT_BOOTSTRAP_SERVERS;
        String topic = args.length > 2 ? args[2] : DEFAULT_TOPIC;
        int batchSize = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_BATCH_SIZE;

        System.out.println("=== Metadata Importer ===");
        System.out.println("File: " + filePath);
        System.out.println("Kafka: " + bootstrapServers);
        System.out.println("Topic: " + topic);
        System.out.println("Batch Size: " + batchSize);
        System.out.println();

        ObjectMapper objectMapper = new ObjectMapper();
        List<String> rawMetadata;

        try (Reader reader = new FileReader(filePath)) {
            rawMetadata = objectMapper.readValue(reader, 
                objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
        }

        System.out.println("Loaded " + rawMetadata.size() + " table metadata entries");

        List<TableMetadata> tables = parseMetadata(rawMetadata);
        System.out.println("Parsed " + tables.size() + " tables");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<ProducerRecord<String, String>> batch = new ArrayList<>();
            int totalSent = 0;
            long startTime = System.currentTimeMillis();

            for (int i = 0; i < tables.size(); i++) {
                TableMetadata table = tables.get(i);
                SyncMessage message = toSyncMessage(table, i);
                String json = objectMapper.writeValueAsString(message);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.getId(), json);
                batch.add(record);

                if (batch.size() >= batchSize) {
                    sendBatch(producer, batch);
                    totalSent += batch.size();
                    System.out.println("Sent " + totalSent + "/" + tables.size() + " messages");
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                sendBatch(producer, batch);
                totalSent += batch.size();
            }

            producer.flush();

            long duration = System.currentTimeMillis() - startTime;
            System.out.println();
            System.out.println("=== Import Complete ===");
            System.out.println("Total sent: " + totalSent);
            System.out.println("Duration: " + duration + "ms");
            System.out.println("TPS: " + (totalSent * 1000L / Math.max(duration, 1)));
        }
    }

    private static void sendBatch(KafkaProducer<String, String> producer, List<ProducerRecord<String, String>> batch) {
        for (ProducerRecord<String, String> record : batch) {
            producer.send(record);
        }
    }

    private static List<TableMetadata> parseMetadata(List<String> rawMetadata) {
        List<TableMetadata> tables = new ArrayList<>();

        for (String entry : rawMetadata) {
            String[] lines = entry.split("\n");
            if (lines.length < 2) continue;

            TableMetadata metadata = new TableMetadata();
            metadata.setDatabase(lines[0].trim());
            metadata.setTableName(lines[1].trim());

            List<String> columns = new ArrayList<>();
            for (int i = 2; i < lines.length; i++) {
                String col = lines[i].trim();
                if (!col.isEmpty()) {
                    columns.add(col);
                }
            }
            metadata.setColumns(columns);
            metadata.setColumnCount(columns.size());
            metadata.setPrimaryKeys(extractPrimaryKeys(columns));
            metadata.setForeignKeys(extractForeignKeys(columns));
            metadata.setComplexity(calculateComplexity(metadata));
            metadata.setFeatures(extractFeatures(metadata));

            tables.add(metadata);
        }

        return tables;
    }

    private static List<String> extractPrimaryKeys(List<String> columns) {
        List<String> pks = new ArrayList<>();
        for (String col : columns) {
            String lower = col.toLowerCase();
            if (lower.endsWith("_id") || lower.equals("id")) {
                pks.add(col);
            }
        }
        return pks;
    }

    private static List<String> extractForeignKeys(List<String> columns) {
        List<String> fks = new ArrayList<>();
        for (String col : columns) {
            String lower = col.toLowerCase();
            if (lower.contains("_id") && !lower.endsWith("_id")) {
                fks.add(col);
            }
        }
        return fks;
    }

    private static String calculateComplexity(TableMetadata metadata) {
        int colCount = metadata.getColumnCount();
        int pkCount = metadata.getPrimaryKeys().size();

        if (colCount <= 5 && pkCount >= 1) return "SIMPLE";
        if (colCount <= 10 && pkCount >= 1) return "MEDIUM";
        return "COMPLEX";
    }

    private static Map<String, Object> extractFeatures(TableMetadata metadata) {
        Map<String, Object> features = new HashMap<>();

        boolean hasDate = false, hasNumeric = false, hasText = false, hasLocation = false;
        List<String> types = new ArrayList<>();

        for (String col : metadata.getColumns()) {
            String lower = col.toLowerCase();
            if (lower.contains("date") || lower.contains("year") || lower.contains("time")) {
                hasDate = true;
                types.add("DATE");
            } else if (lower.contains("id") || lower.contains("count") || lower.contains("num") 
                    || lower.contains("amount") || lower.contains("price") || lower.contains("salary")) {
                hasNumeric = true;
                types.add("NUMBER");
            } else if (lower.contains("name") || lower.contains("title") || lower.contains("desc")) {
                hasText = true;
                types.add("TEXT");
            } else if (lower.contains("country") || lower.contains("city") || lower.contains("location")) {
                hasLocation = true;
                types.add("LOCATION");
            } else {
                types.add("OTHER");
            }
        }

        features.put("hasDateColumn", hasDate);
        features.put("hasNumericColumn", hasNumeric);
        features.put("hasTextColumn", hasText);
        features.put("hasLocationColumn", hasLocation);
        features.put("hasRelationship", !metadata.getForeignKeys().isEmpty());
        features.put("columnTypes", types);

        return features;
    }

    private static SyncMessage toSyncMessage(TableMetadata metadata, int index) {
        Map<String, Object> data = new HashMap<>();
        data.put("database", metadata.getDatabase());
        data.put("tableName", metadata.getTableName());
        data.put("columns", metadata.getColumns());
        data.put("columnCount", metadata.getColumnCount());
        data.put("primaryKeys", metadata.getPrimaryKeys());
        data.put("foreignKeys", metadata.getForeignKeys());
        data.put("complexity", metadata.getComplexity());
        data.put("features", metadata.getFeatures());
        data.put("index", index);

        return SyncMessage.builder()
                .id(String.valueOf(index))
                .type("table_metadata")
                .action("create")
                .data(data)
                .timestamp(System.currentTimeMillis())
                .version(1)
                .build();
    }

    static class TableMetadata {
        private String database;
        private String tableName;
        private List<String> columns;
        private int columnCount;
        private List<String> primaryKeys;
        private List<String> foreignKeys;
        private String complexity;
        private Map<String, Object> features;

        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }
        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }
        public List<String> getColumns() { return columns; }
        public void setColumns(List<String> columns) { this.columns = columns; }
        public int getColumnCount() { return columnCount; }
        public void setColumnCount(int columnCount) { this.columnCount = columnCount; }
        public List<String> getPrimaryKeys() { return primaryKeys; }
        public void setPrimaryKeys(List<String> primaryKeys) { this.primaryKeys = primaryKeys; }
        public List<String> getForeignKeys() { return foreignKeys; }
        public void setForeignKeys(List<String> foreignKeys) { this.foreignKeys = foreignKeys; }
        public String getComplexity() { return complexity; }
        public void setComplexity(String complexity) { this.complexity = complexity; }
        public Map<String, Object> getFeatures() { return features; }
        public void setFeatures(Map<String, Object> features) { this.features = features; }
    }
}
