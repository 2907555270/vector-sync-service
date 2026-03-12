package com.example.vectorsync.benchmark;

import com.example.vectorsync.model.SyncMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TableMetadataService {

    private final ObjectMapper objectMapper;
    private List<TableMetadata> allTables;

    public TableMetadataService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        loadMetadata();
    }

    public void loadMetadata() {
        try {
            Resource resource = new ClassPathResource("metadatas.json");
            if (!resource.exists()) {
                log.warn("metadatas.json not found in classpath, trying file system");
                loadFromFileSystem();
                return;
            }

            String content = new String(resource.getInputStream().readAllBytes());
            List<String> rawMetadata = objectMapper.readValue(content,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));

            allTables = parseMetadata(rawMetadata);
            log.info("Loaded {} table metadata entries", allTables.size());

        } catch (IOException e) {
            log.error("Failed to load metadatas.json", e);
            allTables = new ArrayList<>();
        }
    }

    private void loadFromFileSystem() {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get("metadatas.json");
            if (java.nio.file.Files.exists(path)) {
                String content = java.nio.file.Files.readString(path);
                List<String> rawMetadata = objectMapper.readValue(content,
                        objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));

                allTables = parseMetadata(rawMetadata);
                log.info("Loaded {} table metadata entries from file system", allTables.size());
            }
        } catch (Exception e) {
            log.error("Failed to load metadatas.json from file system", e);
            allTables = new ArrayList<>();
        }
    }

    private List<TableMetadata> parseMetadata(List<String> rawMetadata) {
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

    private List<String> extractPrimaryKeys(List<String> columns) {
        return columns.stream()
                .filter(c -> c.toLowerCase().endsWith("_id") || c.equalsIgnoreCase("id"))
                .collect(Collectors.toList());
    }

    private List<String> extractForeignKeys(List<String> columns) {
        return columns.stream()
                .filter(c -> c.toLowerCase().contains("_id"))
                .filter(c -> !c.toLowerCase().endsWith("_id") || c.toLowerCase().equals("id"))
                .collect(Collectors.toList());
    }

    private String calculateComplexity(TableMetadata metadata) {
        int colCount = metadata.getColumnCount();
        int pkCount = metadata.getPrimaryKeys().size();
        int fkCount = metadata.getForeignKeys().size();

        if (colCount <= 5 && pkCount >= 1) return "SIMPLE";
        if (colCount <= 10 && (pkCount >= 1 || fkCount >= 1)) return "MEDIUM";
        return "COMPLEX";
    }

    private Map<String, Object> extractFeatures(TableMetadata metadata) {
        Map<String, Object> features = new HashMap<>();

        features.put("hasDateColumn", hasColumnType(metadata.getColumns(), "date"));
        features.put("hasNumericColumn", hasColumnType(metadata.getColumns(), "number"));
        features.put("hasTextColumn", hasColumnType(metadata.getColumns(), "name") || hasColumnType(metadata.getColumns(), "title"));
        features.put("hasLocationColumn", hasColumnType(metadata.getColumns(), "location") || hasColumnType(metadata.getColumns(), "country"));
        features.put("hasRelationship", metadata.getForeignKeys().size() > 0);
        features.put("columnTypes", analyzeColumnTypes(metadata.getColumns()));

        return features;
    }

    private boolean hasColumnType(List<String> columns, String type) {
        return columns.stream().anyMatch(c -> c.toLowerCase().contains(type.toLowerCase()));
    }

    private List<String> analyzeColumnTypes(List<String> columns) {
        List<String> types = new ArrayList<>();
        for (String col : columns) {
            String lower = col.toLowerCase();
            if (lower.contains("date") || lower.contains("year") || lower.contains("time")) {
                types.add("DATE");
            } else if (lower.contains("id") || lower.contains("count") || lower.contains("num") || lower.contains("amount") || lower.contains("price") || lower.contains("salary") || lower.contains("budget")) {
                types.add("NUMBER");
            } else if (lower.contains("name") || lower.contains("title") || lower.contains("desc")) {
                types.add("TEXT");
            } else if (lower.contains("country") || lower.contains("city") || lower.contains("location") || lower.contains("building") || lower.contains("address")) {
                types.add("LOCATION");
            } else if (lower.contains("grade") || lower.contains("result") || lower.contains("rate")) {
                types.add("MEASURE");
            } else {
                types.add("OTHER");
            }
        }
        return types;
    }

    public List<TableMetadata> getAllTables() {
        if (allTables == null) {
            loadMetadata();
        }
        return allTables;
    }

    public TableMetadata getTable(int index) {
        if (allTables == null) {
            loadMetadata();
        }
        if (index >= 0 && index < allTables.size()) {
            return allTables.get(index);
        }
        return null;
    }

    public int getTableCount() {
        return allTables != null ? allTables.size() : 0;
    }

    public SyncMessage toSyncMessage(TableMetadata metadata, int index) {
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

    @Data
    public static class TableMetadata {
        private String database;
        private String tableName;
        private List<String> columns;
        private int columnCount;
        private List<String> primaryKeys;
        private List<String> foreignKeys;
        private String complexity;
        private Map<String, Object> features;
    }
}
