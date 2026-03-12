package com.example.vectorsync.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.*;

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

        } catch (Exception e) {
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

            tables.add(metadata);
        }

        return tables;
    }

    @lombok.Data
    public static class TableMetadata {
        private String database;
        private String tableName;
        private List<String> columns;
    }
}
