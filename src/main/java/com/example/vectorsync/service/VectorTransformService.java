package com.example.vectorsync.service;

import com.example.vectorsync.model.SyncMessage;
import com.example.vectorsync.model.VectorDocument;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class VectorTransformService {

    private static final String DEFAULT_VECTOR_FIELD = "content";
    private static final int DEFAULT_VECTOR_DIMENSION = 384;

    @Autowired(required = false)
    private VectorApiService vectorApiService;

    public VectorDocument transform(SyncMessage message) {
        if (message.getData() == null) {
            log.warn("Message data is null, id: {}", message.getId());
            return VectorDocument.fromSyncMessage(message);
        }

        String content = extractContent(message.getData());
        List<Float> vector = generateVector(message.getData());

        Map<String, Object> sparseVector = null;
        if (vectorApiService != null && vectorApiService.isEnabled() && content != null && !content.isEmpty()) {
            try {
                sparseVector = generateSparseVector(content);
            } catch (Exception e) {
                log.warn("Failed to generate sparse vector: {}", e.getMessage());
            }
        }

        VectorDocument doc = VectorDocument.fromSyncMessage(message, vector, content);
        doc.setSparseVector(sparseVector);
        
        return doc;
    }

    @SuppressWarnings("unchecked")
    private String extractContent(Map<String, Object> data) {
        if (data.containsKey(DEFAULT_VECTOR_FIELD)) {
            return String.valueOf(data.get(DEFAULT_VECTOR_FIELD));
        }

        if ("table_metadata".equals(data.get("type")) || data.containsKey("tableName")) {
            return extractTableMetadataContent(data);
        }

        return data.values().stream()
                .filter(v -> v instanceof String)
                .map(String::valueOf)
                .reduce((a, b) -> a + " " + b)
                .orElse("");
    }

    @SuppressWarnings("unchecked")
    private String extractTableMetadataContent(Map<String, Object> data) {
        StringBuilder content = new StringBuilder();

        String database = (String) data.getOrDefault("database", "");
        String tableName = (String) data.getOrDefault("tableName", "");
        
        if (!database.isEmpty()) content.append(database).append(" ");
        if (!tableName.isEmpty()) content.append(tableName).append(" ");

        List<String> columns = (List<String>) data.get("columns");
        if (columns != null) {
            content.append(String.join(" ", columns)).append(" ");
        }

        List<String> primaryKeys = (List<String>) data.get("primaryKeys");
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            content.append("primary key ").append(String.join(" ", primaryKeys)).append(" ");
        }

        List<String> foreignKeys = (List<String>) data.get("foreignKeys");
        if (foreignKeys != null && !foreignKeys.isEmpty()) {
            content.append("foreign key ").append(String.join(" ", foreignKeys)).append(" ");
        }

        String complexity = (String) data.get("complexity");
        if (complexity != null) {
            content.append(complexity).append(" ");
        }

        Map<String, Object> features = (Map<String, Object>) data.get("features");
        if (features != null) {
            features.forEach((key, value) -> {
                if (Boolean.TRUE.equals(value)) {
                    content.append(key.replace("has", "").toLowerCase()).append(" ");
                }
            });
        }

        return content.toString().trim();
    }

    private List<Float> generateVector(Map<String, Object> data) {
        String content = extractContent(data);
        return embedText(content);
    }

    private List<Float> embedText(String text) {
        if (text == null || text.isEmpty()) {
            return new ArrayList<>();
        }

        if (vectorApiService != null && vectorApiService.isEnabled()) {
            try {
                VectorApiService.DenseVectorResult result = vectorApiService.generateDenseVector(text);
                if (result.getDenseVector() != null && !result.getDenseVector().isEmpty()) {
                    return result.getDenseVector();
                }
            } catch (Exception e) {
                log.warn("Failed to call dense vector API: {}", e.getMessage());
            }
        }

        log.debug("Using placeholder dense vector (dimension: {})", DEFAULT_VECTOR_DIMENSION);
        List<Float> vector = new ArrayList<>(DEFAULT_VECTOR_DIMENSION);
        for (int i = 0; i < DEFAULT_VECTOR_DIMENSION; i++) {
            vector.add(0.0f);
        }

        return vector;
    }

    private Map<String, Object> generateSparseVector(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }

        try {
            VectorApiService.SparseVectorResult result = vectorApiService.generateSparseVector(text, false);
            Map<String, Object> sparseMap = new HashMap<>();
            
            if (result.getSparseIdVector() != null) {
                sparseMap.put("sparse_id_vector", result.getSparseIdVector());
            }
            if (result.getSparseWordVector() != null) {
                sparseMap.put("sparse_word_vector", result.getSparseWordVector());
            }
            
            return sparseMap.isEmpty() ? null : sparseMap;
        } catch (Exception e) {
            log.warn("Failed to call sparse vector API: {}", e.getMessage());
            return null;
        }
    }

    public List<Float> embedTextCustom(String text, int dimension) {
        if (text == null || text.isEmpty()) {
            return new ArrayList<>();
        }

        if (vectorApiService != null && vectorApiService.isEnabled()) {
            try {
                VectorApiService.DenseVectorResult result = vectorApiService.generateDenseVector(text);
                if (result.getDenseVector() != null && !result.getDenseVector().isEmpty()) {
                    return result.getDenseVector();
                }
            } catch (Exception e) {
                log.warn("Failed to call dense vector API: {}", e.getMessage());
            }
        }

        List<Float> vector = new ArrayList<>(dimension);
        for (int i = 0; i < dimension; i++) {
            vector.add(0.0f);
        }

        return vector;
    }

    public String translateToEnglish(String text) {
        if (vectorApiService != null && vectorApiService.isEnabled()) {
            try {
                VectorApiService.TranslateResult result = vectorApiService.translate(text);
                if (result.getTranslatedText() != null) {
                    return String.valueOf(result.getTranslatedText());
                }
            } catch (Exception e) {
                log.warn("Failed to translate: {}", e.getMessage());
            }
        }
        return text;
    }

    public double calculateSimilarity(String query, String document) {
        if (vectorApiService != null && vectorApiService.isEnabled()) {
            try {
                return vectorApiService.calculateSimilarity(query, document);
            } catch (Exception e) {
                log.warn("Failed to calculate similarity: {}", e.getMessage());
            }
        }
        return 0.0;
    }
}
