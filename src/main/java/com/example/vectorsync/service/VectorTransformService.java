package com.example.vectorsync.service;

import com.example.vectorsync.model.SyncMessage;
import com.example.vectorsync.model.VectorDocument;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class VectorTransformService {

    private static final String DEFAULT_VECTOR_FIELD = "content";
    private static final int DEFAULT_VECTOR_DIMENSION = 384;

    public VectorDocument transform(SyncMessage message) {
        if (message.getData() == null) {
            log.warn("Message data is null, id: {}", message.getId());
            return VectorDocument.fromSyncMessage(message);
        }

        String content = extractContent(message.getData());
        List<Float> vector = generateVector(message.getData());

        return VectorDocument.fromSyncMessage(message, vector, content);
    }

    private String extractContent(Map<String, Object> data) {
        if (data.containsKey(DEFAULT_VECTOR_FIELD)) {
            return String.valueOf(data.get(DEFAULT_VECTOR_FIELD));
        }
        return data.values().stream()
                .filter(v -> v instanceof String)
                .map(String::valueOf)
                .reduce((a, b) -> a + " " + b)
                .orElse("");
    }

    private List<Float> generateVector(Map<String, Object> data) {
        String content = extractContent(data);
        return embedText(content);
    }

    private List<Float> embedText(String text) {
        if (text == null || text.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<Float> vector = new ArrayList<>(DEFAULT_VECTOR_DIMENSION);
        for (int i = 0; i < DEFAULT_VECTOR_DIMENSION; i++) {
            vector.add(0.0f);
        }
        
        return vector;
    }

    public List<Float> embedTextCustom(String text, int dimension) {
        if (text == null || text.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<Float> vector = new ArrayList<>(dimension);
        for (int i = 0; i < dimension; i++) {
            vector.add(0.0f);
        }
        
        return vector;
    }
}
