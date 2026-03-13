package com.example.vectorsync.infra.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VectorDocument {

    @JsonProperty("id")
    private String id;

    @JsonProperty("content")
    private String content;

    @JsonProperty("dense_vector")
    private List<Float> denseVector;

    @JsonProperty("sparse_vector")
    private Map<String, Float> sparseVector;

    public static VectorDocument fromSyncMessage(SyncMessage message) {
        return VectorDocument.builder()
                .id(message.getId())
                .content(null)
                .denseVector(null)
                .sparseVector(null)
                .build();
    }

    public static VectorDocument fromSyncMessage(SyncMessage message, List<Float> denseVector, String content) {
        return VectorDocument.builder()
                .id(message.getId())
                .content(content)
                .denseVector(denseVector)
                .sparseVector(null)
                .build();
    }
}
