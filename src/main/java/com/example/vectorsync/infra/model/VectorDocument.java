package com.example.vectorsync.infra.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VectorDocument {

    @JsonProperty("id")
    private String id;

    @JsonProperty("type")
    private String type;

    @JsonProperty("data")
    private Map<String, Object> data;

    @JsonProperty("content")
    private String content;

    @JsonProperty("vector")
    private List<Float> vector;

    @JsonProperty("sparse_vector")
    private Map<String, Object> sparseVector;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("syncTime")
    private Long syncTime;

    @JsonProperty("version")
    private Integer version;

    public static VectorDocument fromSyncMessage(SyncMessage message) {
        return VectorDocument.builder()
                .id(message.getId())
                .type(message.getType())
                .data(message.getData())
                .timestamp(message.getTimestamp())
                .syncTime(Instant.now().toEpochMilli())
                .version(message.getVersion())
                .build();
    }

    public static VectorDocument fromSyncMessage(SyncMessage message, List<Float> vector, String content) {
        return VectorDocument.builder()
                .id(message.getId())
                .type(message.getType())
                .data(message.getData())
                .content(content)
                .vector(vector)
                .timestamp(message.getTimestamp())
                .syncTime(Instant.now().toEpochMilli())
                .version(message.getVersion())
                .build();
    }
}
