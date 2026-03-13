package com.example.vectorsync.infra.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncMessage {

    @JsonProperty("message_key")
    private String messageKey;

    @JsonProperty("id")
    private String id;

    @JsonProperty("type")
    private String type;

    @JsonProperty("action")
    private String action;

    @JsonProperty("data")
    private Map<String, Object> data;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("version")
    private Integer version;

    public enum ActionType {
        CREATE("create"),
        UPDATE("update"),
        DELETE("delete");

        private final String value;

        ActionType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static SyncMessage create(String id, String type, Map<String, Object> data) {
        return SyncMessage.builder()
                .messageKey(java.util.UUID.randomUUID().toString())
                .id(id)
                .type(type)
                .action(ActionType.CREATE.getValue())
                .data(data)
                .timestamp(Instant.now().toEpochMilli())
                .version(1)
                .build();
    }

    public static SyncMessage delete(String id, String type) {
        return SyncMessage.builder()
                .messageKey(java.util.UUID.randomUUID().toString())
                .id(id)
                .type(type)
                .action(ActionType.DELETE.getValue())
                .timestamp(Instant.now().toEpochMilli())
                .version(1)
                .build();
    }

    public boolean isDelete() {
        return ActionType.DELETE.getValue().equals(this.action);
    }

    public boolean isCreateOrUpdate() {
        return ActionType.CREATE.getValue().equals(this.action) ||
               ActionType.UPDATE.getValue().equals(this.action);
    }
}
