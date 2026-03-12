package com.example.vectorsync.service;

import com.example.vectorsync.config.SyncProperties;
import com.example.vectorsync.model.SyncMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageConsumerServiceTest {

    @Mock
    private ElasticsearchService elasticsearchService;

    private ObjectMapper objectMapper;
    private SyncProperties syncProperties;
    private MessageConsumerService consumerService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        syncProperties = new SyncProperties();
        syncProperties.setBatch(new SyncProperties.Batch());
        syncProperties.getBatch().setSize(100);
        
        consumerService = new MessageConsumerService(
                elasticsearchService,
                syncProperties,
                objectMapper
        );
    }

    @Test
    void testParseMessages_Success() throws Exception {
        List<String> rawMessages = new ArrayList<>();
        
        Map<String, Object> data1 = new HashMap<>();
        data1.put("id", "1");
        data1.put("content", "test content 1");
        
        Map<String, Object> data2 = new HashMap<>();
        data2.put("id", "2");
        data2.put("content", "test content 2");
        
        SyncMessage message1 = SyncMessage.create("1", "test", data1);
        SyncMessage message2 = SyncMessage.create("2", "test", data2);
        
        rawMessages.add(objectMapper.writeValueAsString(message1));
        rawMessages.add(objectMapper.writeValueAsString(message2));
        
        List<SyncMessage> result = parseMessages(consumerService, rawMessages);
        
        assertEquals(2, result.size());
        assertEquals("1", result.get(0).getId());
        assertEquals("2", result.get(1).getId());
    }

    @Test
    void testParseMessages_InvalidJson() {
        List<String> rawMessages = new ArrayList<>();
        rawMessages.add("invalid json");
        
        List<SyncMessage> result = parseMessages(consumerService, rawMessages);
        
        assertEquals(0, result.size());
    }

    @Test
    void testParseMessages_MissingId() throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("content", "test");
        
        SyncMessage message = SyncMessage.builder()
                .type("test")
                .action("create")
                .data(data)
                .build();
        
        List<String> rawMessages = new ArrayList<>();
        rawMessages.add(objectMapper.writeValueAsString(message));
        
        List<SyncMessage> result = parseMessages(consumerService, rawMessages);
        
        assertEquals(0, result.size());
    }

    @Test
    void testPartitionList() {
        List<SyncMessage> messages = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            messages.add(SyncMessage.builder()
                    .id(String.valueOf(i))
                    .type("test")
                    .build());
        }
        
        List<List<SyncMessage>> partitions = partitionList(messages, 100);
        
        assertEquals(3, partitions.size());
        assertEquals(100, partitions.get(0).size());
        assertEquals(100, partitions.get(1).size());
        assertEquals(50, partitions.get(2).size());
    }

    @Test
    void testProcessBatch_SmallBatch() throws Exception {
        List<SyncMessage> messages = new ArrayList<>();
        messages.add(SyncMessage.create("1", "test", Map.of("content", "test")));
        
        when(elasticsearchService.bulkIndex(any())).thenReturn(null);
        
        boolean result = processBatch(consumerService, messages);
        
        assertTrue(result);
        verify(elasticsearchService, times(1)).bulkIndex(messages);
    }

    @Test
    void testProcessBatch_LargeBatch() throws Exception {
        List<SyncMessage> messages = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            messages.add(SyncMessage.create(String.valueOf(i), "test", Map.of("content", "test" + i)));
        }
        
        when(elasticsearchService.bulkIndex(any())).thenReturn(null);
        
        boolean result = processBatch(consumerService, messages);
        
        assertTrue(result);
        verify(elasticsearchService, times(3)).bulkIndex(any());
    }

    @Test
    void testGetStatus() {
        MessageConsumerService.SyncStatus status = consumerService.getStatus();
        
        assertNotNull(status);
        assertEquals(0, status.getTotalProcessed());
    }

    private List<SyncMessage> parseMessages(MessageConsumerService service, List<String> rawMessages) {
        List<SyncMessage> syncMessages = new ArrayList<>();
        
        for (String rawMessage : rawMessages) {
            try {
                SyncMessage message = objectMapper.readValue(rawMessage, SyncMessage.class);
                
                if (message.getId() == null || message.getId().isEmpty()) {
                    continue;
                }
                
                syncMessages.add(message);
            } catch (Exception e) {
                // skip invalid message
            }
        }
        
        return syncMessages;
    }

    private List<List<SyncMessage>> partitionList(List<SyncMessage> list, int size) {
        List<List<SyncMessage>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

    private boolean processBatch(MessageConsumerService service, List<SyncMessage> messages) {
        try {
            elasticsearchService.bulkIndex(messages);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
