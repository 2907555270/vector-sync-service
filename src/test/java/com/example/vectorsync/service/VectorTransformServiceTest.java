package com.example.vectorsync.service;

import com.example.vectorsync.model.SyncMessage;
import com.example.vectorsync.model.VectorDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VectorTransformServiceTest {

    private VectorTransformService transformService;

    @BeforeEach
    void setUp() {
        transformService = new VectorTransformService();
    }

    @Test
    void testTransform_WithContentField() {
        Map<String, Object> data = new HashMap<>();
        data.put("id", "1");
        data.put("content", "This is test content for vector embedding");
        
        SyncMessage message = SyncMessage.create("1", "document", data);
        
        VectorDocument result = transformService.transform(message);
        
        assertNotNull(result);
        assertEquals("1", result.getId());
        assertEquals("document", result.getType());
        assertEquals("This is test content for vector embedding", result.getContent());
        assertNotNull(result.getVector());
        assertEquals(384, result.getVector().size());
    }

    @Test
    void testTransform_WithoutContentField() {
        Map<String, Object> data = new HashMap<>();
        data.put("id", "2");
        data.put("title", "Test Title");
        data.put("description", "Test Description");
        data.put("price", 100);
        
        SyncMessage message = SyncMessage.create("2", "product", data);
        
        VectorDocument result = transformService.transform(message);
        
        assertNotNull(result);
        assertEquals("2", result.getId());
        assertEquals("product", result.getType());
        assertNotNull(result.getContent());
        assertTrue(result.getContent().contains("Test Title"));
        assertTrue(result.getContent().contains("Test Description"));
    }

    @Test
    void testTransform_WithNullData() {
        SyncMessage message = SyncMessage.builder()
                .id("3")
                .type("test")
                .data(null)
                .build();
        
        VectorDocument result = transformService.transform(message);
        
        assertNotNull(result);
        assertEquals("3", result.getId());
    }

    @Test
    void testTransform_DeleteMessage() {
        SyncMessage message = SyncMessage.delete("4", "document");
        
        VectorDocument result = transformService.transform(message);
        
        assertNotNull(result);
        assertEquals("4", result.getId());
    }

    @Test
    void testEmbedTextCustom() {
        String text = "Custom dimension test";
        List<Float> vector = transformService.embedTextCustom(text, 512);
        
        assertNotNull(vector);
        assertEquals(512, vector.size());
    }

    @Test
    void testEmbedTextCustom_NullText() {
        List<Float> vector = transformService.embedTextCustom(null, 128);
        
        assertNotNull(vector);
        assertTrue(vector.isEmpty());
    }

    @Test
    void testEmbedTextCustom_EmptyText() {
        List<Float> vector = transformService.embedTextCustom("", 128);
        
        assertNotNull(vector);
        assertTrue(vector.isEmpty());
    }
}
