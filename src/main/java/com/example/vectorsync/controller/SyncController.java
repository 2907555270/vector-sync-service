package com.example.vectorsync.controller;

import com.example.vectorsync.service.ElasticsearchService;
import com.example.vectorsync.service.MessageConsumerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/sync")
@RequiredArgsConstructor
public class SyncController {

    private final MessageConsumerService consumerService;
    private final ElasticsearchService elasticsearchService;

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        MessageConsumerService.SyncStatus syncStatus = consumerService.getStatus();
        
        Map<String, Object> status = new HashMap<>();
        status.put("consumer", syncStatus);
        
        try {
            status.put("esCount", elasticsearchService.count());
            status.put("healthy", true);
        } catch (Exception e) {
            status.put("healthy", false);
            status.put("esError", e.getMessage());
        }
        
        return ResponseEntity.ok(status);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> health = new HashMap<>();
        
        try {
            elasticsearchService.count();
            health.put("status", "UP");
            health.put("kafka", "UP");
            health.put("elasticsearch", "UP");
        } catch (Exception e) {
            health.put("status", "DOWN");
            health.put("error", e.getMessage());
        }
        
        return ResponseEntity.ok(health);
    }

    @PostMapping("/reindex/{id}")
    public ResponseEntity<Map<String, Object>> reindex(@PathVariable String id) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            Map<String, Object> doc = elasticsearchService.getDocument(id);
            if (doc.isEmpty()) {
                result.put("success", false);
                result.put("message", "Document not found");
                return ResponseEntity.notFound().build();
            }
            result.put("success", true);
            result.put("document", doc);
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        
        return ResponseEntity.ok(result);
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<Map<String, Object>> delete(@PathVariable String id) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            elasticsearchService.deleteDocument(id);
            result.put("success", true);
            result.put("message", "Document deleted");
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        
        return ResponseEntity.ok(result);
    }

    @PostMapping("/pause")
    public ResponseEntity<Map<String, Object>> pause() {
        Map<String, Object> result = new HashMap<>();
        
        if (consumerService.isPaused()) {
            result.put("success", false);
            result.put("message", "Consumer is already paused");
            return ResponseEntity.ok(result);
        }
        
        consumerService.pause();
        result.put("success", true);
        result.put("message", "Consumer paused");
        
        return ResponseEntity.ok(result);
    }

    @PostMapping("/resume")
    public ResponseEntity<Map<String, Object>> resume() {
        Map<String, Object> result = new HashMap<>();
        
        if (!consumerService.isPaused()) {
            result.put("success", false);
            result.put("message", "Consumer is not paused");
            return ResponseEntity.ok(result);
        }
        
        consumerService.resume();
        result.put("success", true);
        result.put("message", "Consumer resumed");
        
        return ResponseEntity.ok(result);
    }

    @PostMapping("/clear-retry-count")
    public ResponseEntity<Map<String, Object>> clearRetryCount() {
        Map<String, Object> result = new HashMap<>();
        
        consumerService.clearRetryCountMap();
        result.put("success", true);
        result.put("message", "Retry count map cleared");
        
        return ResponseEntity.ok(result);
    }
}
