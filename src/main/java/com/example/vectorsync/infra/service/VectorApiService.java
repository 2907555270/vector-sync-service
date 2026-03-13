package com.example.vectorsync.infra.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class VectorApiService {

    @Value("${vector.api.base-url:http://localhost:8080}")
    private String baseUrl;

    @Value("${vector.api.timeout:30000}")
    private int timeout;

    @Value("${vector.api.enabled:true}")
    private boolean enabled;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public VectorApiService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.restTemplate = new RestTemplate();
    }

    public SparseVectorResult generateSparseVector(String text, boolean queryMode) {
        if (!enabled) {
            log.warn("Vector API is disabled, returning empty sparse vector");
            return createEmptySparseVector();
        }

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("text", text);
            requestBody.put("query_mode", queryMode);

            HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

            ResponseEntity<String> response = restTemplate.exchange(
                    baseUrl + "/api/v1/sparse_vector",
                    HttpMethod.POST,
                    request,
                    String.class
            );

            if (response.getStatusCode() == HttpStatus.OK) {
                JsonNode json = objectMapper.readTree(response.getBody());
                SparseVectorResult result = new SparseVectorResult();
                result.setSparseIdVector(parseFloatMap(json.get("sparse_id_vector")));
                result.setSparseWordVector(parseStringFloatMap(json.get("sparse_word_vector")));
                return result;
            }

            log.error("Failed to generate sparse vector, status: {}", response.getStatusCode());
            return createEmptySparseVector();

        } catch (Exception e) {
            log.error("Error generating sparse vector: {}", e.getMessage(), e);
            return createEmptySparseVector();
        }
    }

    public DenseVectorResult generateDenseVector(String text) {
        if (!enabled) {
            log.warn("Vector API is disabled, returning empty dense vector");
            return createEmptyDenseVector();
        }

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("text", text);

            HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

            ResponseEntity<String> response = restTemplate.exchange(
                    baseUrl + "/api/v1/dense_vector",
                    HttpMethod.POST,
                    request,
                    String.class
            );

            if (response.getStatusCode() == HttpStatus.OK) {
                JsonNode json = objectMapper.readTree(response.getBody());
                DenseVectorResult result = new DenseVectorResult();
                result.setDenseVector(parseFloatList(json.get("dense_vector")));
                return result;
            }

            log.error("Failed to generate dense vector, status: {}", response.getStatusCode());
            return createEmptyDenseVector();

        } catch (Exception e) {
            log.error("Error generating dense vector: {}", e.getMessage(), e);
            return createEmptyDenseVector();
        }
    }

    public TranslateResult translate(String text) {
        return translate(List.of(text));
    }

    public TranslateResult translate(List<String> texts) {
        if (!enabled) {
            log.warn("Vector API is disabled, returning original text");
            TranslateResult result = new TranslateResult();
            result.setTranslatedText(texts.size() == 1 ? texts.get(0) : texts);
            return result;
        }

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, Object> requestBody = new HashMap<>();
            if (texts.size() == 1) {
                requestBody.put("text", texts.get(0));
            } else {
                requestBody.put("text", texts);
            }

            HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

            ResponseEntity<String> response = restTemplate.exchange(
                    baseUrl + "/api/v1/translate",
                    HttpMethod.POST,
                    request,
                    String.class
            );

            if (response.getStatusCode() == HttpStatus.OK) {
                JsonNode json = objectMapper.readTree(response.getBody());
                TranslateResult result = new TranslateResult();
                JsonNode translated = json.get("translated_text");
                if (translated.isArray()) {
                    result.setTranslatedText(parseStringList(translated));
                } else {
                    result.setTranslatedText(translated.asText());
                }
                return result;
            }

            log.error("Failed to translate, status: {}", response.getStatusCode());
            return createEmptyTranslateResult(texts);

        } catch (Exception e) {
            log.error("Error translating: {}", e.getMessage(), e);
            return createEmptyTranslateResult(texts);
        }
    }

    public Double calculateSimilarity(String query, String document) {
        if (!enabled) {
            log.warn("Vector API is disabled, returning 0.0 similarity");
            return 0.0;
        }

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("query", query);
            requestBody.put("document", document);

            HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

            ResponseEntity<String> response = restTemplate.exchange(
                    baseUrl + "/api/v1/similarity",
                    HttpMethod.POST,
                    request,
                    String.class
            );

            if (response.getStatusCode() == HttpStatus.OK) {
                JsonNode json = objectMapper.readTree(response.getBody());
                return json.get("similarity").asDouble();
            }

            log.error("Failed to calculate similarity, status: {}", response.getStatusCode());
            return 0.0;

        } catch (Exception e) {
            log.error("Error calculating similarity: {}", e.getMessage(), e);
            return 0.0;
        }
    }

    private Map<String, Float> parseFloatMap(JsonNode node) {
        Map<String, Float> map = new HashMap<>();
        if (node != null && node.isObject()) {
            node.fields().forEachRemaining(entry -> 
                map.put(entry.getKey(), (float) entry.getValue().asDouble())
            );
        }
        return map;
    }

    private Map<String, Float> parseStringFloatMap(JsonNode node) {
        Map<String, Float> map = new HashMap<>();
        if (node != null && node.isObject()) {
            node.fields().forEachRemaining(entry -> 
                map.put(entry.getKey(), (float) entry.getValue().asDouble())
            );
        }
        return map;
    }

    private List<Float> parseFloatList(JsonNode node) {
        List<Float> list = new ArrayList<>();
        if (node != null && node.isArray()) {
            node.forEach(n -> list.add((float) n.asDouble()));
        }
        return list;
    }

    private List<String> parseStringList(JsonNode node) {
        List<String> list = new ArrayList<>();
        if (node != null && node.isArray()) {
            node.forEach(n -> list.add(n.asText()));
        }
        return list;
    }

    private SparseVectorResult createEmptySparseVector() {
        SparseVectorResult result = new SparseVectorResult();
        result.setSparseIdVector(new HashMap<>());
        result.setSparseWordVector(new HashMap<>());
        return result;
    }

    private DenseVectorResult createEmptyDenseVector() {
        DenseVectorResult result = new DenseVectorResult();
        result.setDenseVector(new ArrayList<>());
        return result;
    }

    private TranslateResult createEmptyTranslateResult(List<String> texts) {
        TranslateResult result = new TranslateResult();
        result.setTranslatedText(texts.size() == 1 ? texts.get(0) : texts);
        return result;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @lombok.Data
    public static class SparseVectorResult {
        private Map<String, Float> sparseIdVector;
        private Map<String, Float> sparseWordVector;
    }

    @lombok.Data
    public static class DenseVectorResult {
        private List<Float> denseVector;
    }

    @lombok.Data
    public static class TranslateResult {
        private Object translatedText;
    }
}
