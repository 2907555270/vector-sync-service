package com.example.vectorsync.benchmark;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.example.vectorsync.service.ElasticsearchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class DataIntegrityService {

    private final ElasticsearchClient esClient;
    private final ElasticsearchService elasticsearchService;

    public IntegrityResult verifyIntegrity(int expectedCount, String index) {
        log.info("Starting integrity verification, expected count: {}", expectedCount);

        IntegrityResult result = new IntegrityResult();
        result.setExpectedCount(expectedCount);

        long startTime = System.currentTimeMillis();

        try {
            long actualCount = elasticsearchService.count();
            result.setActualCount(actualCount);
            result.setLostCount(expectedCount - actualCount);

            result.setZeroLoss(actualCount >= expectedCount);
            log.info("Count verification: expected={}, actual={}", expectedCount, actualCount);

            DuplicateResult duplicateResult = checkDuplicates(index);
            result.setDuplicateCount(duplicateResult.getDuplicateCount());
            result.setZeroDuplicate(duplicateResult.getDuplicateCount() == 0);

            SampleVerifyResult sampleResult = verifySampleData(100);
            result.setSampleVerified(sampleResult.getVerifiedCount());
            result.setSampleTotal(sampleResult.getTotalCount());
            result.setSamplePassRate(sampleResult.getPassRate());

        } catch (Exception e) {
            log.error("Integrity verification failed", e);
            result.setError(e.getMessage());
        }

        result.setDurationMs(System.currentTimeMillis() - startTime);
        result.setPassed(result.isZeroLoss() && result.isZeroDuplicate());

        log.info("Integrity verification completed: passed={}, duration={}ms",
                result.isPassed(), result.getDurationMs());

        return result;
    }

    public DuplicateResult checkDuplicates(String index) {
        DuplicateResult result = new DuplicateResult();
        result.setDuplicateCount(0);
        result.setDuplicateIds(new ArrayList<>());

        try {
            SearchResponse<Map> response = esClient.search(s -> s
                            .index(index)
                            .size(0)
                            .source(sc -> sc.filter(f -> f.includes("id")))
                            .aggs(a -> a
                                    .terms("duplicate_ids", t -> t
                                            .field("id")
                                            .size(10000)
                                            .minDocCount(2)
                                    )
                            ),
                    Map.class
            );

            if (response.aggregations() != null && 
                response.aggregations().containsKey("duplicate_ids")) {
                
                var agg = response.aggregations().get("duplicate_ids");
                if (agg != null && agg.isSterms()) {
                    var buckets = agg.sterms().buckets();
                    if (buckets != null) {
                        List<String> duplicates = new ArrayList<>();
                        long duplicateCount = 0;
                        
                        for (var bucket : buckets.array()) {
                            duplicates.add(bucket.key().stringValue() + ": " + bucket.docCount() + " docs");
                            duplicateCount++;
                        }
                        
                        result.setDuplicateCount((int) duplicateCount);
                        result.setDuplicateIds(duplicates);
                    }
                }
            }

            log.info("Duplicate check result for index '{}': {} duplicates found", index, result.getDuplicateCount());

        } catch (Exception e) {
            log.error("Failed to check duplicates for index '{}': {}", index, e.getMessage());
            result.setDuplicateIds(List.of("Error: " + e.getMessage()));
        }

        return result;
    }

    public SampleVerifyResult verifySampleData(int sampleSize) {
        SampleVerifyResult result = new SampleVerifyResult();
        result.setTotalCount(sampleSize);
        result.setVerifiedCount(0);
        result.setPassRate(0.0);
        result.setErrors(new ArrayList<>());

        try {
            SearchResponse<Map> response = esClient.search(s -> s
                            .index("vectors")
                            .size(sampleSize)
                            .source(sc -> sc.filter(f -> f.includes("id", "type", "data", "timestamp"))),
                    Map.class
            );

            if (response.hits() == null || response.hits().hits() == null) {
                log.warn("No hits returned from ES");
                return result;
            }

            for (var hit : response.hits().hits()) {
                Map<String, Object> source = hit.source();
                if (source == null) {
                    result.getErrors().add("Missing source for id: " + hit.id());
                    continue;
                }

                if (source.get("id") == null) {
                    result.getErrors().add("Missing id field for document: " + hit.id());
                    continue;
                }

                if (source.get("type") == null) {
                    result.getErrors().add("Missing type field for document: " + hit.id());
                    continue;
                }

                if (source.get("data") == null) {
                    result.getErrors().add("Missing data field for document: " + hit.id());
                    continue;
                }

                result.setVerifiedCount(result.getVerifiedCount() + 1);
            }

            result.setPassRate(result.getVerifiedCount() * 100.0 / sampleSize);

        } catch (Exception e) {
            log.error("Sample verification failed: {}", e.getMessage());
            result.getErrors().add("Error: " + e.getMessage());
        }

        log.info("Sample verification: {}/{} passed, pass rate: {}%",
                result.getVerifiedCount(), sampleSize, result.getPassRate());

        return result;
    }

    public Map<String, Object> getDetailedReport(int sampleSize) {
        Map<String, Object> report = new HashMap<>();

        try {
            long totalCount = elasticsearchService.count();
            report.put("totalCount", totalCount);

            DuplicateResult duplicateResult = checkDuplicates("vectors");
            report.put("duplicateCount", duplicateResult.getDuplicateCount());
            report.put("duplicateIds", duplicateResult.getDuplicateIds());

            SampleVerifyResult sampleResult = verifySampleData(sampleSize);
            report.put("sampleVerified", sampleResult.getVerifiedCount());
            report.put("sampleTotal", sampleResult.getTotalCount());
            report.put("sampleErrors", sampleResult.getErrors());

        } catch (Exception e) {
            log.error("Failed to generate detailed report", e);
            report.put("error", e.getMessage());
        }

        return report;
    }

    @lombok.Data
    public static class IntegrityResult {
        private int expectedCount;
        private long actualCount;
        private long lostCount;
        private boolean zeroLoss;
        private int duplicateCount;
        private boolean zeroDuplicate;
        private int sampleVerified;
        private int sampleTotal;
        private double samplePassRate;
        private long durationMs;
        private boolean passed;
        private String error;
    }

    @lombok.Data
    public static class DuplicateResult {
        private int duplicateCount;
        private List<String> duplicateIds;
    }

    @lombok.Data
    public static class SampleVerifyResult {
        private int totalCount;
        private int verifiedCount;
        private double passRate;
        private List<String> errors;
    }
}
