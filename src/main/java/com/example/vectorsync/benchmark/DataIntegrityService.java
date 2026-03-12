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

    public DuplicateResult checkDuplicates(String index) throws IOException {
        DuplicateResult result = new DuplicateResult();

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

        long duplicateCount = 0;
        if (response.aggregations().containsKey("duplicate_ids")) {
            duplicateCount = response.aggregations().get("duplicate_ids").sterms().buckets().array().size();
            result.setDuplicateCount((int) duplicateCount);

            List<String> duplicates = new ArrayList<>();
            response.aggregations().get("duplicate_ids").sterms().buckets().array().forEach(bucket -> {
                duplicates.add(bucket.key().stringValue() + ": " + bucket.docCount() + " docs");
            });
            result.setDuplicateIds(duplicates);
        }

        log.info("Duplicate check result: {} duplicates found", duplicateCount);
        return result;
    }

    public SampleVerifyResult verifySampleData(int sampleSize) {
        SampleVerifyResult result = new SampleVerifyResult();
        result.setTotalCount(sampleSize);

        int verifiedCount = 0;
        List<String> errors = new ArrayList<>();

        try {
            SearchResponse<Map> response = esClient.search(s -> s
                            .index("vectors")
                            .size(sampleSize)
                            .source(sc -> sc.filter(f -> f.includes("id", "type", "data", "timestamp"))),
                    Map.class
            );

            for (var hit : response.hits().hits()) {
                Map<String, Object> source = hit.source();
                if (source == null) {
                    errors.add("Missing source for id: " + hit.id());
                    continue;
                }

                if (source.get("id") == null) {
                    errors.add("Missing id field for document: " + hit.id());
                    continue;
                }

                if (source.get("type") == null) {
                    errors.add("Missing type field for document: " + hit.id());
                    continue;
                }

                if (source.get("data") == null) {
                    errors.add("Missing data field for document: " + hit.id());
                    continue;
                }

                verifiedCount++;
            }

            result.setVerifiedCount(verifiedCount);
            result.setErrors(errors);
            result.setPassRate(verifiedCount * 100.0 / sampleSize);

        } catch (Exception e) {
            log.error("Sample verification failed", e);
            result.setErrors(List.of(e.getMessage()));
        }

        log.info("Sample verification: {}/{} passed, pass rate: {}%",
                verifiedCount, sampleSize, result.getPassRate());

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
