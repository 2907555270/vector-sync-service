package com.example.vectorsync.benchmark;

import com.example.vectorsync.config.SyncProperties;
import com.example.vectorsync.service.ElasticsearchService;
import com.example.vectorsync.service.MessageConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class BenchmarkReportService {

    private final DataPreparationService dataPreparationService;
    private final ThroughputTestService throughputTestService;
    private final DataIntegrityService dataIntegrityService;
    private final ReliabilityTestService reliabilityTestService;
    private final ResourceMonitorService resourceMonitorService;
    private final ElasticsearchService elasticsearchService;
    private final MessageConsumerService consumerService;
    private final SyncProperties syncProperties;

    public BenchmarkReport runFullBenchmark(int recordCount) {
        log.info("Starting full benchmark with {} records", recordCount);

        BenchmarkReport report = new BenchmarkReport();
        report.setStartTime(LocalDateTime.now());
        report.setRecordCount(recordCount);

        ResourceSnapshot resourceBefore = resourceMonitorService.getResourceSnapshot();
        report.setResourceBefore(resourceBefore);

        report.setPrepareResult(dataPreparationService.prepareTestData(recordCount, "article"));

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        ThroughputResult throughputResult = throughputTestService.completeThroughputTest();
        report.setThroughputResult(throughputResult);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        IntegrityResult integrityResult = dataIntegrityService.verifyIntegrity(
                recordCount,
                syncProperties.getElasticsearch().getIndex()
        );
        report.setIntegrityResult(integrityResult);

        ResourceSnapshot resourceAfter = resourceMonitorService.getResourceSnapshot();
        report.setResourceAfter(resourceAfter);

        report.setEndTime(LocalDateTime.now());
        report.setOverallPassed(integrityResult.isPassed());

        generateSummary(report);

        log.info("Full benchmark completed. Passed: {}", report.isOverallPassed());

        return report;
    }

    public Map<String, Object> generateQuickReport() {
        Map<String, Object> report = new LinkedHashMap<>();

        report.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        try {
            long esCount = elasticsearchService.count();
            report.put("esTotalCount", esCount);
        } catch (Exception e) {
            report.put("esTotalCount", "error: " + e.getMessage());
        }

        MessageConsumerService.SyncStatus syncStatus = consumerService.getStatus();
        Map<String, Object> sync = new LinkedHashMap<>();
        sync.put("totalProcessed", syncStatus.getTotalProcessed());
        sync.put("totalSuccess", syncStatus.getTotalSuccess());
        sync.put("totalFailed", syncStatus.getTotalFailed());
        report.put("syncStatus", sync);

        report.put("resources", resourceMonitorService.getResourceSnapshot());

        return report;
    }

    private void generateSummary(BenchmarkReport report) {
        StringBuilder summary = new StringBuilder();
        summary.append("\n========== BENCHMARK REPORT ==========\n");
        summary.append(String.format("Record Count: %d\n", report.getRecordCount()));
        summary.append(String.format("Start Time: %s\n", report.getStartTime()));
        summary.append(String.format("End Time: %s\n", report.getEndTime()));

        if (report.getPrepareResult() != null) {
            summary.append("\n--- Data Preparation ---\n");
            summary.append(String.format("Success: %d\n", report.getPrepareResult().getSuccessCount()));
            summary.append(String.format("Failed: %d\n", report.getPrepareResult().getFailedCount()));
            summary.append(String.format("TPS: %d\n", report.getPrepareResult().getTps()));
        }

        if (report.getThroughputResult() != null) {
            summary.append("\n--- Throughput ---\n");
            summary.append(String.format("Duration: %d ms\n", report.getThroughputResult().getDurationMs()));
            summary.append(String.format("TPS: %d\n", report.getThroughputResult().getTps()));
        }

        if (report.getIntegrityResult() != null) {
            summary.append("\n--- Data Integrity ---\n");
            summary.append(String.format("Zero Loss: %s\n", report.getIntegrityResult().isZeroLoss()));
            summary.append(String.format("Zero Duplicate: %s\n", report.getIntegrityResult().isZeroDuplicate()));
            summary.append(String.format("Lost: %d\n", report.getIntegrityResult().getLostCount()));
            summary.append(String.format("Duplicates: %d\n", report.getIntegrityResult().getDuplicateCount()));
        }

        summary.append("\n=======================================\n");
        summary.append(String.format("OVERALL: %s\n", report.isOverallPassed() ? "PASSED" : "FAILED"));

        report.setSummary(summary.toString());
    }

    public String exportReportAsText(BenchmarkReport report) {
        if (report.getSummary() == null) {
            generateSummary(report);
        }
        return report.getSummary();
    }

    @lombok.Data
    public static class BenchmarkReport {
        private LocalDateTime startTime;
        private LocalDateTime endTime;
        private int recordCount;
        private DataPreparationService.PrepareResult prepareResult;
        private ThroughputTestService.ThroughputResult throughputResult;
        private DataIntegrityService.IntegrityResult integrityResult;
        private ResourceMonitorService.ResourceSnapshot resourceBefore;
        private ResourceMonitorService.ResourceSnapshot resourceAfter;
        private boolean overallPassed;
        private String summary;
    }
}
