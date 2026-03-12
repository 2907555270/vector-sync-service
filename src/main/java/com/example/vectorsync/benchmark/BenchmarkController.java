package com.example.vectorsync.benchmark;

import com.example.vectorsync.service.ElasticsearchService;
import com.example.vectorsync.service.MessageConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/benchmark")
@RequiredArgsConstructor
public class BenchmarkController {

    private final DataPreparationService dataPreparationService;
    private final ThroughputTestService throughputTestService;
    private final DataIntegrityService dataIntegrityService;
    private final ReliabilityTestService reliabilityTestService;
    private final ResourceMonitorService resourceMonitorService;
    private final BenchmarkReportService reportService;
    private final ElasticsearchService elasticsearchService;
    private final MessageConsumerService consumerService;

    @PostMapping("/prepare")
    public ResponseEntity<Map<String, Object>> prepareData(
            @RequestParam(defaultValue = "30000") int count,
            @RequestParam(defaultValue = "article") String dataType) {

        log.info("Preparing {} test records of type {}", count, dataType);
        DataPreparationService.PrepareResult result = dataPreparationService.prepareTestData(count, dataType);

        return ResponseEntity.ok(Map.of(
                "success", result.getSuccessCount() > 0,
                "result", result
        ));
    }

    @PostMapping("/throughput/start")
    public ResponseEntity<Map<String, Object>> startThroughputTest(
            @RequestParam(defaultValue = "30000") int count) {

        log.info("Starting throughput test with {} records", count);
        ThroughputTestService.ThroughputResult result = throughputTestService.startThroughputTest(count);

        return ResponseEntity.ok(Map.of(
                "status", "started",
                "result", result
        ));
    }

    @PostMapping("/throughput/complete")
    public ResponseEntity<Map<String, Object>> completeThroughputTest() {
        log.info("Completing throughput test");
        ThroughputTestService.ThroughputResult result = throughputTestService.completeThroughputTest();

        return ResponseEntity.ok(Map.of(
                "status", "completed",
                "result", result
        ));
    }

    @GetMapping("/throughput/status")
    public ResponseEntity<Map<String, Object>> getThroughputStatus() {
        ThroughputTestService.ThroughputResult result = throughputTestService.getCurrentStatus();

        return ResponseEntity.ok(Map.of(
                "status", result.getStatus(),
                "result", result
        ));
    }

    @PostMapping("/integrity/verify")
    public ResponseEntity<Map<String, Object>> verifyIntegrity(
            @RequestParam(defaultValue = "30000") int expectedCount) {

        log.info("Verifying integrity with expected count {}", expectedCount);
        DataIntegrityService.IntegrityResult result = dataIntegrityService.verifyIntegrity(
                expectedCount,
                "vectors"
        );

        return ResponseEntity.ok(Map.of(
                "passed", result.isPassed(),
                "result", result
        ));
    }

    @GetMapping("/integrity/details")
    public ResponseEntity<Map<String, Object>> getIntegrityDetails(
            @RequestParam(defaultValue = "100") int sampleSize) {

        Map<String, Object> report = dataIntegrityService.getDetailedReport(sampleSize);
        return ResponseEntity.ok(report);
    }

    @PostMapping("/reliability/failure-recovery")
    public ResponseEntity<Map<String, Object>> testFailureRecovery(
            @RequestParam(defaultValue = "10000") int count) {

        log.info("Starting failure recovery test with {} records", count);
        String testId = reliabilityTestService.startFailureRecoveryTest(count);

        return ResponseEntity.ok(Map.of(
                "testId", testId,
                "status", "started"
        ));
    }

    @PostMapping("/reliability/consumer-restart")
    public ResponseEntity<Map<String, Object>> testConsumerRestart(
            @RequestParam(defaultValue = "10000") int count) {

        log.info("Starting consumer restart test with {} records", count);
        String testId = reliabilityTestService.startConsumerRestartTest(count);

        return ResponseEntity.ok(Map.of(
                "testId", testId,
                "status", "started"
        ));
    }

    @PostMapping("/reliability/backpressure")
    public ResponseEntity<Map<String, Object>> testBackpressure(
            @RequestParam(defaultValue = "30000") int count) {

        log.info("Starting backpressure test with {} records", count);
        String testId = reliabilityTestService.startBackpressureTest(count);

        return ResponseEntity.ok(Map.of(
                "testId", testId,
                "status", "started"
        ));
    }

    @GetMapping("/reliability/status/{testId}")
    public ResponseEntity<Map<String, Object>> getReliabilityTestStatus(@PathVariable String testId) {
        ReliabilityTestService.TestScenario scenario = reliabilityTestService.getTestStatus(testId);

        if (scenario == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(Map.of(
                "testId", testId,
                "scenario", scenario
        ));
    }

    @GetMapping("/reliability/tests")
    public ResponseEntity<Map<String, Object>> getAllReliabilityTests() {
        return ResponseEntity.ok(reliabilityTestService.getRunningTests());
    }

    @GetMapping("/resources")
    public ResponseEntity<Map<String, Object>> getResources() {
        ResourceMonitorService.ResourceSnapshot snapshot = resourceMonitorService.getResourceSnapshot();
        Map<String, Object> details = resourceMonitorService.getDetailedResourceInfo();

        return ResponseEntity.ok(Map.of(
                "snapshot", snapshot,
                "details", details
        ));
    }

    @PostMapping("/run")
    public ResponseEntity<Map<String, Object>> runFullBenchmark(
            @RequestParam(defaultValue = "30000") int count) {

        log.info("Starting full benchmark with {} records", count);
        BenchmarkReportService.BenchmarkReport report = reportService.runFullBenchmark(count);

        return ResponseEntity.ok(Map.of(
                "passed", report.isOverallPassed(),
                "report", report
        ));
    }

    @GetMapping("/report")
    public ResponseEntity<Map<String, Object>> getQuickReport() {
        return ResponseEntity.ok(reportService.generateQuickReport());
    }

    @PostMapping("/reset")
    public ResponseEntity<Map<String, Object>> resetBenchmark() {
        throughputTestService.reset();
        reliabilityTestService.clearTests();

        return ResponseEntity.ok(Map.of(
                "status", "reset"
        ));
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        return ResponseEntity.ok(Map.of(
                "kafka", "UP",
                "elasticsearch", "UP",
                "throughputTest", throughputTestService.getStatus(),
                "esCount", elasticsearchService.count(),
                "syncStatus", consumerService.getStatus()
        ));
    }
}
