package com.example.vectorsync.benchmark;

import com.example.vectorsync.service.ElasticsearchService;
import com.example.vectorsync.service.MessageConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class ThroughputTestService {

    private final MessageConsumerService consumerService;
    private final ElasticsearchService elasticsearchService;

    private final AtomicLong testStartTime = new AtomicLong(0);
    private final AtomicLong testEndTime = new AtomicLong(0);
    private final AtomicInteger testStatus = new AtomicInteger(0);

    public static final int STATUS_NOT_STARTED = 0;
    public static final int STATUS_RUNNING = 1;
    public static final int STATUS_COMPLETED = 2;
    public static final int STATUS_FAILED = 3;

    public ThroughputResult startThroughputTest(int expectedRecords) {
        log.info("Starting throughput test with expected {} records", expectedRecords);

        testStartTime.set(System.currentTimeMillis());
        testStatus.set(STATUS_RUNNING);

        long initialEsCount;
        try {
            initialEsCount = elasticsearchService.count();
        } catch (Exception e) {
            log.error("Failed to get initial ES count", e);
            initialEsCount = 0;
        }

        ThroughputResult result = new ThroughputResult();
        result.setStartTime(testStartTime.get());
        result.setExpectedRecords(expectedRecords);
        result.setInitialEsCount(initialEsCount);

        return result;
    }

    public ThroughputResult completeThroughputTest() {
        testEndTime.set(System.currentTimeMillis());
        testStatus.set(STATUS_COMPLETED);

        log.info("Throughput test completed at {}", testEndTime.get());

        ThroughputResult result = new ThroughputResult();
        result.setEndTime(testEndTime.get());

        try {
            long finalEsCount = elasticsearchService.count();
            result.setFinalEsCount(finalEsCount);
            result.setActualRecords((int) (finalEsCount - result.getInitialEsCount()));
        } catch (Exception e) {
            log.error("Failed to get final ES count", e);
            result.setActualRecords(0);
        }

        long duration = testEndTime.get() - testStartTime.get();
        result.setDurationMs(duration);
        result.setTps(result.getActualRecords() * 1000L / Math.max(duration, 1));

        MessageConsumerService.SyncStatus syncStatus = consumerService.getStatus();
        result.setProcessedRecords((int) syncStatus.getTotalProcessed());
        result.setSuccessRecords((int) syncStatus.getTotalSuccess());
        result.setFailedRecords((int) syncStatus.getTotalFailed());

        log.info("Throughput test result: {} records in {}ms, TPS: {}",
                result.getActualRecords(), duration, result.getTps());

        return result;
    }

    public ThroughputResult failThroughputTest(String error) {
        testEndTime.set(System.currentTimeMillis());
        testStatus.set(STATUS_FAILED);

        ThroughputResult result = new ThroughputResult();
        result.setEndTime(testEndTime.get());
        result.setError(error);
        result.setDurationMs(testEndTime.get() - testStartTime.get());

        return result;
    }

    public ThroughputResult getCurrentStatus() {
        ThroughputResult result = new ThroughputResult();

        if (testStartTime.get() > 0) {
            result.setStartTime(testStartTime.get());
            result.setDurationMs(System.currentTimeMillis() - testStartTime.get());
        }

        result.setStatus(testStatus.get());

        try {
            result.setFinalEsCount(elasticsearchService.count());
        } catch (Exception e) {
            log.error("Failed to get ES count", e);
        }

        MessageConsumerService.SyncStatus syncStatus = consumerService.getStatus();
        result.setProcessedRecords((int) syncStatus.getTotalProcessed());
        result.setSuccessRecords((int) syncStatus.getTotalSuccess());
        result.setFailedRecords((int) syncStatus.getTotalFailed());

        return result;
    }

    public int getStatus() {
        return testStatus.get();
    }

    public void reset() {
        testStartTime.set(0);
        testEndTime.set(0);
        testStatus.set(STATUS_NOT_STARTED);
    }

    @lombok.Data
    public static class ThroughputResult {
        private long startTime;
        private long endTime;
        private int expectedRecords;
        private int actualRecords;
        private long durationMs;
        private long tps;
        private long initialEsCount;
        private long finalEsCount;
        private int processedRecords;
        private int successRecords;
        private int failedRecords;
        private int status;
        private String error;
    }
}
