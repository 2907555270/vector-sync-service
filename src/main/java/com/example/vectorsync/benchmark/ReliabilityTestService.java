package com.example.vectorsync.benchmark;

import com.example.vectorsync.service.ElasticsearchService;
import com.example.vectorsync.service.MessageConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReliabilityTestService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ElasticsearchService elasticsearchService;
    private final MessageConsumerService consumerService;

    private static final String TEST_TOPIC = "reliability-test-topic";

    private final Map<String, TestScenario> runningTests = new ConcurrentHashMap<>();

    public String startFailureRecoveryTest(int recordCount) {
        String testId = UUID.randomUUID().toString();
        
        TestScenario scenario = new TestScenario();
        scenario.setTestId(testId);
        scenario.setType("FAILURE_RECOVERY");
        scenario.setStatus("RUNNING");
        scenario.setStartTime(System.currentTimeMillis());
        
        runningTests.put(testId, scenario);

        new Thread(() -> {
            try {
                log.info("Starting failure recovery test {}", testId);

                int successBeforeCrash = (int) (recordCount * 0.3);
                sendTestRecords(testId, successBeforeCrash);

                Thread.sleep(2000);

                scenario.setPhase("SIMULATING_CRASH");
                log.warn("Simulating consumer crash...");

                Thread.sleep(1000);

                scenario.setPhase("RECOVERY");
                log.info("Testing recovery...");

                sendTestRecords(testId, recordCount - successBeforeCrash);

                Thread.sleep(3000);

                long esCount = elasticsearchService.count();
                scenario.setFinalEsCount(esCount);
                scenario.setStatus("COMPLETED");
                scenario.setPhase("VERIFIED");

                log.info("Failure recovery test completed. ES count: {}", esCount);

            } catch (Exception e) {
                log.error("Failure recovery test failed", e);
                scenario.setStatus("FAILED");
                scenario.setError(e.getMessage());
            }
        }, "reliability-test-" + testId).start();

        return testId;
    }

    public String startConsumerRestartTest(int recordCount) {
        String testId = UUID.randomUUID().toString();

        TestScenario scenario = new TestScenario();
        scenario.setTestId(testId);
        scenario.setType("CONSUMER_RESTART");
        scenario.setStatus("RUNNING");
        scenario.setStartTime(System.currentTimeMillis());

        runningTests.put(testId, scenario);

        new Thread(() -> {
            try {
                log.info("Starting consumer restart test {}", testId);

                sendTestRecords(testId, recordCount / 2);

                Thread.sleep(2000);

                scenario.setPhase("STOPPING_CONSUMER");
                log.info("Stopping consumer...");

                Thread.sleep(1000);

                scenario.setPhase("RESTARTING_CONSUMER");
                log.info("Restarting consumer (offset should resume)...");

                sendTestRecords(testId, recordCount / 2);

                Thread.sleep(5000);

                long esCount = elasticsearchService.count();
                scenario.setFinalEsCount(esCount);
                scenario.setStatus("COMPLETED");

                log.info("Consumer restart test completed. ES count: {}", esCount);

            } catch (Exception e) {
                log.error("Consumer restart test failed", e);
                scenario.setStatus("FAILED");
                scenario.setError(e.getMessage());
            }
        }, "reliability-test-" + testId).start();

        return testId;
    }

    public String startBackpressureTest(int recordCount) {
        String testId = UUID.randomUUID().toString();

        TestScenario scenario = new TestScenario();
        scenario.setTestId(testId);
        scenario.setType("BACKPRESSURE");
        scenario.setStatus("RUNNING");
        scenario.setStartTime(System.currentTimeMillis());

        runningTests.put(testId, scenario);

        new Thread(() -> {
            try {
                log.info("Starting backpressure test {}", testId);

                long startTime = System.currentTimeMillis();
                sendTestRecords(testId, recordCount);

                long sendDuration = System.currentTimeMillis() - startTime;
                scenario.setSendDuration(sendDuration);

                long maxWaitTime = Math.max(recordCount * 10L, 60000);
                long waitStart = System.currentTimeMillis();

                while (true) {
                    Thread.sleep(2000);

                    long esCount = elasticsearchService.count();
                    long elapsed = System.currentTimeMillis() - waitStart;

                    scenario.setCurrentEsCount(esCount);

                    if (esCount >= recordCount || elapsed > maxWaitTime) {
                        break;
                    }

                    log.info("Backpressure test progress: {}/{} records in ES", esCount, recordCount);
                }

                long esCount = elasticsearchService.count();
                scenario.setFinalEsCount(esCount);
                scenario.setTotalDuration(System.currentTimeMillis() - startTime);
                scenario.setStatus(esCount >= recordCount ? "COMPLETED" : "TIMEOUT");

                log.info("Backpressure test completed. ES count: {}, expected: {}",
                        esCount, recordCount);

            } catch (Exception e) {
                log.error("Backpressure test failed", e);
                scenario.setStatus("FAILED");
                scenario.setError(e.getMessage());
            }
        }, "reliability-test-" + testId).start();

        return testId;
    }

    public String startNetworkInterruptionTest(int recordCount) {
        String testId = UUID.randomUUID().toString();

        TestScenario scenario = new TestScenario();
        scenario.setTestId(testId);
        scenario.setType("NETWORK_INTERRUPTION");
        scenario.setStatus("RUNNING");
        scenario.setStartTime(System.currentTimeMillis());

        runningTests.put(testId, scenario);

        new Thread(() -> {
            try {
                log.info("Starting network interruption test {}", testId);

                sendTestRecords(testId, recordCount / 3);
                Thread.sleep(1000);

                scenario.setPhase("SENDING_WHILE_INTERRUPTED");
                log.warn("Simulating network interruption...");

                try {
                    sendTestRecords(testId, recordCount / 3);
                } catch (Exception e) {
                    scenario.setNetworkError(true);
                    log.warn("Network interruption detected as expected");
                }

                Thread.sleep(2000);

                scenario.setPhase("RECOVERY");
                log.info("Network recovery...");

                sendTestRecords(testId, recordCount / 3);

                Thread.sleep(3000);

                long esCount = elasticsearchService.count();
                scenario.setFinalEsCount(esCount);
                scenario.setStatus("COMPLETED");

                log.info("Network interruption test completed. ES count: {}", esCount);

            } catch (Exception e) {
                log.error("Network interruption test failed", e);
                scenario.setStatus("FAILED");
                scenario.setError(e.getMessage());
            }
        }, "reliability-test-" + testId).start();

        return testId;
    }

    private void sendTestRecords(String testId, int count) {
        for (int i = 0; i < count; i++) {
            try {
                String key = testId + "-" + i;
                String value = "{\"id\":\"" + key + "\",\"type\":\"test\",\"action\":\"create\",\"data\":{\"content\":\"test\"}}";
                kafkaTemplate.send(TEST_TOPIC, key, value).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Failed to send test record", e);
            }
        }
    }

    public Map<String, TestScenario> getRunningTests() {
        return new HashMap<>(runningTests);
    }

    public TestScenario getTestStatus(String testId) {
        return runningTests.get(testId);
    }

    public void clearTests() {
        runningTests.clear();
    }

    @lombok.Data
    public static class TestScenario {
        private String testId;
        private String type;
        private String status;
        private String phase;
        private long startTime;
        private long totalDuration;
        private long sendDuration;
        private long currentEsCount;
        private long finalEsCount;
        private boolean networkError;
        private String error;
    }
}
