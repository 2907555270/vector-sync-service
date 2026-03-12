package com.example.vectorsync.benchmark;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Service
public class ResourceMonitorService {

    private final Runtime runtime = Runtime.getRuntime();
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    public ResourceSnapshot getResourceSnapshot() {
        ResourceSnapshot snapshot = new ResourceSnapshot();
        snapshot.setTimestamp(System.currentTimeMillis());

        snapshot.setAvailableProcessors(runtime.availableProcessors());

        memoryBean.gc();
        Map<String, Object> heapMemory = new LinkedHashMap<>();
        heapMemory.put("used", memoryBean.getHeapMemoryUsage().getUsed());
        heapMemory.put("max", memoryBean.getHeapMemoryUsage().getMax());
        heapMemory.put("committed", memoryBean.getHeapMemoryUsage().getCommitted());
        heapMemory.put("init", memoryBean.getHeapMemoryUsage().getInit());
        snapshot.setHeapMemory(heapMemory);

        Map<String, Object> nonHeapMemory = new LinkedHashMap<>();
        nonHeapMemory.put("used", memoryBean.getNonHeapMemoryUsage().getUsed());
        nonHeapMemory.put("max", memoryBean.getNonHeapMemoryUsage().getMax());
        snapshot.setNonHeapMemory(nonHeapMemory);

        snapshot.setSystemLoadAverage(osBean.getSystemLoadAverage());
        snapshot.setAvailableMemory(getAvailableMemory());

        snapshot.setThreadCount(threadBean.getThreadCount());
        snapshot.setPeakThreadCount(threadBean.getPeakThreadCount());
        snapshot.setDaemonThreadCount(threadBean.getDaemonThreadCount());

        return snapshot;
    }

    private long getAvailableMemory() {
        Runtime rt = Runtime.getRuntime();
        long totalMemory = rt.totalMemory();
        long freeMemory = rt.freeMemory();
        long maxMemory = rt.maxMemory();
        return maxMemory - (totalMemory - freeMemory);
    }

    public Map<String, Object> getDetailedResourceInfo() {
        Map<String, Object> info = new LinkedHashMap<>();

        info.put("jvm", getJvmInfo());
        info.put("memory", getMemoryInfo());
        info.put("cpu", getCpuInfo());
        info.put("threads", getThreadInfo());

        return info;
    }

    private Map<String, Object> getJvmInfo() {
        Map<String, Object> jvm = new LinkedHashMap<>();
        jvm.put("name", ManagementFactory.getRuntimeMXBean().getVmName());
        jvm.put("version", ManagementFactory.getRuntimeMXBean().getVmVersion());
        jvm.put("uptime", ManagementFactory.getRuntimeMXBean().getUptime());
        return jvm;
    }

    private Map<String, Object> getMemoryInfo() {
        Map<String, Object> memory = new LinkedHashMap<>();

        Map<String, Long> heap = new LinkedHashMap<>();
        heap.put("used", memoryBean.getHeapMemoryUsage().getUsed());
        heap.put("max", memoryBean.getHeapMemoryUsage().getMax());
        heap.put("free", memoryBean.getHeapMemoryUsage().getUsed());
        memory.put("heap", heap);

        Map<String, Long> nonHeap = new LinkedHashMap<>();
        nonHeap.put("used", memoryBean.getNonHeapMemoryUsage().getUsed());
        nonHeap.put("max", memoryBean.getNonHeapMemoryUsage().getMax());
        memory.put("nonHeap", nonHeap);

        memory.put("available", getAvailableMemory());
        memory.put("total", runtime.totalMemory());

        return memory;
    }

    private Map<String, Object> getCpuInfo() {
        Map<String, Object> cpu = new LinkedHashMap<>();
        cpu.put("availableProcessors", runtime.availableProcessors());
        cpu.put("systemLoadAverage", osBean.getSystemLoadAverage());
        return cpu;
    }

    private Map<String, Object> getThreadInfo() {
        Map<String, Object> threads = new LinkedHashMap<>();
        threads.put("count", threadBean.getThreadCount());
        threads.put("peak", threadBean.getPeakThreadCount());
        threads.put("daemon", threadBean.getDaemonThreadCount());
        threads.put("totalStarted", threadBean.getTotalStartedThreadCount());
        return threads;
    }

    @lombok.Data
    public static class ResourceSnapshot {
        private long timestamp;
        private int availableProcessors;
        private Map<String, Object> heapMemory;
        private Map<String, Object> nonHeapMemory;
        private double systemLoadAverage;
        private long availableMemory;
        private int threadCount;
        private int peakThreadCount;
        private int daemonThreadCount;
    }
}
