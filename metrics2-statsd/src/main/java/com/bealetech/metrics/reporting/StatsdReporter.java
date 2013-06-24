/**
 * Copyright (C) 2012-2013 Sean Laurent
 * Copyright (C) 2013 Michael Keesey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.bealetech.metrics.reporting;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class StatsdReporter extends AbstractPollingReporter implements MetricProcessor<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(StatsdReporter.class);
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    protected final String prefix;
    protected final MetricPredicate predicate;
    protected final Locale locale = Locale.US;
    protected final Clock clock;
    protected final UDPSocketProvider socketProvider;
    protected final VirtualMachineMetrics vm;
    protected Writer writer;
    protected ByteArrayOutputStream outputData;

    private boolean printVMMetrics = true;
        private StatsdSerializer serializer;

    public interface UDPSocketProvider {
        DatagramSocket get() throws Exception;
        DatagramPacket newPacket(ByteArrayOutputStream out);
    }

    public StatsdReporter(String host, int port) {
        this(Metrics.defaultRegistry(), host, port, null);
    }

    public StatsdReporter(String host, int port, String prefix) {
        this(Metrics.defaultRegistry(), host, port, prefix);
    }

    public StatsdReporter(MetricsRegistry metricsRegistry, String host, int port) {
        this(metricsRegistry, host, port, null);
    }

    public StatsdReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix) {
        this(metricsRegistry,
             prefix,
             MetricPredicate.ALL,
             new DefaultSocketProvider(host, port),
             Clock.defaultClock());
    }

    public StatsdReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, UDPSocketProvider socketProvider, Clock clock) {
        this(metricsRegistry, prefix, predicate, socketProvider, clock, VirtualMachineMetrics.getInstance());
    }

    public StatsdReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, UDPSocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm) {
        this(metricsRegistry, prefix, predicate, socketProvider, clock, vm, "statsd-reporter");
    }

    public StatsdReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, UDPSocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm, String name) {
        super(metricsRegistry, name);

        this.socketProvider = socketProvider;
        this.vm = vm;

        this.clock = clock;

        if (prefix != null) {
            // Pre-append the "." so that we don't need to make anything conditional later.
            this.prefix = prefix + ".";
        } else {
            this.prefix = "";
        }
        this.predicate = predicate;
        this.outputData = new ByteArrayOutputStream();
    }

    public boolean isPrintVMMetrics() {
        return printVMMetrics;
    }

    public void setPrintVMMetrics(boolean printVMMetrics) {
        this.printVMMetrics = printVMMetrics;
    }

    @Override
    public void run() {
        DatagramSocket socket = null;
        try {
            socket = this.socketProvider.get();
            outputData.reset();
            writer = new BufferedWriter(new OutputStreamWriter(this.outputData, UTF_8));
            serializer = new StatsdSerializer(prefix, writer);

            final long epoch = clock.time() / 1000;
            if (this.printVMMetrics) {
                printVmMetrics(epoch);
            }
            printRegularMetrics(epoch);

            // Send UDP data
            writer.flush();
            DatagramPacket packet = this.socketProvider.newPacket(outputData);
            packet.setData(outputData.toByteArray());
            socket.send(packet);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error writing to StatsD", e);
            } else {
                LOG.warn("Error writing to StatsD: {}", e.getMessage());
            }
            if (writer != null) {
                try {
                    writer.flush();
                } catch (IOException e1) {
                    LOG.error("Error while flushing writer:", e1);
                }
            }
        } finally {
            if (socket != null) {
                socket.close();
            }
            writer = null;
        }
    }

    protected void printVmMetrics(long epoch) {
        // Memory
        serializer.writeGauge("jvm.memory.totalInit", vm.totalInit());
        serializer.writeGauge("jvm.memory.totalUsed", vm.totalUsed());
        serializer.writeGauge("jvm.memory.totalMax", vm.totalMax());
        serializer.writeGauge("jvm.memory.totalCommitted", vm.totalCommitted());

        serializer.writeGauge("jvm.memory.heapInit", vm.heapInit());
        serializer.writeGauge("jvm.memory.heapUsed", vm.heapUsed());
        serializer.writeGauge("jvm.memory.heapMax", vm.heapMax());
        serializer.writeGauge("jvm.memory.heapCommitted", vm.heapCommitted());

        serializer.writeGauge("jvm.memory.heapUsage", vm.heapUsage());
        serializer.writeGauge("jvm.memory.nonHeapUsage", vm.nonHeapUsage());

        for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            serializer.writeGauge("jvm.memory.memory_pool_usages." + pool.getKey(), pool.getValue());
        }

        // Buffer Pool
        final Map<String, VirtualMachineMetrics.BufferPoolStats> bufferPoolStats = vm.getBufferPoolStats();
        if (!bufferPoolStats.isEmpty()) {
            serializer.writeGauge("jvm.buffers.direct.count", bufferPoolStats.get("direct").getCount());
            serializer.writeGauge("jvm.buffers.direct.memoryUsed", bufferPoolStats.get("direct").getMemoryUsed());
            serializer.writeGauge("jvm.buffers.direct.totalCapacity", bufferPoolStats.get("direct").getTotalCapacity());

            serializer.writeGauge("jvm.buffers.mapped.count", bufferPoolStats.get("mapped").getCount());
            serializer.writeGauge("jvm.buffers.mapped.memoryUsed", bufferPoolStats.get("mapped").getMemoryUsed());
            serializer.writeGauge("jvm.buffers.mapped.totalCapacity", bufferPoolStats.get("mapped").getTotalCapacity());
        }

        serializer.writeGauge("jvm.daemon_thread_count", vm.daemonThreadCount());
        serializer.writeGauge("jvm.thread_count", vm.threadCount());
        serializer.writeGauge("jvm.uptime", vm.uptime());
        serializer.writeGauge("jvm.fd_usage", vm.fileDescriptorUsage());

        for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
            serializer.writeGauge("jvm.thread-states." + entry.getKey().toString().toLowerCase(), entry.getValue());
        }

        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            final String name = "jvm.gc." + entry.getKey();
            serializer.writeGauge(name + ".time", entry.getValue().getTime(TimeUnit.MILLISECONDS));
            serializer.writeGauge(name + ".runs", entry.getValue().getRuns());
        }
    }

    protected void printRegularMetrics(long epoch) {
        for (Map.Entry<String,SortedMap<MetricName,Metric>> entry : getMetricsRegistry().groupedMetrics(predicate).entrySet()) {
            for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        metric.processWith(this, subEntry.getKey(), epoch);
                    } catch (Exception ignored) {
                        LOG.error("Error printing regular metrics:", ignored);
                    }
                }
            }
        }
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Long epoch) {
        final String sanitizedName = sanitizeName(name);
        serializer.writeGauge(sanitizedName + ".count", meter.count());
        serializer.writeTimer(sanitizedName + ".meanRate", meter.meanRate());
        serializer.writeTimer(sanitizedName + ".1MinuteRate", meter.oneMinuteRate());
        serializer.writeTimer(sanitizedName + ".5MinuteRate", meter.fiveMinuteRate());
        serializer.writeTimer(sanitizedName + ".15MinuteRate", meter.fifteenMinuteRate());
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Long epoch) {
        serializer.writeGauge(sanitizeName(name) + ".count", counter.count());
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Long epoch) {
        final String sanitizedName = sanitizeName(name);
        sendSummarizable(sanitizedName, histogram);
        sendSampling(sanitizedName, histogram);
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Long epoch) {
        processMeter(name, timer, epoch);
        final String sanitizedName = sanitizeName(name);
        sendSummarizable(sanitizedName, timer);
        sendSampling(sanitizedName, timer);
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Long epoch) {
        Object value = gauge.value();
        if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
            serializer.writeGauge(sanitizeName(name) + ".count", ((Number) value).doubleValue());
        } else if (value instanceof Number) {
            serializer.writeGauge(sanitizeName(name) + ".count", ((Number) value).longValue());
        } else {
            LOG.warn("Cannot serialize non-numeric guage {} with value {} for statsd",
                gauge,
                gauge.value());
        }
    }

    protected void sendSummarizable(String sanitizedName, Summarizable metric) {
        serializer.writeTimer(sanitizedName + ".min", metric.min());
        serializer.writeTimer(sanitizedName + ".max", metric.max());
        serializer.writeTimer(sanitizedName + ".mean", metric.mean());
        serializer.writeTimer(sanitizedName + ".stddev", metric.stdDev());
    }

    protected void sendSampling(String sanitizedName, Sampling metric) {
        final Snapshot snapshot = metric.getSnapshot();
        serializer.writeTimer(sanitizedName + ".median", snapshot.getMedian());
        serializer.writeTimer(sanitizedName + ".75percentile", snapshot.get75thPercentile());
        serializer.writeTimer(sanitizedName + ".95percentile", snapshot.get95thPercentile());
        serializer.writeTimer(sanitizedName + ".98percentile", snapshot.get98thPercentile());
        serializer.writeTimer(sanitizedName + ".99percentile", snapshot.get99thPercentile());
        serializer.writeTimer(sanitizedName + ".999percentile", snapshot.get999thPercentile());
    }

    protected String sanitizeName(MetricName name) {
        final StringBuilder sb = new StringBuilder()
                .append(name.getGroup())
                .append('.')
                .append(name.getType())
                .append('.');
        if (name.hasScope()) {
            sb.append(name.getScope())
                    .append('.');
        }
        return sb.append(name.getName()).toString();
    }

    public static class DefaultSocketProvider implements UDPSocketProvider {

        private final String host;
        private final int port;

        public DefaultSocketProvider(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public DatagramSocket get() throws SocketException {
            return new DatagramSocket();
        }

        @Override
        public DatagramPacket newPacket(ByteArrayOutputStream out) {
            byte[] dataBuffer;

            if (out != null) {
                dataBuffer = out.toByteArray();
            }
            else {
                dataBuffer = new byte[8192];
            }

            try {
                return new DatagramPacket(dataBuffer, dataBuffer.length, InetAddress.getByName(this.host), this.port);
            } catch (Exception e) {
                return null;
            }
        }
    }
}
