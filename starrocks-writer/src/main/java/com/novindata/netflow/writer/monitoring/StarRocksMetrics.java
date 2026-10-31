package com.novindata.netflow.writer.monitoring;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;

import java.io.IOException;

public final class StarRocksMetrics {
    private static final Counter RECORDS_RECEIVED = Counter.builder()
            .name("netflow_starrocks_records_received_total")
            .help("Number of records received from Kafka")
            .labelNames("topic")
            .register();

    private static final Counter RECORDS_LOADED = Counter.builder()
            .name("netflow_starrocks_records_loaded_total")
            .help("Number of records successfully loaded into StarRocks")
            .labelNames("topic")
            .register();

    private static final Counter RECORDS_COMMITTED = Counter.builder()
            .name("netflow_starrocks_records_committed_total")
            .help("Number of source records covered by committed Kafka offsets")
            .labelNames("topic")
            .register();

    private static final Counter RECORDS_DLQ = Counter.builder()
            .name("netflow_starrocks_records_dlq_total")
            .help("Number of records successfully sent to the DLQ")
            .labelNames("topic", "reason")
            .register();

    private static final Counter OPERATION_RETRIES = Counter.builder()
            .name("netflow_starrocks_operation_retries_total")
            .help("Number of explicit retry attempts after a failed operation")
            .labelNames("topic", "stage")
            .register();

    private static final Counter OPERATION_ERRORS = Counter.builder()
            .name("netflow_starrocks_operation_errors_total")
            .help("Number of writer operations that failed without recovery")
            .labelNames("topic", "stage")
            .register();

    private static final Gauge WRITER_READY = Gauge.builder()
            .name("netflow_starrocks_writer_ready")
            .help("Whether StarRocks writer startup completed successfully")
            .register();

    private static final Gauge LAST_COMMIT = Gauge.builder()
            .name("netflow_starrocks_last_commit_timestamp_seconds")
            .help("Unix timestamp of the latest successful Kafka offset commit")
            .labelNames("topic")
            .register();

    private static final Histogram PARTITION_BATCH_DURATION = Histogram.builder()
            .name("netflow_starrocks_partition_batch_duration_seconds")
            .help("Time spent processing and committing one Kafka partition batch")
            .labelNames("topic")
            .classicOnly()
            .register();

    private static final Histogram LOAD_DURATION = Histogram.builder()
            .name("netflow_starrocks_load_duration_seconds")
            .help("End-to-end duration of one StarRocks stream load, including retries")
            .labelNames("topic")
            .classicOnly()
            .register();

    private static HTTPServer server;
    private static boolean jvmMetricsRegistered;
    private static String topic = "unknown";

    private StarRocksMetrics() {
    }

    public static synchronized void start(int port) throws IOException {
        if (server != null) {
            return;
        }
        if (!jvmMetricsRegistered) {
            JvmMetrics.builder().register();
            jvmMetricsRegistered = true;
        }
        server = HTTPServer.builder().port(port).buildAndStart();
        System.out.println("Prometheus metrics: http://localhost:" + server.getPort() + "/metrics");
    }

    public static synchronized void stop() {
        markNotReady();
        if (server != null) {
            server.close();
            server = null;
        }
    }

    public static void markReady() {
        WRITER_READY.set(1);
    }

    public static void markNotReady() {
        WRITER_READY.set(0);
    }

    public static void configureTopic(String sourceTopic) {
        topic = sourceTopic;
        LAST_COMMIT.labelValues(topic).set(0);
    }

    public static void recordReceived(int count) {
        RECORDS_RECEIVED.labelValues(topic).inc(count);
    }

    public static void recordPartitionCommitted(int recordCount) {
        RECORDS_COMMITTED.labelValues(topic).inc(recordCount);
        LAST_COMMIT.labelValues(topic).set(System.currentTimeMillis() / 1000.0);
    }

    public static void recordDlq(String reason) {
        RECORDS_DLQ.labelValues(topic, reason).inc();
    }

    public static void recordLoadSuccess(int recordCount) {
        RECORDS_LOADED.labelValues(topic).inc(recordCount);
    }

    public static void recordLoadFailure() {
        OPERATION_ERRORS.labelValues(topic, "starrocks_load").inc();
    }

    public static void recordLoadRetry() {
        OPERATION_RETRIES.labelValues(topic, "starrocks_load").inc();
    }

    public static void recordError(String stage) {
        OPERATION_ERRORS.labelValues(topic, stage).inc();
    }

    public static void recordPartitionBatchDuration(double seconds) {
        PARTITION_BATCH_DURATION.labelValues(topic).observe(seconds);
    }

    public static void recordLoadDuration(double seconds) {
        LOAD_DURATION.labelValues(topic).observe(seconds);
    }
}
