package com.novindata.netflow.monitoring;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;

import java.io.IOException;

public final class PipelineMetrics {
    private static final Counter RECORDS_RECEIVED = Counter.builder()
            .name("netflow_hdfs_records_received_total")
            .help("Number of records received from Kafka")
            .labelNames("topic")
            .register();

    private static final Counter RECORDS_WRITTEN = Counter.builder()
            .name("netflow_hdfs_records_written_total")
            .help("Number of records appended to temporary HDFS Parquet files")
            .labelNames("topic")
            .register();

    private static final Counter RECORDS_DLQ = Counter.builder()
            .name("netflow_hdfs_records_dlq_total")
            .help("Number of records successfully sent to the DLQ")
            .labelNames("topic", "reason")
            .register();

    private static final Counter RECORDS_COMMITTED = Counter.builder()
            .name("netflow_hdfs_records_committed_total")
            .help("Number of records committed in final HDFS files")
            .labelNames("topic")
            .register();

    private static final Gauge BUFFERED_RECORDS = Gauge.builder()
            .name("netflow_hdfs_buffered_records")
            .help("Number of records in the current uncommitted HDFS file")
            .labelNames("topic")
            .register();

    private static final Gauge LAST_COMMIT = Gauge.builder()
            .name("netflow_hdfs_last_commit_timestamp_seconds")
            .help("Unix timestamp of the latest successful HDFS file commit")
            .labelNames("topic")
            .register();

    private static final Counter KAFKA_RECORDS_COMMITTED = Counter.builder()
            .name("netflow_hdfs_kafka_records_committed_total")
            .help("Number of source records covered by committed Kafka offsets")
            .labelNames("topic")
            .register();

    private static final Gauge LAST_KAFKA_COMMIT = Gauge.builder()
            .name("netflow_hdfs_last_kafka_commit_timestamp_seconds")
            .help("Unix timestamp of the latest successful Kafka offset commit")
            .labelNames("topic")
            .register();

    private static final Histogram BATCH_DURATION = Histogram.builder()
            .name("netflow_hdfs_batch_duration_seconds")
            .help("Time spent processing one Kafka poll, including HDFS writes and commit")
            .labelNames("topic")
            .classicOnly()
            .register();

    private static final Counter OPERATION_RETRIES = Counter.builder()
            .name("netflow_hdfs_operation_retries_total")
            .help("Number of explicit retry attempts after a failed operation")
            .labelNames("topic", "stage")
            .register();

    private static final Counter OPERATION_ERRORS = Counter.builder()
            .name("netflow_hdfs_operation_errors_total")
            .help("Number of pipeline operations that failed without recovery")
            .labelNames("topic", "stage")
            .register();

    private static final Gauge WRITER_READY = Gauge.builder()
            .name("netflow_hdfs_writer_ready")
            .help("Whether application startup completed successfully")
            .register();

    private static HTTPServer server;
    private static boolean jvmMetricsRegistered;
    private static String topic = "unknown";

    private PipelineMetrics() {
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
        WRITER_READY.set(0);
        if (server != null) {
            server.close();
            server = null;
        }
    }

    public static void markReady() {
        WRITER_READY.set(1);
    }

    public static void configureTopic(String sourceTopic) {
        topic = sourceTopic;
        BUFFERED_RECORDS.labelValues(topic).set(0);
        LAST_COMMIT.labelValues(topic).set(0);
        LAST_KAFKA_COMMIT.labelValues(topic).set(0);
    }

    public static void recordReceived(int count) {
        RECORDS_RECEIVED.labelValues(topic).inc(count);
    }

    public static void recordWritten(int bufferedRecords) {
        RECORDS_WRITTEN.labelValues(topic).inc();
        BUFFERED_RECORDS.labelValues(topic).set(bufferedRecords);
    }

    public static void recordDlq(String reason, int count) {
        RECORDS_DLQ.labelValues(topic, reason).inc(count);
    }

    public static void recordError(String stage) {
        OPERATION_ERRORS.labelValues(topic, stage).inc();
    }

    public static void recordRetry(String stage) {
        OPERATION_RETRIES.labelValues(topic, stage).inc();
    }

    public static void recordBatchDuration(double seconds) {
        BATCH_DURATION.labelValues(topic).observe(seconds);
    }

    public static void recordFileCommitted(int recordCount) {
        RECORDS_COMMITTED.labelValues(topic).inc(recordCount);
        BUFFERED_RECORDS.labelValues(topic).set(0);
        LAST_COMMIT.labelValues(topic).set(System.currentTimeMillis() / 1000.0);
    }

    public static void recordKafkaCommitted(long recordCount) {
        KAFKA_RECORDS_COMMITTED.labelValues(topic).inc(recordCount);
        LAST_KAFKA_COMMIT.labelValues(topic).set(System.currentTimeMillis() / 1000.0);
    }

    public static void clearBuffered() {
        BUFFERED_RECORDS.labelValues(topic).set(0);
    }
}
