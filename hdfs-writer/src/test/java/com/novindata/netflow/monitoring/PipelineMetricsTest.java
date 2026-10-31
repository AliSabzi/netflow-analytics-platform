package com.novindata.netflow.monitoring;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineMetricsTest {
    @Test
    void registersOnlyActionableHdfsMetrics() {
        try {
            PipelineMetrics.configureTopic("netflow");
            PipelineMetrics.markReady();
            PipelineMetrics.recordReceived(3);
            PipelineMetrics.recordWritten(1);
            PipelineMetrics.recordDlq("validation", 1);
            PipelineMetrics.recordFileCommitted(2);
            PipelineMetrics.recordKafkaCommitted(3);
            PipelineMetrics.recordRetry("kafka_ack");
            PipelineMetrics.recordError("hdfs");
            PipelineMetrics.recordBatchDuration(0.25);

            Set<String> names = PrometheusRegistry.defaultRegistry.scrape().stream()
                    .map(metric -> metric.getMetadata().getPrometheusName())
                    .filter(name -> name.startsWith("netflow_hdfs_"))
                    .collect(Collectors.toSet());

            assertTrue(names.contains("netflow_hdfs_writer_ready"));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_hdfs_records_received")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_hdfs_records_written")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_hdfs_records_committed")));
            assertTrue(names.contains("netflow_hdfs_buffered_records"));
            assertTrue(names.contains("netflow_hdfs_last_commit_timestamp_seconds"));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_hdfs_kafka_records_committed")));
            assertTrue(names.contains("netflow_hdfs_last_kafka_commit_timestamp_seconds"));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_hdfs_records_dlq")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_hdfs_operation_retries")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_hdfs_operation_errors")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_hdfs_batch_duration_seconds")));
        } finally {
            PipelineMetrics.stop();
        }
    }
}
