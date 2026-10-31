package com.novindata.netflow.writer.monitoring;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StarRocksMetricsTest {
    @Test
    void registersOnlyActionableStarRocksMetrics() {
        try {
            StarRocksMetrics.configureTopic("netflow");
            StarRocksMetrics.markReady();
            StarRocksMetrics.recordReceived(3);
            StarRocksMetrics.recordLoadSuccess(2);
            StarRocksMetrics.recordDlq("INVALID_AVRO");
            StarRocksMetrics.recordPartitionCommitted(3);
            StarRocksMetrics.recordLoadRetry();
            StarRocksMetrics.recordError("kafka_poll");
            StarRocksMetrics.recordPartitionBatchDuration(0.5);
            StarRocksMetrics.recordLoadDuration(0.25);

            Set<String> names = PrometheusRegistry.defaultRegistry.scrape().stream()
                    .map(metric -> metric.getMetadata().getPrometheusName())
                    .filter(name -> name.startsWith("netflow_starrocks_"))
                    .collect(Collectors.toSet());

            assertTrue(names.contains("netflow_starrocks_writer_ready"));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_records_received")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_records_loaded")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_records_committed")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_records_dlq")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_operation_retries")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_operation_errors")));
            assertTrue(names.contains("netflow_starrocks_last_commit_timestamp_seconds"));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_partition_batch_duration_seconds")));
            assertTrue(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_load_duration_seconds")));
            assertFalse(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_partition_batch_seconds")));
            assertFalse(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_load_seconds")));
            assertFalse(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_partition_batches")));
            assertFalse(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_loads")));
            assertFalse(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_current_batch_records")));
            assertFalse(names.stream().anyMatch(
                    name -> name.startsWith("netflow_starrocks_load_retries")));
        } finally {
            StarRocksMetrics.stop();
        }
    }
}
