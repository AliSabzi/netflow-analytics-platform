package com.novindata.netflow.spark.monitoring;

import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.exporter.pushgateway.PushGateway;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.Duration;


public final class SparkMetrics {
    private static final Logger log = LoggerFactory.getLogger(SparkMetrics.class);
    private final PrometheusRegistry registry = new PrometheusRegistry();
    private final long startedAt = System.nanoTime();

    private final Gauge success = Gauge.builder()
            .name("netflow_spark_last_run_success")
            .help("Whether the latest completed attempt processed all ready folders successfully")
            .register(registry);

    private final Gauge duration = Gauge.builder()
            .name("netflow_spark_last_run_duration_seconds")
            .help("Duration of the latest completed attempt, including startup and cleanup")
            .register(registry);

    private final Gauge files = Gauge.builder()
            .name("netflow_spark_last_run_files_processed")
            .help("Parquet files committed and marked processed during the latest attempt")
            .register(registry);

    private final Gauge recordsReceived = Gauge.builder()
            .name("netflow_spark_last_run_records_received")
            .help("Input records read from ready HDFS folders during the latest attempt")
            .register(registry);

    private final Gauge recordsCommitted = Gauge.builder()
            .name("netflow_spark_last_run_records_committed")
            .help("Input records whose aggregate output completed during the latest attempt")
            .register(registry);

    private final Gauge errors = Gauge.builder()
            .name("netflow_spark_last_run_errors")
            .help("Folders that failed during the latest attempt")
            .register(registry);

    public void recordFilesProcessed(int count) {
        files.inc(count);
    }

    public void recordRecordsReceived(long count) {
        recordsReceived.inc(count);
    }

    public void recordRecordsCommitted(long count) {
        recordsCommitted.inc(count);
    }

    public void recordError() {
        errors.inc();
    }

    public void publish(String address, boolean completedSuccessfully) {
        if (address == null || address.isBlank()) {
            log.warn("PUSHGATEWAY_ADDRESS is unset; Spark batch metrics were not published");
            return;

        }
        success.set(completedSuccessfully ? 1 : 0);

        duration.set((System.nanoTime() - startedAt) / 1_000_000_000.0);
        if (completedSuccessfully) {
            Gauge.builder().name("netflow_spark_last_success_timestamp_seconds")
                    .help("Unix time of the latest fully successful attempt, including an empty input")
                    .register(registry).set(System.currentTimeMillis() / 1000.0);
        }
        try {
            PushGateway.builder().address(address).job("spark-analyzer").registry(registry)
                    .connectionTimeout(Duration.ofSeconds(5)).readTimeout(Duration.ofSeconds(5))
                    .build().pushAdd();
        } catch (IOException | RuntimeException e) {
            log.warn("Failed to publish Spark batch metrics", e);
        }
    }
}
