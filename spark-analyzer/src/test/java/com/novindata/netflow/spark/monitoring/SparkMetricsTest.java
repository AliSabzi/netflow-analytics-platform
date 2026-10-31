package com.novindata.netflow.spark.monitoring;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class SparkMetricsTest {
    @Container
    static final GenericContainer<?> gateway = new GenericContainer<>("prom/pushgateway:v1.11.3")
            .withExposedPorts(9091);
    private final HttpClient client = HttpClient.newHttpClient();

    private String address() {
        return gateway.getHost() + ":" + gateway.getMappedPort(9091);
    }

    private String scrape() throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("http://" + address() + "/metrics"))
                .GET().build(), HttpResponse.BodyHandlers.ofString()).body();
    }

    private double value(String metrics, String name) {
        String line = metrics.lines().filter(s -> s.startsWith(name + "{")).findFirst().orElseThrow();
        return Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
    }

    @BeforeEach
    void clearLastRun() throws Exception {
        client.send(HttpRequest.newBuilder(URI.create("http://" + address() + "/metrics/job/spark-analyzer"))
                .DELETE().build(), HttpResponse.BodyHandlers.discarding());
    }

    @Test
    void failedRetryKeepsPreviousSuccessAndReplacesAttemptCounts() throws Exception {
        SparkMetrics successful = new SparkMetrics();
        successful.recordFilesProcessed(3);
        successful.recordRecordsReceived(100);
        successful.recordRecordsCommitted(100);
        successful.publish(address(), true);
        String first = scrape();
        double successTime = value(first, "netflow_spark_last_success_timestamp_seconds");
        assertTrue(successTime > 0);
        assertEquals(3, value(first, "netflow_spark_last_run_files_processed"));
        assertEquals(100, value(first, "netflow_spark_last_run_records_received"));
        assertEquals(100, value(first, "netflow_spark_last_run_records_committed"));

        SparkMetrics failed = new SparkMetrics();
        failed.recordFilesProcessed(1);
        failed.recordRecordsReceived(20);
        failed.recordError();
        failed.publish(address(), false);
        String second = scrape();
        assertEquals(0, value(second, "netflow_spark_last_run_success"));
        assertEquals(1, value(second, "netflow_spark_last_run_files_processed"));
        assertEquals(20, value(second, "netflow_spark_last_run_records_received"));
        assertEquals(0, value(second, "netflow_spark_last_run_records_committed"));
        assertEquals(1, value(second, "netflow_spark_last_run_errors"));
        assertEquals(successTime, value(second, "netflow_spark_last_success_timestamp_seconds"));
        assertTrue(value(second, "netflow_spark_last_run_duration_seconds") >= 0);
        assertFalse(second.contains("pod="));
    }

    @Test
    void firstFailureDoesNotInventSuccessAndEmptyInputCanSucceed() throws Exception {
        new SparkMetrics().publish(address(), false);
        assertFalse(scrape().contains("netflow_spark_last_success_timestamp_seconds"));
        new SparkMetrics().publish(address(), true);
        String result = scrape();
        assertEquals(1, value(result, "netflow_spark_last_run_success"));
        assertEquals(0, value(result, "netflow_spark_last_run_files_processed"));
        assertEquals(0, value(result, "netflow_spark_last_run_records_received"));
        assertEquals(0, value(result, "netflow_spark_last_run_records_committed"));
        assertEquals(0, value(result, "netflow_spark_last_run_errors"));
        assertTrue(value(result, "netflow_spark_last_success_timestamp_seconds") > 0);
    }

    @Test
    void rejectedMetricsDoNotFailDataProcessing() throws Exception {
        HttpServer unavailable = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        unavailable.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        unavailable.start();
        try {
            assertDoesNotThrow(() -> new SparkMetrics().publish(
                    "127.0.0.1:" + unavailable.getAddress().getPort(), true));
        } finally {
            unavailable.stop(0);
        }
    }
}
