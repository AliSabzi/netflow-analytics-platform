package com.novindata.netflow.writer;

import com.novindata.netflow.writer.monitoring.StarRocksMetrics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

public final class StarRocksClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(StarRocksClient.class);
    private static final long MAX_RETRY_BACKOFF_MS = 30_000;

    private final URI loadUrl;
    private final String authorization;
    private final int maxAttempts;
    private final int retryBackoffMs;
    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    public StarRocksClient(
            String feUrl,
            String database,
            String table,
            String user,
            String password,
            int maxAttempts,
            int retryBackoffMs) {
        String baseUrl = feUrl.endsWith("/") ? feUrl.substring(0, feUrl.length() - 1) : feUrl;
        loadUrl = URI.create(baseUrl + "/api/" + database + "/" + table + "/_stream_load");
        authorization = "Basic " + Base64.getEncoder().encodeToString(
                (user + ":" + password).getBytes(StandardCharsets.UTF_8));
        this.maxAttempts = maxAttempts;
        this.retryBackoffMs = retryBackoffMs;
    }

    public void load(List<String> rows, String columns, String label) throws Exception {
        long startedAt = System.nanoTime();
        String body = String.join("\n", rows) + "\n";
        try {
            sendWithRetry(body, columns, label);
            StarRocksMetrics.recordLoadSuccess(rows.size());
            LOGGER.info("loaded to StarRocks: rows={}, label={}", rows.size(), label);
        } catch (BadDataException rejectedRows) {
            throw rejectedRows;
        } catch (Exception loadFailure) {
            StarRocksMetrics.recordLoadFailure();
            throw loadFailure;
        } finally {
            StarRocksMetrics.recordLoadDuration(
                    (System.nanoTime() - startedAt) / 1_000_000_000.0);
        }
    }

    private void sendWithRetry(String body, String columns, String label) throws Exception {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                send(body, columns, label);
                return;
            } catch (RetryableLoadException retryable) {
                if (attempt == maxAttempts) {
                    throw new IOException(
                            "StarRocks load failed after " + attempt + " attempts: label=" + label,
                            retryable);
                }

                StarRocksMetrics.recordLoadRetry();
                long backoffMs = retryBackoffMs(attempt);
                LOGGER.warn(
                        "temporary StarRocks load failure; retrying: attempt={}/{}, "
                                + "backoffMs={}, label={}, reason={}",
                        attempt, maxAttempts, backoffMs, label, retryable.getMessage());
                sleep(backoffMs);
            }
        }
    }

    private void send(String body, String columns, String label) throws Exception {
        HttpResponse<String> response;
        try {
            response = request(loadUrl, body, columns, label);
        } catch (IOException networkFailure) {
            throw new RetryableLoadException(
                    "StarRocks network request failed: " + networkFailure.getMessage(), networkFailure);
        }

        if (response.statusCode() == 307 || response.statusCode() == 308) {
            String location = response.headers().firstValue("Location")
                    .orElseThrow(() -> new IOException("StarRocks redirect has no Location header"));
            response = request(loadUrl.resolve(location), body, columns, label);
        }

        if (isRetryableStatus(response.statusCode())) {
            throw new RetryableLoadException(
                    "StarRocks HTTP " + response.statusCode() + ": " + response.body());
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException(
                    "StarRocks HTTP " + response.statusCode() + ": " + response.body());
        }

        JsonNode result = json.readTree(response.body());
        String status = result.path("Status").asText();
        String message = result.path("Message").asText();

        if (status.equalsIgnoreCase("Success") || status.equalsIgnoreCase("Publish Timeout")) {
            return;
        }

        if (status.equalsIgnoreCase("Fail") &&
                (message.contains("Connection reset") || message.contains("Not connected") || message.contains("Fail to read from Socket"))) {
            throw new RetryableLoadException("StarRocks backend socket/connection failure, retrying: " + message);
        }

        if (status.equalsIgnoreCase("Label Already Exists")) {
            String existingJobStatus = result.path("ExistingJobStatus").asText();
            if (existingJobStatus.equalsIgnoreCase("FINISHED")) {
                return;
            }
            throw new RetryableLoadException(
                    "StarRocks label already exists with status " + existingJobStatus);
        }
        if (result.path("NumberFilteredRows").asLong() > 0) {
            throw new BadDataException(response.body());
        }
        throw new IllegalStateException("StarRocks rejected the load: " + response.body());
    }

    private HttpResponse<String> request(URI url, String body, String columns, String label)
            throws InterruptedException, RetryableLoadException {
        HttpRequest request = HttpRequest.newBuilder(url)
                .timeout(Duration.ofSeconds(60))
                .expectContinue(true)
                .header("Authorization", authorization)
                .header("Content-Type", "text/plain; charset=utf-8")
                .header("format", "csv")
                .header("column_separator", ",")
                .header("columns", columns)
                .header("strict_mode", "true")
                .header("max_filter_ratio", "0")
                .header("label", label)
                .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        try {
            return http.send(
                    request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException networkFailure) {
            throw new RetryableLoadException(
                    "StarRocks request failed: " + describe(networkFailure), networkFailure);
        }
    }

    private static String describe(Throwable failure) {
        String message = failure.getMessage();
        if (message == null || message.isBlank()) {
            return failure.getClass().getSimpleName();
        }
        return failure.getClass().getSimpleName() + ": " + message;
    }

    private static boolean isRetryableStatus(int statusCode) {
        return statusCode == 408
                || statusCode == 429
                || statusCode == 500
                || statusCode == 502
                || statusCode == 503
                || statusCode == 504;
    }

    private long retryBackoffMs(int failedAttempt) {
        long multiplier = 1L << Math.min(failedAttempt - 1, 20);
        return Math.min(retryBackoffMs * multiplier, MAX_RETRY_BACKOFF_MS);
    }

    private static void sleep(long milliseconds) throws InterruptedException {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    public static final class BadDataException extends Exception {
        public BadDataException(String message) {
            super(message);
        }
    }

    private static final class RetryableLoadException extends IOException {
        private RetryableLoadException(String message) {
            super(message);
        }

        private RetryableLoadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
