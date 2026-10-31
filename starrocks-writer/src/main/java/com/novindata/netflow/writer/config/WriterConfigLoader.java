package com.novindata.netflow.writer.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

public final class WriterConfigLoader {
    private WriterConfigLoader() {
    }

    public static WriterConfig load(Path file) throws IOException {
        return load(file, System.getenv());
    }

    public static WriterConfig load(Path file, Map<String, String> environment) throws IOException {
        try (InputStream input = Files.newInputStream(file)) {
            return load(input, environment);
        }
    }

    public static WriterConfig loadFromClasspath(String resourceName) throws IOException {
        return loadFromClasspath(resourceName, System.getenv());
    }

    static WriterConfig loadFromClasspath(
            String resourceName,
            Map<String, String> environment) throws IOException {
        try (InputStream input = WriterConfigLoader.class.getClassLoader()
                .getResourceAsStream(resourceName)) {
            if (input == null) {
                throw new IOException("Classpath resource not found: " + resourceName);
            }
            return load(input, environment);
        }
    }

    private static WriterConfig load(
            InputStream input,
            Map<String, String> environment) throws IOException {
        Properties settings = new Properties();
        settings.load(input);

        applyEnvironmentOverrides(settings, environment);
        validateSettings(settings);

        String password = settings.getProperty("starrocks.password", "");
        if (password.isBlank()) {
            throw new IllegalArgumentException("Set STARROCKS_PASSWORD before running the writer");
        }

        WriterConfig.KafkaConfig kafka = new WriterConfig.KafkaConfig(
                required(settings, "kafka.bootstrap.servers"),
                required(settings, "kafka.source.topic"),
                required(settings, "kafka.consumer.group"),
                required(settings, "kafka.dlq.topic"),
                number(settings, "kafka.max.poll.records"),
                number(settings, "kafka.poll.timeout.ms"),
                number(settings, "kafka.max.poll.interval.ms"),
                number(settings, "kafka.max.partition.fetch.bytes"),
                required(settings, "kafka.auto.offset.reset"));

        WriterConfig.StarRocksConfig starRocks = new WriterConfig.StarRocksConfig(
                required(settings, "starrocks.fe.http-url"),
                required(settings, "starrocks.fe.query-url"),
                required(settings, "starrocks.database"),
                required(settings, "starrocks.table"),
                required(settings, "starrocks.user"),
                password,
                numberInRange(settings, "starrocks.load.max-attempts", 1, 10),
                numberInRange(settings, "starrocks.load.retry-backoff-ms", 1, 30_000));

        return new WriterConfig(kafka, starRocks);
    }

    static void applyEnvironmentOverrides(
            Properties settings,
            Map<String, String> environment) {
        override(settings, environment, "KAFKA_BOOTSTRAP_SERVERS", "kafka.bootstrap.servers");
        override(settings, environment, "KAFKA_SOURCE_TOPIC", "kafka.source.topic");
        override(settings, environment, "KAFKA_CONSUMER_GROUP", "kafka.consumer.group");
        override(settings, environment, "KAFKA_DLQ_TOPIC", "kafka.dlq.topic");
        override(settings, environment, "KAFKA_MAX_POLL_RECORDS", "kafka.max.poll.records");
        override(settings, environment, "KAFKA_POLL_TIMEOUT_MS", "kafka.poll.timeout.ms");
        override(settings, environment, "KAFKA_MAX_POLL_INTERVAL_MS", "kafka.max.poll.interval.ms");
        override(settings, environment, "KAFKA_MAX_PARTITION_FETCH_BYTES", "kafka.max.partition.fetch.bytes");
        override(settings, environment, "KAFKA_AUTO_OFFSET_RESET", "kafka.auto.offset.reset");
        override(settings, environment, "STARROCKS_FE_HTTP_URL", "starrocks.fe.http-url");
        override(settings, environment, "STARROCKS_FE_QUERY_URL", "starrocks.fe.query-url");
        override(settings, environment, "STARROCKS_DATABASE", "starrocks.database");
        override(settings, environment, "STARROCKS_TABLE", "starrocks.table");
        override(settings, environment, "STARROCKS_USER", "starrocks.user");
        override(settings, environment, "STARROCKS_PASSWORD", "starrocks.password");
        override(settings, environment, "STARROCKS_LOAD_MAX_ATTEMPTS",
                "starrocks.load.max-attempts");
        override(settings, environment, "STARROCKS_LOAD_RETRY_BACKOFF_MS",
                "starrocks.load.retry-backoff-ms");
    }

    private static void override(
            Properties settings,
            Map<String, String> environment,
            String environmentName,
            String propertyName) {
        String value = environment.get(environmentName);
        if (value != null && !value.isBlank()) {
            settings.setProperty(propertyName, value);
        }
    }

    private static String required(Properties settings, String name) {
        String value = settings.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing setting: " + name);
        }
        return value.trim();
    }

    private static void validateSettings(Properties settings) {
        number(settings, "kafka.max.poll.records");
        number(settings, "kafka.poll.timeout.ms");
        number(settings, "kafka.max.poll.interval.ms");
        number(settings, "kafka.max.partition.fetch.bytes");
    }

    static int number(Properties settings, String name) {
        String value = required(settings, name);
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException("Setting must be a positive integer: " + name);
            }
            return parsed;
        } catch (NumberFormatException invalidNumber) {
            throw new IllegalArgumentException(
                    "Setting must be a positive integer: " + name,
                    invalidNumber);
        }
    }

    private static int numberInRange(
            Properties settings,
            String name,
            int minimum,
            int maximum) {
        int value = number(settings, name);
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    "Setting must be between " + minimum + " and " + maximum + ": " + name);
        }
        return value;
    }
}
