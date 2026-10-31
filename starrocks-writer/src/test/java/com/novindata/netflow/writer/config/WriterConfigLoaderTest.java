package com.novindata.netflow.writer.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WriterConfigLoaderTest {
    @Test
    void environmentOverridesApplicationPollTimeout() throws Exception {
        WriterConfig config = WriterConfigLoader.loadFromClasspath(
                "application-test.properties", Map.of(
                "KAFKA_POLL_TIMEOUT_MS", "2000",
                "STARROCKS_LOAD_MAX_ATTEMPTS", "4",
                "STARROCKS_LOAD_RETRY_BACKOFF_MS", "1500"));

        assertEquals(2_000, config.kafka().pollTimeoutMs());
        assertEquals(4, config.starRocks().loadMaxAttempts());
        assertEquals(1_500, config.starRocks().loadRetryBackoffMs());
    }

    @Test
    void rejectsMissingNonNumericAndNonPositiveNumbers() {
        Properties settings = new Properties();

        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                () -> WriterConfigLoader.number(settings, "kafka.max.poll.records"));
        assertEquals("Missing setting: kafka.max.poll.records", missing.getMessage());

        settings.setProperty("kafka.max.poll.records", "many");
        IllegalArgumentException nonNumeric = assertThrows(IllegalArgumentException.class,
                () -> WriterConfigLoader.number(settings, "kafka.max.poll.records"));
        assertEquals("Setting must be a positive integer: kafka.max.poll.records",
                nonNumeric.getMessage());

        settings.setProperty("kafka.max.poll.records", "0");
        IllegalArgumentException nonPositive = assertThrows(IllegalArgumentException.class,
                () -> WriterConfigLoader.number(settings, "kafka.max.poll.records"));
        assertEquals("Setting must be a positive integer: kafka.max.poll.records",
                nonPositive.getMessage());
    }

    @Test
    void rejectsRetrySettingsOutsideSafeRange() {
        IllegalArgumentException tooManyAttempts = assertThrows(IllegalArgumentException.class,
                () -> WriterConfigLoader.loadFromClasspath(
                        "application-test.properties",
                        Map.of("STARROCKS_LOAD_MAX_ATTEMPTS", "11")));
        assertEquals(
                "Setting must be between 1 and 10: starrocks.load.max-attempts",
                tooManyAttempts.getMessage());

        IllegalArgumentException excessiveBackoff = assertThrows(IllegalArgumentException.class,
                () -> WriterConfigLoader.loadFromClasspath(
                        "application-test.properties",
                        Map.of("STARROCKS_LOAD_RETRY_BACKOFF_MS", "30001")));
        assertEquals(
                "Setting must be between 1 and 30000: starrocks.load.retry-backoff-ms",
                excessiveBackoff.getMessage());
    }
}
