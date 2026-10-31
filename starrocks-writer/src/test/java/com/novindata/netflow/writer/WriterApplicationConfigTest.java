package com.novindata.netflow.writer;

import com.novindata.netflow.writer.config.WriterConfig;
import com.novindata.netflow.writer.config.WriterConfigLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WriterApplicationConfigTest {
    @Test
    void usesKafkaConsumerValuesFromTestConfiguration() throws Exception {
        WriterConfig config = WriterConfigLoader.load(testSettingsFile(), Map.of());

        Properties consumer = WriterApplication.consumerSettings(config.kafka());

        assertEquals("kafka-test:9092",
                consumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(50_000, consumer.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(300_000, consumer.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals(5_242_880, consumer.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        assertEquals("earliest", consumer.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(false, consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    void environmentOverridesKafkaConsumerValues() throws Exception {
        WriterConfig config = WriterConfigLoader.load(testSettingsFile(), Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_MAX_POLL_RECORDS", "2500",
                "KAFKA_MAX_POLL_INTERVAL_MS", "600000",
                "KAFKA_MAX_PARTITION_FETCH_BYTES", "10485760",
                "KAFKA_AUTO_OFFSET_RESET", "latest"
        ));

        Properties consumer = WriterApplication.consumerSettings(config.kafka());

        assertEquals("kafka:9092", consumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(2_500, consumer.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(600_000, consumer.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals(10_485_760, consumer.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        assertEquals("latest", consumer.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    private static Path testSettingsFile() throws Exception {
        return Path.of(Objects.requireNonNull(
                WriterApplicationConfigTest.class.getResource("/application-test.properties"),
                "Missing application-test.properties").toURI());
    }
}
