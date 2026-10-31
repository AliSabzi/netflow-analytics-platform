package com.novindata.netflow.writer;

import com.novindata.netflow.writer.config.WriterConfig;
import com.novindata.netflow.writer.monitoring.StarRocksMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public final class DlqPublisher implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DlqPublisher.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Producer<byte[], byte[]> producer;
    private final String topic;
    private final Clock clock;

    public DlqPublisher(WriterConfig.KafkaConfig kafka) {
        this.producer = new KafkaProducer<>(producerSettings(kafka));
        this.topic = kafka.dlqTopic();
        this.clock = Clock.systemUTC();
    }

    public Future<RecordMetadata> publish(
            ConsumerRecord<byte[], byte[]> record,
            String reason,
            Exception error) throws Exception {
        try {
            return producer.send(createRecord(record, reason, error), (metadata, failure) -> {
                if (failure == null) {
                    StarRocksMetrics.recordDlq(reason);
                } else {
                    LOGGER.error("Failed to send record to DLQ: partition={}, offset={}, reason={}",
                            record.partition(), record.offset(), reason, failure);
                    StarRocksMetrics.recordError("dlq");
                }
            });
        } catch (Exception synchronousPublishFailure) {
            StarRocksMetrics.recordError("dlq");
            throw synchronousPublishFailure;
        }
    }

    private ProducerRecord<byte[], byte[]> createRecord(
            ConsumerRecord<byte[], byte[]> record,
            String reason,
            Exception error) throws Exception {
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("source_topic", record.topic());
        message.put("source_partition", record.partition());
        message.put("source_offset", record.offset());
        message.put("reason", reason);
        message.put("error", error.getMessage());
        message.put("failed_at", Instant.now(clock).toString());
        message.put("payload_base64", record.value() == null
                ? null : Base64.getEncoder().encodeToString(record.value()));

        byte[] key = identity(record).getBytes(StandardCharsets.UTF_8);
        byte[] value = JSON.writeValueAsBytes(message);
        return new ProducerRecord<>(topic, key, value);
    }

    @Override
    public void close() {
        producer.close();
    }

    private static Properties producerSettings(WriterConfig.KafkaConfig kafka) {
        Properties result = new Properties();
        result.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        result.put(ProducerConfig.ACKS_CONFIG, "all");
        result.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return result;
    }

    private static String identity(ConsumerRecord<byte[], byte[]> record) {
        return record.topic() + ":" + record.partition() + ":" + record.offset();
    }
}
