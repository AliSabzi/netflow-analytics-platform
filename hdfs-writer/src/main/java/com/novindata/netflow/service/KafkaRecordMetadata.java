package com.novindata.netflow.service;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public record KafkaRecordMetadata(
        String topic,
        int partition,
        long offset,
        Instant timestamp,
        String timestampType) {

    static final String SOURCE_TOPIC = "source-topic";
    static final String SOURCE_PARTITION = "source-partition";
    static final String SOURCE_OFFSET = "source-offset";
    static final String SOURCE_TIMESTAMP = "source-timestamp";
    static final String SOURCE_TIMESTAMP_TYPE = "source-timestamp-type";

    public static KafkaRecordMetadata from(ConsumerRecord<String, byte[]> record) {
        return new KafkaRecordMetadata(
                record.topic(),
                record.partition(),
                record.offset(),
                Instant.ofEpochMilli(record.timestamp()),
                record.timestampType().name());
    }

    public static KafkaRecordMetadata fromSourceHeaders(ConsumerRecord<String, byte[]> record) {
        return new KafkaRecordMetadata(
                header(record, SOURCE_TOPIC),
                Integer.parseInt(header(record, SOURCE_PARTITION)),
                Long.parseLong(header(record, SOURCE_OFFSET)),
                Instant.ofEpochMilli(Long.parseLong(header(record, SOURCE_TIMESTAMP))),
                header(record, SOURCE_TIMESTAMP_TYPE));
    }

    public void writeTo(GenericRecord record) {
        record.put("KAFKA_RECORD_TIMESTAMP", timestamp.toEpochMilli());
        record.put("KAFKA_TOPIC", topic);
        record.put("KAFKA_PARTITION", partition);
        record.put("KAFKA_OFFSET", offset);
        record.put("KAFKA_TIMESTAMP_TYPE", timestampType);
    }

    private static String header(ConsumerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Late record is missing header: " + name);
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
