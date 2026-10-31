package com.novindata.netflow.writer;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.novindata.netflow.core.avro.NetFlowV2Record;
import com.novindata.netflow.core.validation.NetFlowValidator;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public final class NetflowConverter {
    private static final ZoneId PIPELINE_ZONE = ZoneId.of("Asia/Tehran");
    private static final DateTimeFormatter DATE =
            DateTimeFormatter.ISO_LOCAL_DATE.withZone(PIPELINE_ZONE);
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(PIPELINE_ZONE);

    private final SpecificDatumReader<NetFlowV2Record> avroReader;

    public NetflowConverter() {
        avroReader = new SpecificDatumReader<>(NetFlowV2Record.class);
    }

    public String toCsv(ConsumerRecord<byte[], byte[]> record) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            throw new IOException("Kafka value is empty");
        }

        NetFlowV2Record avro;
        try {
            avro = avroReader.read(null, DecoderFactory.get().binaryDecoder(record.value(), null));
        } catch (IOException | RuntimeException invalidAvro) {
            throw new IOException("Invalid Avro record", invalidAvro);
        }

        NetFlowValidator.validate(avro);

        java.util.StringJoiner csv = new java.util.StringJoiner(",");
        for (Schema.Field field : avro.getSchema().getFields()) {
            csv.add(csvValue(avro.get(field.pos())));
        }

        csv.add(record.topic() + ":" + record.partition() + ":" + record.offset());
        csv.add(record.topic());
        csv.add(String.valueOf(record.partition()));
        csv.add(String.valueOf(record.offset()));
        csv.add(record.timestampType().name());

        if (record.timestamp() < 0) {
            throw new IOException("Kafka record timestamp is missing");
        }
        Instant timestamp = Instant.ofEpochMilli(record.timestamp());
        csv.add(String.valueOf(record.timestamp()));
        csv.add(DATE.format(timestamp));
        csv.add(TIME.format(timestamp));

        return csv.toString();
    }

    private static String csvValue(Object value) throws IOException {
        if (value == null) {
            return "\\N";
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof CharSequence) {
            return value.toString();
        }
        throw new IOException("Unsupported Avro value: " + value.getClass().getSimpleName());
    }

    public String getCsvColumnsHeader() {
        java.util.StringJoiner columns = new java.util.StringJoiner(",");
        for (Schema.Field field : NetFlowV2Record.getClassSchema().getFields()) {
            columns.add(field.name());
        }
        columns.add("record_id");
        columns.add("kafka_topic");
        columns.add("kafka_partition");
        columns.add("kafka_offset");
        columns.add("kafka_timestamp_type");
        columns.add("kafka_timestamp_ms");
        columns.add("kafka_record_date");
        columns.add("kafka_record_time");
        return columns.toString();
    }
}
