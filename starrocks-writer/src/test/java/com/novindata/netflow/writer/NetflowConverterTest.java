package com.novindata.netflow.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import com.novindata.netflow.core.avro.NetFlowV2Record;
import com.novindata.netflow.core.validation.NetFlowValidationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class NetflowConverterTest {
    @Test
    void convertsGoodDataAndRejectsBadData() throws Exception {
        Schema schema = NetFlowV2Record.getClassSchema();
        byte[] goodData = encode(sampleRecord(schema), schema);
        NetflowConverter converter = new NetflowConverter();

        String csv = converter.toCsv(kafkaRecord(goodData));

        assertTrue(csv.contains("10.0.0.1"));
        assertTrue(csv.contains("netflow:0:7"));
        assertTrue(csv.contains("2023-11-15"));
        assertTrue(csv.contains("2023-11-15 01:43:20"));

        assertThrows(IOException.class, () -> converter.toCsv(kafkaRecord(new byte[]{1, 2, 3})));
    }

    @Test
    void rejectsRecordWithoutKafkaTimestamp() throws Exception {
        Schema schema = NetFlowV2Record.getClassSchema();
        byte[] data = encode(sampleRecord(schema), schema);

        assertThrows(IOException.class,
                () -> new NetflowConverter().toCsv(new ConsumerRecord<>("netflow", 0, 7, null, data)));
    }

    @Test
    void rejectsAvroRecordThatFailsValidation() throws Exception {
        Schema schema = NetFlowV2Record.getClassSchema();
        GenericData.Record record = sampleRecord(schema);
        record.put("IPV4_SRC_ADDR", "999.0.0.1");

        byte[] data = encode(record, schema);

        assertThrows(
                NetFlowValidationException.class,
                () -> new NetflowConverter().toCsv(kafkaRecord(data)));
    }

    private static GenericData.Record sampleRecord(Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object value = switch (field.schema().getType()) {
                case STRING -> "value";
                case INT -> 1;
                case LONG -> 1L;
                case DOUBLE -> 1.0;
                default -> throw new IllegalArgumentException("Unsupported field: " + field.name());
            };
            record.put(field.name(), value);
        }
        record.put("IPV4_SRC_ADDR", "10.0.0.1");
        record.put("IPV4_DST_ADDR", "10.0.0.2");
        record.put("FTP_COMMAND_RET_CODE", 0);
        return record;
    }

    private static byte[] encode(GenericData.Record record, Schema schema) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(schema);
        var encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        writer.write(record, encoder);
        encoder.flush();
        return bytes.toByteArray();
    }

    private static ConsumerRecord<byte[], byte[]> kafkaRecord(byte[] value) {
        return new ConsumerRecord<>(
                "netflow",
                0,
                7,
                1_700_000_000_000L,
                TimestampType.CREATE_TIME,
                0,
                value.length,
                null,
                value,
                new RecordHeaders(),
                Optional.empty());
    }
}
