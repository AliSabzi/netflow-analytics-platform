package com.novindata.netflow.service;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.ConsumerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LateDataJobTest {

    @Mock
    private ConsumerFactory<String, byte[]> consumerFactory;
    @Mock
    private Consumer<String, byte[]> consumer;
    @Mock
    private NetFlowDecoder decoder;
    @Mock
    private HdfsSessionManager sessionManager;

    @Test
    void writesDifferentHoursFromSamePartitionBeforeCommittingOffset() throws Exception {
        String topic = "netflow-late";
        TopicPartition partition = new TopicPartition(topic, 0);
        Instant hour10 = Instant.parse("2026-09-29T06:30:00Z");
        Instant hour11 = Instant.parse("2026-09-29T07:30:00Z");
        ConsumerRecord<String, byte[]> first = lateRecord(topic, 0L, 7L, hour10);
        ConsumerRecord<String, byte[]> second = lateRecord(topic, 1L, 8L, hour11);
        GenericRecord firstDecoded = mock(GenericRecord.class);
        GenericRecord secondDecoded = mock(GenericRecord.class);
        LateDataJob job = new LateDataJob(
                consumerFactory, decoder, sessionManager, topic, "late-job-group");

        when(consumerFactory.createConsumer("late-job-group", null)).thenReturn(consumer);
        when(consumer.partitionsFor(topic))
                .thenReturn(List.of(new PartitionInfo(topic, 0, null, null, null)));
        when(consumer.endOffsets(List.of(partition))).thenReturn(Map.of(partition, 2L));
        when(consumer.position(partition)).thenReturn(0L, 2L);
        when(consumer.poll(any())).thenReturn(
                new ConsumerRecords<>(Map.of(partition, List.of(first, second))));
        when(decoder.decodeAndValidate(any(), any())).thenReturn(firstDecoded, secondDecoded);

        job.run();

        verify(sessionManager).writeLateRecord(firstDecoded, hour10);
        verify(sessionManager).writeLateRecord(secondDecoded, hour11);
        InOrder durableOrder = inOrder(sessionManager, consumer);
        durableOrder.verify(sessionManager).closeAndCommitAll();
        durableOrder.verify(consumer).commitSync(
                Map.of(partition, new OffsetAndMetadata(2L)));
    }

    private ConsumerRecord<String, byte[]> lateRecord(String topic, long lateOffset,
                                                       long sourceOffset, Instant sourceTimestamp) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(header(KafkaRecordMetadata.SOURCE_TOPIC, "netflow"));
        headers.add(header(KafkaRecordMetadata.SOURCE_PARTITION, "2"));
        headers.add(header(KafkaRecordMetadata.SOURCE_OFFSET, String.valueOf(sourceOffset)));
        headers.add(header(KafkaRecordMetadata.SOURCE_TIMESTAMP,
                String.valueOf(sourceTimestamp.toEpochMilli())));
        headers.add(header(KafkaRecordMetadata.SOURCE_TIMESTAMP_TYPE, "CREATE_TIME"));
        return new ConsumerRecord<>(topic, 0, lateOffset, 2_000L, TimestampType.CREATE_TIME,
                0, 3, "2026-09-29T10", new byte[]{1, 2, 3}, headers, Optional.empty());
    }

    private RecordHeader header(String name, String value) {
        return new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
