package com.novindata.netflow.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DlqPublisherTest {

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    private DlqPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new DlqPublisher(kafkaTemplate);
        ReflectionTestUtils.setField(publisher, "dlqTopic", "netflow-dlq");
        ReflectionTestUtils.setField(publisher, "lateTopic", "netflow-late");
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void publishesInvalidRecordToDlqWithOriginalKeyAndReason() throws Exception {
        ConsumerRecord<String, byte[]> source = record("source-key", timestamp(2026, 9, 29, 10, 15));

        publisher.publish(List.of(source), List.of("invalid payload"));

        ProducerRecord<String, byte[]> published = capturedRecord();
        assertEquals("netflow-dlq", published.topic());
        assertEquals("source-key", published.key());
        assertEquals("invalid payload", headerValue(published, "error-reason"));
        assertEquals("7", headerValue(published, "source-offset"));
    }

    @Test
    void publishesLateRecordWithTehranEventHourAsKey() throws Exception {
        ConsumerRecord<String, byte[]> source = record("source-key", timestamp(2026, 9, 29, 10, 15));

        publisher.publishLate(List.of(source));

        ProducerRecord<String, byte[]> published = capturedRecord();
        assertEquals("netflow-late", published.topic());
        assertEquals("2026-09-29T10", published.key());
        assertEquals("CREATE_TIME", headerValue(published, "source-timestamp-type"));
        assertNull(published.headers().lastHeader("error-reason"));
    }

    private ConsumerRecord<String, byte[]> record(String key, long timestamp) {
        return new ConsumerRecord<>("netflow", 2, 7L, timestamp, TimestampType.CREATE_TIME,
                key.length(), 3, key, new byte[]{1, 2, 3}, new RecordHeaders(), Optional.empty());
    }

    private ProducerRecord<String, byte[]> capturedRecord() {
        ArgumentCaptor<ProducerRecord<String, byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        return captor.getValue();
    }

    private String headerValue(ProducerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private long timestamp(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, ZoneId.of("Asia/Tehran"))
                .toInstant()
                .toEpochMilli();
    }
}
