package com.novindata.netflow.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaWatermarkStoreTest {

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Mock
    private ConsumerFactory<String, byte[]> consumerFactory;

    @Mock
    private Consumer<String, byte[]> consumer;

    private KafkaWatermarkStore store;

    @BeforeEach
    void setUp() {
        store = new KafkaWatermarkStore(kafkaTemplate, consumerFactory);
        ReflectionTestUtils.setField(store, "watermarkTopic", "netflow-watermarks");
    }

    @Test
    @SuppressWarnings("unchecked")
    void savesPartitionWatermark() throws Exception {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        store.save("netflow", 3, Instant.ofEpochMilli(1_790_670_600_000L), 54_321L);

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, byte[]> record = captor.getValue();

        assertEquals("netflow-watermarks", record.topic());
        assertEquals("netflow:3", record.key());
        assertEquals("1790670600000,54321", new String(record.value(), StandardCharsets.UTF_8));
    }

    @Test
    void loadsLatestWatermarkForEveryPartition() {
        TopicPartition watermarkPartition = new TopicPartition("netflow-watermarks", 0);
        ConsumerRecord<String, byte[]> oldState = new ConsumerRecord<>(
                "netflow-watermarks", 0, 0, "netflow:3", "1000,10".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<String, byte[]> latestState = new ConsumerRecord<>(
                "netflow-watermarks", 0, 1, "netflow:3", "2000,20".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<String, byte[]> otherPartition = new ConsumerRecord<>(
                "netflow-watermarks", 0, 2, "netflow:4", "3000,30".getBytes(StandardCharsets.UTF_8));

        when(consumerFactory.createConsumer()).thenReturn(consumer);
        when(consumer.endOffsets(List.of(watermarkPartition))).thenReturn(Map.of(watermarkPartition, 3L));
        when(consumer.position(watermarkPartition)).thenReturn(0L, 3L);
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(
                Map.of(watermarkPartition, List.of(oldState, latestState, otherPartition))));

        Map<String, KafkaWatermarkStore.WatermarkState> watermarks = store.loadAll();

        assertEquals(Instant.ofEpochMilli(2000), watermarks.get("netflow:3").maxTimestamp());
        assertEquals(20L, watermarks.get("netflow:3").durableOffset());
        assertEquals(Instant.ofEpochMilli(3000), watermarks.get("netflow:4").maxTimestamp());
    }

    @Test
    void returnsMinimumOnlyWhenEverySourcePartitionHasWatermark() {
        TopicPartition watermarkPartition = new TopicPartition("netflow-watermarks", 0);
        ConsumerRecord<String, byte[]> partitionZero = new ConsumerRecord<>(
                "netflow-watermarks", 0, 0, "netflow:0", "3000,30".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<String, byte[]> partitionOne = new ConsumerRecord<>(
                "netflow-watermarks", 0, 1, "netflow:1", "2000,20".getBytes(StandardCharsets.UTF_8));

        when(consumerFactory.createConsumer()).thenReturn(consumer);
        when(consumer.endOffsets(List.of(watermarkPartition))).thenReturn(Map.of(watermarkPartition, 2L));
        when(consumer.position(watermarkPartition)).thenReturn(0L, 2L);
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(
                Map.of(watermarkPartition, List.of(partitionZero, partitionOne))));
        when(consumer.partitionsFor("netflow")).thenReturn(List.of(
                new PartitionInfo("netflow", 0, null, null, null),
                new PartitionInfo("netflow", 1, null, null, null)));

        assertEquals(Instant.ofEpochMilli(2000), store.minimumMaxTimestamp("netflow"));
    }
}
