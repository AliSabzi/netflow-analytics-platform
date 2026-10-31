package com.novindata.netflow.service;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaWatermarkStore {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ConsumerFactory<String, byte[]> consumerFactory;
    private final Map<String, WatermarkState> watermarks = new HashMap<>();
    private Consumer<String, byte[]> watermarkConsumer;
    private TopicPartition watermarkPartition;

    @Value("${app.kafka.watermark-topic}")
    private String watermarkTopic;

    public KafkaWatermarkStore(KafkaTemplate<String, byte[]> kafkaTemplate,
                               ConsumerFactory<String, byte[]> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.consumerFactory = consumerFactory;
    }

    public void save(String sourceTopic, int sourcePartition,
                     Instant maxTimestamp, long durableOffset) throws Exception {
        Objects.requireNonNull(sourceTopic, "sourceTopic");
        Objects.requireNonNull(maxTimestamp, "maxTimestamp");

        String key = sourceTopic + ":" + sourcePartition;
        String value = maxTimestamp.toEpochMilli() + "," + durableOffset;
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                watermarkTopic, key, value.getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record).get(60, TimeUnit.SECONDS);
    }

    public synchronized Map<String, WatermarkState> loadAll() {
        refresh();
        return Map.copyOf(watermarks);
    }

    public synchronized Instant minimumMaxTimestamp(String sourceTopic) {
        refresh();
        List<PartitionInfo> sourcePartitions = watermarkConsumer.partitionsFor(sourceTopic);
        if (sourcePartitions == null || sourcePartitions.isEmpty()) {
            return null;
        }

        Instant minimum = null;
        for (PartitionInfo partition : sourcePartitions) {
            WatermarkState state = watermarks.get(sourceTopic + ":" + partition.partition());
            if (state == null) {
                return null;
            }
            if (minimum == null || state.maxTimestamp().isBefore(minimum)) {
                minimum = state.maxTimestamp();
            }
        }
        return minimum;
    }

    private void refresh() {
        initializeConsumer();
        long endOffset = watermarkConsumer.endOffsets(List.of(watermarkPartition)).get(watermarkPartition);

        while (watermarkConsumer.position(watermarkPartition) < endOffset) {
            ConsumerRecords<String, byte[]> records = watermarkConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, byte[]> record : records) {
                if (record.value() == null) {
                    watermarks.remove(record.key());
                } else {
                    watermarks.put(record.key(), parse(record.value()));
                }
            }
        }
    }

    private void initializeConsumer() {
        if (watermarkConsumer != null) {
            return;
        }
        watermarkPartition = new TopicPartition(watermarkTopic, 0);
        watermarkConsumer = consumerFactory.createConsumer();
        watermarkConsumer.assign(List.of(watermarkPartition));
        watermarkConsumer.seekToBeginning(List.of(watermarkPartition));
    }

    @PreDestroy
    public synchronized void close() {
        if (watermarkConsumer != null) {
            watermarkConsumer.close();
        }
    }

    private WatermarkState parse(byte[] value) {
        String[] fields = new String(value, StandardCharsets.UTF_8).split(",", 2);
        if (fields.length != 2) {
            throw new IllegalStateException("Invalid watermark value");
        }
        return new WatermarkState(
                Instant.ofEpochMilli(Long.parseLong(fields[0])),
                Long.parseLong(fields[1]));
    }

    public record WatermarkState(Instant maxTimestamp, long durableOffset) {
    }
}
