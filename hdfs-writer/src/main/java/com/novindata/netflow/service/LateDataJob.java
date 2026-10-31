package com.novindata.netflow.service;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Profile("late-job")
public class LateDataJob {
    private static final Logger log = LoggerFactory.getLogger(LateDataJob.class);

    private final ConsumerFactory<String, byte[]> consumerFactory;
    private final NetFlowDecoder decoder;
    private final HdfsSessionManager sessionManager;

    private final String lateTopic;
    private final String consumerGroup;

    public LateDataJob(ConsumerFactory<String, byte[]> consumerFactory,
                       NetFlowDecoder decoder,
                       HdfsSessionManager sessionManager,
                       @Value("${app.kafka.late-topic}") String lateTopic,
                       @Value("${app.kafka.late-consumer-group}") String consumerGroup) {
        this.consumerFactory = consumerFactory;
        this.decoder = decoder;
        this.sessionManager = sessionManager;
        this.lateTopic = lateTopic;
        this.consumerGroup = consumerGroup;
    }

    public void run() throws Exception {
        try (Consumer<String, byte[]> consumer = consumerFactory.createConsumer(consumerGroup, null)) {
            List<TopicPartition> partitions = consumer.partitionsFor(lateTopic).stream()
                    .map(PartitionInfo::partition)
                    .map(partition -> new TopicPartition(lateTopic, partition))
                    .toList();
            consumer.assign(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            log.info("Late-data job will process {} partition(s) up to the startup offsets",
                    partitions.size());

            while (!reachedEnd(consumer, endOffsets)) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                process(records, endOffsets, consumer);
            }
            log.info("Late-data job finished");
        } catch (Exception e) {
            sessionManager.abortAll();
            throw e;
        }
    }

    private void process(ConsumerRecords<String, byte[]> records,
                         Map<TopicPartition, Long> endOffsets,
                         Consumer<String, byte[]> consumer) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        for (ConsumerRecord<String, byte[]> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            if (record.offset() >= endOffsets.get(partition)) {
                continue;
            }

            GenericRecord decoded = decoder.decodeAndValidate(record.topic(),record.value());
            KafkaRecordMetadata metadata = KafkaRecordMetadata.fromSourceHeaders(record);
            metadata.writeTo(decoded);
            sessionManager.writeLateRecord(decoded, metadata.timestamp());
            offsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
        }

        if (!offsets.isEmpty()) {
            sessionManager.closeAndCommitAll();
            consumer.commitSync(offsets);
            log.info("Committed {} late-topic partition offset(s)", offsets.size());
        }
    }

    private boolean reachedEnd(Consumer<String, byte[]> consumer,
                               Map<TopicPartition, Long> endOffsets) {
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            if (consumer.position(entry.getKey()) < entry.getValue()) {
                return false;
            }
        }
        return true;
    }
}
