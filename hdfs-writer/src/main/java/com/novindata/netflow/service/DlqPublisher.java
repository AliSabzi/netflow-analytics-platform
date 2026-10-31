package com.novindata.netflow.service;

import com.novindata.netflow.monitoring.PipelineMetrics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class DlqPublisher {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);
    private static final ZoneId ZONE_ID = ZoneId.of("Asia/Tehran");
    private static final DateTimeFormatter HOUR_KEY_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH");

    private static final String HEADER_ERROR_REASON = "error-reason";

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    @Value("${app.kafka.dlq-topic}")
    private String dlqTopic;

    @Value("${app.kafka.late-topic}")
    private String lateTopic;

    public DlqPublisher(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(List<ConsumerRecord<String, byte[]>> failedRecords, List<String> reasons) throws Exception {
        publishRecords(failedRecords, reasons, dlqTopic, "DLQ", "dlq", false);
    }

    public void publishLate(List<ConsumerRecord<String, byte[]>> lateRecords) throws Exception {
        publishRecords(lateRecords, null, lateTopic, "late topic", "late", true);
    }

    private void publishRecords(List<ConsumerRecord<String, byte[]>> records, List<String> reasons,
                                String destinationTopic, String destinationName, String metricName,
                                boolean useEventHourKey) throws Exception {
        if (records == null || records.isEmpty()) {
            return;
        }

        int maxRetries = 5;
        long waitTimeMs = 2000;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                List<CompletableFuture<?>> futures = new ArrayList<>();

                for (int i = 0; i < records.size(); i++) {
                    ConsumerRecord<String, byte[]> sourceRecord = records.get(i);
                    String key = useEventHourKey ? eventHourKey(sourceRecord.timestamp()) : sourceRecord.key();

                    ProducerRecord<String, byte[]> targetRecord = new ProducerRecord<>(
                            destinationTopic, null, key, sourceRecord.value());

                    addSourceHeaders(targetRecord, sourceRecord);
                    if (!useEventHourKey) {
                        String reason = (reasons != null && i < reasons.size()) ? reasons.get(i) : "Unknown error";
                        targetRecord.headers().add(new RecordHeader(
                                HEADER_ERROR_REASON, reason.getBytes(StandardCharsets.UTF_8)));
                    }

                    futures.add(kafkaTemplate.send(targetRecord));
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(60, TimeUnit.SECONDS);

                log.info("Successfully sent {} records to {} on attempt {}", records.size(), destinationName, attempt);
                return;

            } catch (Exception e) {
                if (attempt == maxRetries) {
                    log.error("Failed to send records to {} after {} attempts. Giving up.",
                            destinationName, maxRetries, e);
                    throw e;
                }

                PipelineMetrics.recordRetry(metricName);
                log.warn("Attempt {} to send to {} failed. Retrying in {} ms... Error: {}",
                        attempt, destinationName, waitTimeMs, e.getMessage());
                Thread.sleep(waitTimeMs);
            }
        }
    }

    private void addSourceHeaders(ProducerRecord<String, byte[]> targetRecord,
                                  ConsumerRecord<String, byte[]> sourceRecord) {
        targetRecord.headers()
                .add(header(KafkaRecordMetadata.SOURCE_TOPIC, sourceRecord.topic()))
                .add(header(KafkaRecordMetadata.SOURCE_PARTITION, sourceRecord.partition()))
                .add(header(KafkaRecordMetadata.SOURCE_OFFSET, sourceRecord.offset()))
                .add(header(KafkaRecordMetadata.SOURCE_TIMESTAMP, sourceRecord.timestamp()))
                .add(header(KafkaRecordMetadata.SOURCE_TIMESTAMP_TYPE, sourceRecord.timestampType().name()));
    }

    private RecordHeader header(String name, Object value) {
        return new RecordHeader(name, String.valueOf(value).getBytes(StandardCharsets.UTF_8));
    }

    private String eventHourKey(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(ZONE_ID).format(HOUR_KEY_FORMATTER);
    }
}
