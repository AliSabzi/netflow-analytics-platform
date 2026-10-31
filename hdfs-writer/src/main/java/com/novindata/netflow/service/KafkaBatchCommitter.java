package com.novindata.netflow.service;

import com.novindata.netflow.monitoring.PipelineMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Component
public class KafkaBatchCommitter {
    private static final Logger log = LoggerFactory.getLogger(KafkaBatchCommitter.class);
    private final DlqPublisher dlqPublisher;
    private final KafkaWatermarkStore watermarkStore;
    private final KafkaRecordTimeRouter timeRouter;
    private final HdfsDirectoryManager directoryManager;
    private long pendingRecordCount;

    @Value("${app.kafka.topic}")
    private String sourceTopic;

    public KafkaBatchCommitter(DlqPublisher dlqPublisher, KafkaWatermarkStore watermarkStore,
                               KafkaRecordTimeRouter timeRouter,
                               HdfsDirectoryManager directoryManager) {
        this.dlqPublisher = dlqPublisher;
        this.watermarkStore = watermarkStore;
        this.timeRouter = timeRouter;
        this.directoryManager = directoryManager;
    }

    public void handleDlq(List<ConsumerRecord<String, byte[]>> fails, List<String> reasons, HdfsSessionManager session) {
        if (!fails.isEmpty()) {
            try {
                dlqPublisher.publish(fails, reasons);
            } catch (Exception dlqEx) {
                pendingRecordCount = 0;
                PipelineMetrics.recordError("dlq");
                log.error("CRITICAL: Failed to publish to DLQ. Aborting HDFS file.", dlqEx);
                session.abortAll();
                throw new RuntimeException("Batch processing failed due to DLQ Kafka error", dlqEx);
            }
        }
    }

    public void handleLate(List<ConsumerRecord<String, byte[]>> lateRecords, HdfsSessionManager session) {
        if (!lateRecords.isEmpty()) {
            try {
                dlqPublisher.publishLate(lateRecords);
            } catch (Exception lateEx) {
                pendingRecordCount = 0;
                PipelineMetrics.recordError("late");
                log.error("CRITICAL: Failed to publish to late topic. Aborting HDFS file.", lateEx);
                session.abortAll();
                throw new RuntimeException("Batch processing failed due to late-topic Kafka error", lateEx);
            }
        }
    }

    public void commitAndAckIfNeeded(HdfsSessionManager session, Acknowledgment ack,
                                     boolean hasForwardedRecords, int recordCount) throws Exception {
        long recordsToCommit = pendingRecordCount + recordCount;
        boolean thresholdReached = session.isThresholdReached();
        boolean onlyForwardedRecords = hasForwardedRecords && !session.hasBufferedRecords();

        if (thresholdReached || onlyForwardedRecords) {
            try {
                if (session.hasBufferedRecords()) {
                    session.closeAndCommitAll();
                }
                saveWatermarks();
                closeCompletedHours();
                for (int attempt = 1; attempt <= 3; attempt++) {
                    try {
                        ack.acknowledge();
                        PipelineMetrics.recordKafkaCommitted(recordsToCommit);
                        pendingRecordCount = 0;
                        break;
                    } catch (Exception ackEx) {
                        if (attempt == 3) {
                            PipelineMetrics.recordError("kafka_ack");
                            log.error("CRITICAL: Failed to ACK Kafka offset after 3 attempts.", ackEx);
                            session.abortAll();
                            throw new RuntimeException("Failed to commit Kafka offset", ackEx);
                        }
                        PipelineMetrics.recordRetry("kafka_ack");
                        log.warn("Kafka ACK failed on attempt {}. Retrying in 1 second...", attempt);
                        try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    }
                }
            } catch (Exception e) {
                pendingRecordCount = 0;
                throw e;
            }
        } else {
            pendingRecordCount = recordsToCommit;
        }
    }

    private void saveWatermarks() throws Exception {
        Map<Integer, Instant> timestamps = timeRouter.maxSeenKafkaTimes();
        Map<Integer, Long> offsets = timeRouter.nextOffsets();
        for (Map.Entry<Integer, Instant> entry : timestamps.entrySet()) {
            int partition = entry.getKey();
            watermarkStore.save(sourceTopic, partition, entry.getValue(), offsets.get(partition));
        }
    }

    private void closeCompletedHours() throws Exception {
        Instant minimumMaxTimestamp = watermarkStore.minimumMaxTimestamp(sourceTopic);
        if (minimumMaxTimestamp == null) {
            log.info("Not all source partitions have a persisted watermark; no HDFS hour was closed");
            return;
        }

        Instant globalWatermark = timeRouter.watermarkFor(minimumMaxTimestamp);
        directoryManager.markHoursReadyBefore(globalWatermark);
        log.info("Finalized HDFS hours ending at or before global watermark {}", globalWatermark);
    }

    public void clearPendingRecords() {
        pendingRecordCount = 0;
    }
}
