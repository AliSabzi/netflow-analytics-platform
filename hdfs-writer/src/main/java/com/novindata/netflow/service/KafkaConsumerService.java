package com.novindata.netflow.service;

import com.novindata.netflow.monitoring.PipelineMetrics;
import org.apache.avro.generic.GenericRecord;
import com.novindata.netflow.core.validation.NetFlowValidationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Service
@Profile("!late-job")
public class KafkaConsumerService implements ConsumerSeekAware {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final HdfsSessionManager sessionManager;
    private final KafkaBatchCommitter batchCommitter;
    private final NetFlowDecoder decoderService;
    private final KafkaRecordTimeRouter timeRouter;
    private final KafkaWatermarkStore watermarkStore;

    public KafkaConsumerService(HdfsSessionManager sessionManager, KafkaBatchCommitter batchCommitter,
                                NetFlowDecoder decoderService,
                                KafkaRecordTimeRouter timeRouter, KafkaWatermarkStore watermarkStore,
                                @Value("${app.kafka.topic}") String topic) {
        this.sessionManager = sessionManager;
        this.batchCommitter = batchCommitter;
        this.decoderService = decoderService;
        this.timeRouter = timeRouter;
        this.watermarkStore = watermarkStore;
        PipelineMetrics.configureTopic(topic);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
                                     ConsumerSeekCallback callback) {
        Map<String, KafkaWatermarkStore.WatermarkState> watermarks = watermarkStore.loadAll();
        for (TopicPartition partition : assignments.keySet()) {
            timeRouter.forget(partition.partition());
            KafkaWatermarkStore.WatermarkState state = watermarks.get(
                    partition.topic() + ":" + partition.partition());
            if (state != null && state.durableOffset() <= assignments.get(partition)) {
                timeRouter.restore(partition.partition(), state.maxTimestamp(), state.durableOffset());
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            timeRouter.forget(partition.partition());
        }
    }

    @KafkaListener(topics = "${app.kafka.topic}")
    public synchronized void consume(List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack) throws Exception {
        long batchStartedAt = System.nanoTime();
        PipelineMetrics.recordReceived(records.size());
        log.info("Received poll of {} records from Kafka", records.size());
        KafkaRecordTimeRouter.PollContext pollContext = timeRouter.preparePoll(records, Instant.now());


        List<ConsumerRecord<String, byte[]>> failedRecords = new ArrayList<>();
        List<String> failedReasons = new ArrayList<>();
        List<ConsumerRecord<String, byte[]>> lateRecords = new ArrayList<>();
        int pollSuccessCount = 0, pollFailedValidationCount = 0, pollFailedDecodeCount = 0;

        try {
            try {
                for (ConsumerRecord<String, byte[]> record : records) {
                    GenericRecord decoded;
                    try {
                        decoded = decoderService.decodeAndValidate(record.topic(),record.value());
                    } catch (NetFlowValidationException e) {
                        pollFailedValidationCount++;
                        failedRecords.add(record);
                        failedReasons.add(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                        continue;
                    } catch (Exception e) {
                        pollFailedDecodeCount++;
                        failedRecords.add(record);
                        failedReasons.add(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                        continue;
                    }

                    KafkaRecordTimeRouter.Decision decision = timeRouter.route(record, pollContext);
                    if (decision == KafkaRecordTimeRouter.Decision.LATE) {
                        lateRecords.add(record);
                        continue;
                    }
                    if (decision == KafkaRecordTimeRouter.Decision.INVALID_FUTURE) {
                        failedRecords.add(record);
                        failedReasons.add("Kafka timestamp is too far in the future");
                        pollFailedValidationCount++;
                        continue;
                    }
                    KafkaRecordMetadata.from(record).writeTo(decoded);
                    sessionManager.writeRecord(decoded, Instant.ofEpochMilli(record.timestamp()));
                    pollSuccessCount++;
                }
            } catch (Exception e) {
                log.error("IO error while writing Parquet files", e);
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                if (e instanceof IOException) throw (IOException) e;
                throw new RuntimeException(e);
            }

            batchCommitter.handleDlq(failedRecords, failedReasons, sessionManager);
            batchCommitter.handleLate(lateRecords, sessionManager);
            PipelineMetrics.recordDlq("validation", pollFailedValidationCount);
            PipelineMetrics.recordDlq("decode", pollFailedDecodeCount);
            boolean recordsForwarded = !failedRecords.isEmpty() || !lateRecords.isEmpty();
            batchCommitter.commitAndAckIfNeeded(sessionManager, ack, recordsForwarded, records.size());

            log.info("Batch processing finished: success={}, late={}, validationFailed={}, decodeFailed={}",
                    pollSuccessCount, lateRecords.size(), pollFailedValidationCount, pollFailedDecodeCount);

        } catch (IOException e) {
            PipelineMetrics.recordError("hdfs");
            log.error("CRITICAL: HDFS Writing failed. Aborting current file.", e);
            sessionManager.abortAll();
            batchCommitter.clearPendingRecords();
            throw new RuntimeException("Batch processing failed due to HDFS error", e);
        } finally {
            PipelineMetrics.recordBatchDuration(
                    (System.nanoTime() - batchStartedAt) / 1_000_000_000.0);
        }
    }
}
