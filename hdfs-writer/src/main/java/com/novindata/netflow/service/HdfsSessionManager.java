package com.novindata.netflow.service;

import com.novindata.netflow.monitoring.PipelineMetrics;
import jakarta.annotation.PreDestroy;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import org.apache.avro.Schema;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class HdfsSessionManager {
    private static final Logger log = LoggerFactory.getLogger(HdfsSessionManager.class);
    private static final ZoneId ZONE_ID = ZoneId.of("Asia/Tehran");
    private final HdfsParquetWriter hdfsWriter;
    private final Map<String, HourSession> sessions = new LinkedHashMap<>();
    private final List<HourSession> closedSessions = new ArrayList<>();
    private Schema writerSchema;

    @Value("${app.hdfs.flush-threshold-records}")
    private int flushThresholdRecords;

    @Value("${app.hdfs.flush-threshold-seconds:180}")
    private int flushThresholdSeconds;

    public HdfsSessionManager(HdfsParquetWriter hdfsWriter) {
        this.hdfsWriter = hdfsWriter;
    }

    public void writeRecord(GenericRecord record, Instant kafkaTimestamp) throws IOException {
        writeRecord(record, kafkaTimestamp, "processing");
    }

    public void writeLateRecord(GenericRecord record, Instant kafkaTimestamp) throws IOException {
        writeRecord(record, kafkaTimestamp, "late");
    }

    private void writeRecord(GenericRecord record, Instant kafkaTimestamp,
                             String folderSuffix) throws IOException {

        if (writerSchema == null) {

            writerSchema = record.getSchema();

        } else if (!writerSchema.equals(record.getSchema())) {
            closeActiveSessions();
            writerSchema = record.getSchema();
        }
        String hourPath = getHourPath(kafkaTimestamp, folderSuffix);
        HourSession session = sessions.get(hourPath);
        if (session == null) {
            session = openSession(hourPath);
            sessions.put(hourPath, session);
        }

        session.writer.write(record);
        session.recordCount++;
        PipelineMetrics.recordWritten(totalBufferedRecords());
    }

    private HourSession openSession(String hourPath) throws IOException {
        String batchId = hdfsWriter.newBatchId();
        ParquetWriter<GenericRecord> writer = hdfsWriter.openWriter(hourPath, batchId,writerSchema);
        log.info("Opened new HDFS file {} for {}", batchId, hourPath);
        return new HourSession(hourPath, batchId, writer, Instant.now());
    }

    private void closeActiveSessions() throws IOException {
        for (HourSession session : sessions.values()) {
            session.writer.close();
            closedSessions.add(session);
        }
        sessions.clear();
    }

    public void closeAndCommitAll() throws IOException {
        closeActiveSessions();
        for (HourSession session : closedSessions) {
            hdfsWriter.commit(session.hourPath, session.batchId, session.recordCount);
            PipelineMetrics.recordFileCommitted(session.recordCount);
            log.info("Committed file {} with {} records", session.batchId, session.recordCount);
        }
        closedSessions.clear();
    }

    public void abortAll() {
        for (HourSession session : sessions.values()) {
            try {
                session.writer.close();
                hdfsWriter.abort(session.hourPath, session.batchId);
            } catch (Exception e) {
                log.error("Failed to abort file {} properly", session.batchId, e);
            }
        }
        for (HourSession session : closedSessions) {

            hdfsWriter.abort(
                    session.hourPath,
                    session.batchId
            );
        }
        PipelineMetrics.clearBuffered();
        sessions.clear();
        closedSessions.clear();
    }

    public boolean hasBufferedRecords() {
        return !sessions.isEmpty() || !closedSessions.isEmpty();
    }

    @PreDestroy
    public void onShutdown() {
        if (hasBufferedRecords()) {
            log.info("Shutdown detected. aborting partial files...");
            abortAll();
        }
    }

    public boolean isThresholdReached() {
        if (totalBufferedRecords() >= flushThresholdRecords) {
            return true;
        }
        for (HourSession session : sessions.values()) {
            if (Duration.between(session.openTime, Instant.now())
                    .getSeconds() >= flushThresholdSeconds) {
                return true;
            }
        }
        for (HourSession session : closedSessions) {
            if (Duration.between(session.openTime, Instant.now())
                    .getSeconds() >= flushThresholdSeconds) {
                return true;
            }
        }
        return false;
    }

    public String getHourPath(Instant timestamp) {
        return getHourPath(timestamp, "processing");
    }

    private String getHourPath(Instant timestamp, String folderSuffix) {
        var time = timestamp.atZone(ZONE_ID);
        return String.format("%s/hour_%02d_%s", time.toLocalDate(), time.getHour(), folderSuffix);
    }

    private int totalBufferedRecords() {
        int active = sessions.values().stream().mapToInt(session -> session.recordCount).sum();
        int closed = closedSessions.stream().mapToInt(session -> session.recordCount).sum();
        return active + closed;
    }

    private static final class HourSession {
        private final String hourPath;
        private final String batchId;
        private final ParquetWriter<GenericRecord> writer;
        private final Instant openTime;
        private int recordCount;

        private HourSession(String hourPath, String batchId,
                            ParquetWriter<GenericRecord> writer, Instant openTime) {
            this.hourPath = hourPath;
            this.batchId = batchId;
            this.writer = writer;
            this.openTime = openTime;
        }
    }
}
