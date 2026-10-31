package com.novindata.netflow.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.novindata.netflow.core.avro.NetFlowV2OutputRecord;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HdfsSessionManagerTest {

    @Mock
    private HdfsParquetWriter hdfsWriter;
    @Mock
    private ParquetWriter<GenericRecord> hour10Writer;
    @Mock
    private ParquetWriter<GenericRecord> hour11Writer;
    @Mock
    private ParquetWriter<GenericRecord> hour10LateWriter;
    @Mock
    private ParquetWriter<GenericRecord> hour11LateWriter;

    private HdfsSessionManager manager;

    @BeforeEach
    void setUp() {
        manager = new HdfsSessionManager(hdfsWriter);
    }

    @Test
    void writesLateRecordsFromDifferentHoursToSeparateFolders() throws Exception {
        NetFlowV2OutputRecord hour10Record = new NetFlowV2OutputRecord();
        NetFlowV2OutputRecord hour11Record = new NetFlowV2OutputRecord();
        when(hdfsWriter.newBatchId()).thenReturn("batch-10-late", "batch-11-late");
        when(hdfsWriter.openWriter(eq("2026-09-29/hour_10_late"), eq("batch-10-late"), any(Schema.class)))
                .thenReturn(hour10LateWriter);
        when(hdfsWriter.openWriter(eq("2026-09-29/hour_11_late"), eq("batch-11-late"), any(Schema.class)))
                .thenReturn(hour11LateWriter);

        manager.writeLateRecord(hour10Record, time(10, 30));
        manager.writeLateRecord(hour11Record, time(11, 30));
        manager.closeAndCommitAll();

        verify(hour10LateWriter).write(hour10Record);
        verify(hour11LateWriter).write(hour11Record);
        verify(hdfsWriter).commit("2026-09-29/hour_10_late", "batch-10-late", 1);
        verify(hdfsWriter).commit("2026-09-29/hour_11_late", "batch-11-late", 1);
    }

    @Test
    void keepsOneWriterForEachKafkaHour() throws Exception {
        when(hdfsWriter.newBatchId()).thenReturn("batch-10", "batch-11");
        when(hdfsWriter.openWriter(eq("2026-09-29/hour_10_processing"), eq("batch-10"), any(Schema.class)))
                .thenReturn(hour10Writer);
        when(hdfsWriter.openWriter(eq("2026-09-29/hour_11_processing"), eq("batch-11"), any(Schema.class)))
                .thenReturn(hour11Writer);
        NetFlowV2OutputRecord firstHour10 = new NetFlowV2OutputRecord();
        NetFlowV2OutputRecord hour11 = new NetFlowV2OutputRecord();
        NetFlowV2OutputRecord secondHour10 = new NetFlowV2OutputRecord();

        manager.writeRecord(firstHour10, time(10, 59));
        manager.writeRecord(hour11, time(11, 0));
        manager.writeRecord(secondHour10, time(10, 58));
        manager.closeAndCommitAll();

        verify(hour10Writer).write(same(firstHour10));
        verify(hour10Writer).write(same(secondHour10));
        verify(hour11Writer).write(hour11);
        verify(hdfsWriter).commit("2026-09-29/hour_10_processing", "batch-10", 2);
        verify(hdfsWriter).commit("2026-09-29/hour_11_processing", "batch-11", 1);
    }

    private Instant time(int hour, int minute) {
        return ZonedDateTime.of(2026, 9, 29, hour, minute, 0, 0,
                ZoneId.of("Asia/Tehran")).toInstant();
    }
}
