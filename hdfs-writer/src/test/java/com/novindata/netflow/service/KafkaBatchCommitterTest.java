package com.novindata.netflow.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Map;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaBatchCommitterTest {
    @Mock private DlqPublisher publisher;
    @Mock private KafkaWatermarkStore watermarkStore;
    @Mock private KafkaRecordTimeRouter timeRouter;
    @Mock private HdfsDirectoryManager directoryManager;
    @Mock private HdfsSessionManager session;
    @Mock private Acknowledgment acknowledgment;

    @InjectMocks private KafkaBatchCommitter committer;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(committer, "sourceTopic", "netflow");
    }

    @Test
    void forwardedRecordsDoNotCloseBufferedHdfsWriters() throws Exception {
        when(session.isThresholdReached()).thenReturn(false);
        when(session.hasBufferedRecords()).thenReturn(true);

        committer.commitAndAckIfNeeded(session, acknowledgment, true, 1);

        verify(session, never()).closeAndCommitAll();
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void forwardedOnlyPollIsAcknowledgedWithoutClosingHdfsWriters() throws Exception {
        Instant maxTimestamp = Instant.parse("2026-09-30T06:00:00Z");
        when(session.isThresholdReached()).thenReturn(false);
        when(session.hasBufferedRecords()).thenReturn(false);
        when(timeRouter.maxSeenKafkaTimes()).thenReturn(Map.of(2, maxTimestamp));
        when(timeRouter.nextOffsets()).thenReturn(Map.of(2, 101L));

        committer.commitAndAckIfNeeded(session, acknowledgment, true, 1);

        verify(session, never()).closeAndCommitAll();
        verify(watermarkStore).save("netflow", 2, maxTimestamp, 101L);
        verify(acknowledgment).acknowledge();
    }
}
