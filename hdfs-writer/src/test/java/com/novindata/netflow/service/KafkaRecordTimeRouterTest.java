package com.novindata.netflow.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import static com.novindata.netflow.service.KafkaRecordTimeRouter.Decision.ACTIVE_HOUR;
import static com.novindata.netflow.service.KafkaRecordTimeRouter.Decision.INVALID_FUTURE;
import static com.novindata.netflow.service.KafkaRecordTimeRouter.Decision.LATE;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaRecordTimeRouterTest {

    private final KafkaRecordTimeRouter router = new KafkaRecordTimeRouter(
            Duration.ofHours(1), Duration.ofMinutes(5), ZoneId.of("Asia/Tehran"));

    @Test
    void routesEachPartitionUsingItsOwnLargestTimestamp() {
        List<ConsumerRecord<String, byte[]>> records = List.of(
                record(0, 9, 59),
                record(1, 9, 59),
                record(1, 10, 20),
                record(0, 11, 30));

        KafkaRecordTimeRouter.PollContext context = router.preparePoll(
                records, time(2026, 9, 29, 11, 30));
        List<KafkaRecordTimeRouter.Decision> decisions = records.stream()
                .map(record -> router.route(record, context))
                .toList();

        assertEquals(List.of(LATE, ACTIVE_HOUR, ACTIVE_HOUR, ACTIVE_HOUR), decisions);
        assertEquals(time(2026, 9, 29, 10, 30), router.watermark(0).orElseThrow());
        assertEquals(time(2026, 9, 29, 9, 20), router.watermark(1).orElseThrow());
    }

    @Test
    void keepsLargestKafkaTimestampAcrossPolls() {
        router.preparePoll(List.of(record(0, 11, 30)), time(2026, 9, 29, 11, 30));

        ConsumerRecord<String, byte[]> record = record(0, 9, 59);
        KafkaRecordTimeRouter.PollContext context = router.preparePoll(
                List.of(record), time(2026, 9, 29, 11, 31));

        assertEquals(LATE, router.route(record, context));
    }

    @Test
    void rejectsFutureTimestampWithoutAdvancingWatermark() {
        ConsumerRecord<String, byte[]> validRecord = record(0, 11, 30);
        ConsumerRecord<String, byte[]> invalidRecord = record(0, 12, 0);

        KafkaRecordTimeRouter.PollContext context = router.preparePoll(
                List.of(validRecord, invalidRecord), time(2026, 9, 29, 11, 30));

        assertEquals(ACTIVE_HOUR, router.route(validRecord, context));
        assertEquals(INVALID_FUTURE, router.route(invalidRecord, context));
        assertEquals(Instant.ofEpochMilli(validRecord.timestamp()), router.maxSeenKafkaTime(0).orElseThrow());
    }

    @Test
    void restoresAndForgetsPartitionState() {
        Instant restoredTimestamp = time(2026, 9, 29, 11, 30);

        router.restore(2, restoredTimestamp, 42L);
        assertEquals(restoredTimestamp, router.maxSeenKafkaTime(2).orElseThrow());
        assertEquals(42L, router.nextOffsets().get(2));

        router.forget(2);
        assertEquals(Optional.empty(), router.maxSeenKafkaTime(2));
    }

    private ConsumerRecord<String, byte[]> record(int partition, int hour, int minute) {
        return new ConsumerRecord<>("netflow", partition, 0L,
                time(2026, 9, 29, hour, minute).toEpochMilli(), TimestampType.CREATE_TIME,
                0, 0, "key", new byte[0], new RecordHeaders(), Optional.empty());
    }

    private Instant time(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0,
                        ZoneId.of("Asia/Tehran"))
                .toInstant();
    }

}
