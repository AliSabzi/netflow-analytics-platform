package com.novindata.netflow.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Component
public final class KafkaRecordTimeRouter {

    public enum Decision {
        ACTIVE_HOUR,
        LATE,
        INVALID_FUTURE
    }

    public record PollContext(Instant futureLimit, Map<Integer, Instant> watermarks) {
    }

    private final Duration allowedLateness;
    private final Duration allowedFutureSkew;
    private final ZoneId zoneId;
    private final Map<Integer, Instant> maxSeenKafkaTimeByPartition = new HashMap<>();
    private final Map<Integer, Long> nextOffsetByPartition = new HashMap<>();

    @Autowired
    public KafkaRecordTimeRouter(
            @Value("${app.hdfs.allowed-lateness-minutes:60}") long allowedLatenessMinutes) {
        this(Duration.ofMinutes(allowedLatenessMinutes), Duration.ofMinutes(5), ZoneId.of("Asia/Tehran"));
    }

    public KafkaRecordTimeRouter(Duration allowedLateness, Duration allowedFutureSkew, ZoneId zoneId) {
        this.allowedLateness = requireNonNegative(allowedLateness, "allowedLateness");
        this.allowedFutureSkew = requireNonNegative(allowedFutureSkew, "allowedFutureSkew");
        this.zoneId = Objects.requireNonNull(zoneId, "zoneId");
    }

    public synchronized PollContext preparePoll(List<ConsumerRecord<String, byte[]>> records,
                                                Instant currentTime) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(currentTime, "currentTime");

        Instant futureLimit = currentTime.plus(allowedFutureSkew);
        for (ConsumerRecord<String, byte[]> record : records) {
            int partition = record.partition();
            nextOffsetByPartition.merge(partition, record.offset() + 1, Math::max);
            Instant timestamp = Instant.ofEpochMilli(record.timestamp());
            if (!timestamp.isAfter(futureLimit)) {
                Instant currentMax = maxSeenKafkaTimeByPartition.get(partition);
                if (currentMax == null || timestamp.isAfter(currentMax)) {
                    maxSeenKafkaTimeByPartition.put(partition, timestamp);
                }
            }
        }

        Map<Integer, Instant> watermarks = new HashMap<>();
        for (Map.Entry<Integer, Instant> entry : maxSeenKafkaTimeByPartition.entrySet()) {
            int partition = entry.getKey();
            Instant maxSeen = entry.getValue();
            watermarks.put(partition, maxSeen.minus(allowedLateness));
        }
        return new PollContext(futureLimit, Map.copyOf(watermarks));
    }

    public synchronized void restore(int partition, Instant maxTimestamp, long nextOffset) {
        maxSeenKafkaTimeByPartition.put(partition, Objects.requireNonNull(maxTimestamp, "maxTimestamp"));
        nextOffsetByPartition.put(partition, nextOffset);
    }

    public synchronized void forget(int partition) {
        maxSeenKafkaTimeByPartition.remove(partition);
        nextOffsetByPartition.remove(partition);
    }

    public synchronized Map<Integer, Instant> maxSeenKafkaTimes() {
        return Map.copyOf(maxSeenKafkaTimeByPartition);
    }

    public synchronized Map<Integer, Long> nextOffsets() {
        return Map.copyOf(nextOffsetByPartition);
    }

    public Decision route(ConsumerRecord<String, byte[]> record, PollContext context) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(context, "context");

        Instant timestamp = Instant.ofEpochMilli(record.timestamp());
        if (timestamp.isAfter(context.futureLimit())) {
            return Decision.INVALID_FUTURE;
        }
        Instant partitionWatermark = context.watermarks().get(record.partition());
        if (partitionWatermark != null && isClosedHour(timestamp, partitionWatermark)) {
            return Decision.LATE;
        }
        return Decision.ACTIVE_HOUR;
    }

    public synchronized Optional<Instant> maxSeenKafkaTime(int partition) {
        return Optional.ofNullable(maxSeenKafkaTimeByPartition.get(partition));
    }

    public synchronized Optional<Instant> watermark(int partition) {
        return maxSeenKafkaTime(partition).map(timestamp -> timestamp.minus(allowedLateness));
    }

    public Instant watermarkFor(Instant maxTimestamp) {
        return Objects.requireNonNull(maxTimestamp, "maxTimestamp").minus(allowedLateness);
    }

    private boolean isClosedHour(Instant timestamp, Instant currentWatermark) {
        Instant hourEnd = timestamp.atZone(zoneId)
                .truncatedTo(ChronoUnit.HOURS)
                .plusHours(1)
                .toInstant();
        return !hourEnd.isAfter(currentWatermark);
    }

    private static Duration requireNonNegative(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return duration;
    }
}
