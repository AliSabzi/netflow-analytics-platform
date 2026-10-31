package com.novindata.netflow.writer.config;

public record WriterConfig(
        KafkaConfig kafka,
        StarRocksConfig starRocks) {

    public record KafkaConfig(
            String bootstrapServers,
            String sourceTopic,
            String consumerGroup,
            String dlqTopic,
            int maxPollRecords,
            int pollTimeoutMs,
            int maxPollIntervalMs,
            int maxPartitionFetchBytes,
            String autoOffsetReset) {
    }

    public record StarRocksConfig(
            String feHttpUrl,
            String feQueryUrl,
            String database,
            String table,
            String user,
            String password,
            int loadMaxAttempts,
            int loadRetryBackoffMs) {
    }
}
