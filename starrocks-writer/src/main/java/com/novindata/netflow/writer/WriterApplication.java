package com.novindata.netflow.writer;

import com.novindata.netflow.writer.config.WriterConfig;
import com.novindata.netflow.writer.config.WriterConfigLoader;
import com.novindata.netflow.writer.monitoring.StarRocksMetrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class WriterApplication implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriterApplication.class);

    private final String sourceTopic;
    private final String dlqTopic;
    private final Duration pollTimeout;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final DlqPublisher dlqPublisher;
    private final PartitionBatchProcessor batchProcessor;
    private final SchemaInitializer schemaInitializer;
    private volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        int metricsPort = Integer.parseInt(System.getenv().getOrDefault("METRICS_PORT", "9402"));
        StarRocksMetrics.start(metricsPort);

        try {
            WriterConfig config = args.length == 0
                    ? WriterConfigLoader.loadFromClasspath("application.properties")
                    : WriterConfigLoader.load(Path.of(args[0]));

            try (WriterApplication writer = new WriterApplication(config)) {
                Runtime.getRuntime().addShutdownHook(new Thread(writer::stop));
                writer.run();
            }
        } finally {
            StarRocksMetrics.stop();
        }
    }

    WriterApplication(WriterConfig config) throws Exception {
        WriterConfig.KafkaConfig kafka = config.kafka();
        sourceTopic = kafka.sourceTopic();
        StarRocksMetrics.configureTopic(sourceTopic);
        dlqTopic = kafka.dlqTopic();
        pollTimeout = Duration.ofMillis(kafka.pollTimeoutMs());
        consumer = new KafkaConsumer<>(consumerSettings(kafka));
        dlqPublisher = new DlqPublisher(kafka);

        WriterConfig.StarRocksConfig starRocksConfig = config.starRocks();
        StarRocksClient starRocks = new StarRocksClient(
                starRocksConfig.feHttpUrl(),
                starRocksConfig.database(),
                starRocksConfig.table(),
                starRocksConfig.user(),
                starRocksConfig.password(),
                starRocksConfig.loadMaxAttempts(),
                starRocksConfig.loadRetryBackoffMs());
        batchProcessor = new PartitionBatchProcessor(
                new NetflowConverter(), starRocks, dlqPublisher);
        schemaInitializer = new SchemaInitializer(starRocksConfig);
    }

    public void run() throws Exception {
        schemaInitializer.initialize();
        consumer.subscribe(List.of(sourceTopic));
        StarRocksMetrics.markReady();
        LOGGER.info("writer started: source={}, dlq={}", sourceTopic, dlqTopic);

        while (running) {
            ConsumerRecords<byte[], byte[]> polled;
            try {
                polled = consumer.poll(pollTimeout);
            } catch (RuntimeException pollFailure) {
                StarRocksMetrics.recordError("kafka_poll");
                throw pollFailure;
            }
            try {
                process(polled);
            } catch (Exception e) {
                LOGGER.error("Critical error during polling or batch processing. Backing off for 20 seconds...", e);
                try {
                    Thread.sleep(20_000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Writer thread interrupted during backoff sleep.");
                    break;
                }
            }        }
    }

    void process(ConsumerRecords<byte[], byte[]> polled) throws Exception {
        StarRocksMetrics.recordReceived(polled.count());
        for (TopicPartition partition : polled.partitions()) {
            long batchStartedAt = System.nanoTime();
            List<ConsumerRecord<byte[], byte[]>> records = polled.records(partition);
            try {
                batchProcessor.process(records);
                long nextOffset = records.get(records.size() - 1).offset() + 1;
                try {
                    consumer.commitSync(Map.of(partition, new OffsetAndMetadata(nextOffset)));
                } catch (RuntimeException commitFailure) {
                    StarRocksMetrics.recordError("kafka_commit");
                    throw commitFailure;
                }
                StarRocksMetrics.recordPartitionCommitted(records.size());
                LOGGER.info("committed: partition={}, nextOffset={}",
                        partition.partition(), nextOffset);
            } finally {
                StarRocksMetrics.recordPartitionBatchDuration(
                        (System.nanoTime() - batchStartedAt) / 1_000_000_000.0);
            }
        }
    }

    public void stop() {
        running = false;
        StarRocksMetrics.markNotReady();
    }

    @Override
    public void close() {
        StarRocksMetrics.markNotReady();
        dlqPublisher.close();
        consumer.close();
    }

    static Properties consumerSettings(WriterConfig.KafkaConfig kafka) {
        Properties result = new Properties();
        result.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        result.put(ConsumerConfig.GROUP_ID_CONFIG, kafka.consumerGroup());
        result.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        result.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        result.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        result.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafka.autoOffsetReset());
        result.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafka.maxPollRecords());
        result.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafka.maxPollIntervalMs());
        result.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                kafka.maxPartitionFetchBytes());
        return result;
    }
}
