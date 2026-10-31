package com.novindata.netflow.integration;

import com.novindata.netflow.support.KafkaTestExtension;
import com.novindata.netflow.support.MiniDfsTestExtension;
import com.novindata.netflow.support.NetFlowTestData;
import com.novindata.netflow.service.KafkaWatermarkStore;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(KafkaToHdfsIT.TestTopics.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaToHdfsIT {

    private static final String TEST_SOURCE_IP = "10.99.0.7";
    private static final String COORDINATION_TOPIC = "netflow-watermark-coordination-test";
    private static final String SCHEMA_REGISTRY_URL = "mock://kafka-to-hdfs-it";
    private static final ZoneId ZONE_ID = ZoneId.of("Asia/Tehran");
    private static final DateTimeFormatter HOUR_KEY = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH");
    private static final String TEST_OUTPUT_DIRECTORY =
            "/netflow-integration-test/" + UUID.randomUUID();
    private static final KafkaAvroSerializer AVRO_SERIALIZER = createAvroSerializer();

    @RegisterExtension
    static final KafkaTestExtension KAFKA = new KafkaTestExtension();

    @RegisterExtension
    static final MiniDfsTestExtension MINI_DFS = new MiniDfsTestExtension();

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, byte[]> consumerFactory;

    @Autowired
    private KafkaWatermarkStore watermarkStore;

    @Value("${app.kafka.topic}")
    private String topic;

    @Value("${app.kafka.dlq-topic}")
    private String dlqTopic;

    @Value("${app.kafka.late-topic}")
    private String lateTopic;

    @Value("${app.kafka.watermark-topic}")
    private String watermarkTopic;

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.schema-registry.url", () -> SCHEMA_REGISTRY_URL);
        registry.add("app.hdfs.output-path",
                () -> MINI_DFS.getUri() + TEST_OUTPUT_DIRECTORY);
    }

    @AfterAll
    static void deleteTestOutputDirectory() throws IOException {
        MINI_DFS.getFileSystem().delete(new Path(TEST_OUTPUT_DIRECTORY), true);
    }

    @Test
    void startsKafkaContainer() {
        assertFalse(KAFKA.getBootstrapServers().isBlank());
    }

    @Test
    void createsSerializedNetFlowMessage() throws Exception {
        byte[] message = serialize(NetFlowTestData.validRecord());

        assertTrue(message.length > 0);
    }

    @Test
    @Order(3)
    void sendsInvalidMessageToDlq() throws Exception {
        byte[] invalidMessage = "not-an-avro-message".getBytes(UTF_8);
        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(
                KAFKA.getBootstrapServers(), "dlq-test-" + UUID.randomUUID(), "false");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        try (KafkaConsumer<String, byte[]> dlqConsumer = new KafkaConsumer<>(consumerProperties)) {
            dlqConsumer.subscribe(List.of(dlqTopic));
            send(0, "invalid-flow", targetHour().plusMinutes(30).toInstant(), invalidMessage);

            ConsumerRecord<String, byte[]> dlqRecord = KafkaTestUtils.getSingleRecord(
                    dlqConsumer, dlqTopic, Duration.ofSeconds(10));

            assertEquals("invalid-flow", dlqRecord.key());
            assertArrayEquals(invalidMessage, dlqRecord.value());
            assertNotNull(dlqRecord.headers().lastHeader("error-reason"));
        }
    }

    @Test
    @Order(4)
    void sendsNetFlowMessageAndCreatesParquetFile() throws Exception {
        GenericRecord record = NetFlowTestData.validRecord();
        record.put("IPV4_SRC_ADDR", TEST_SOURCE_IP);
        byte[] message = serialize(record);

        SendResult<String, byte[]> result = kafkaTemplate
                .send(topic, "flow-1", message)
                .get(10, TimeUnit.SECONDS);

        assertEquals(topic, result.getRecordMetadata().topic());
        assertTrue(result.getRecordMetadata().offset() >= 0);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertTrue(parquetFiles().stream()
                        .anyMatch(path -> containsExpectedRecord(path, result.getRecordMetadata()))));
    }

    @Test
    @Order(1)
    void routesByKafkaTimeWaitsForSlowPartitionAndPublishesLateRecord() throws Exception {
        ZonedDateTime targetHour = targetHour();
        Instant firstTimestamp = targetHour.plusMinutes(10).toInstant();
        Instant advancedTimestamp = targetHour.plusHours(2).plusMinutes(30).toInstant();
        byte[] message = serialize(NetFlowTestData.validRecord());

        send(0, "first-0", firstTimestamp, message);
        send(1, "first-1", firstTimestamp, message);
        awaitWatermark(0, firstTimestamp);
        awaitWatermark(1, firstTimestamp);

        Path processing = hourPath(targetHour, "processing");
        Path ready = hourPath(targetHour, "ready");
        assertTrue(MINI_DFS.getFileSystem().exists(processing));
        assertTrue(MINI_DFS.getFileSystem()
                .listStatus(processing, path -> path.getName().endsWith(".parquet")).length > 0);
        assertFalse(MINI_DFS.getFileSystem().exists(ready));

        send(0, "advanced-0", advancedTimestamp, message);
        awaitWatermark(0, advancedTimestamp);

        assertTrue(MINI_DFS.getFileSystem().exists(processing));
        assertFalse(MINI_DFS.getFileSystem().exists(ready));

        send(1, "advanced-1", advancedTimestamp, message);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertFalse(MINI_DFS.getFileSystem().exists(processing));
            assertTrue(MINI_DFS.getFileSystem().exists(ready));
        });

        try (KafkaConsumer<String, byte[]> lateConsumer = consumer(lateTopic)) {
            Instant lateTimestamp = targetHour.plusMinutes(20).toInstant();
            send(0, "late-flow", lateTimestamp, message);

            ConsumerRecord<String, byte[]> lateRecord = KafkaTestUtils.getSingleRecord(
                    lateConsumer, lateTopic, Duration.ofSeconds(10));

            assertEquals(targetHour.format(HOUR_KEY), lateRecord.key());
            assertArrayEquals(message, lateRecord.value());
            assertEquals("0", header(lateRecord, "source-partition"));
            assertEquals(String.valueOf(lateTimestamp.toEpochMilli()), header(lateRecord, "source-timestamp"));
            awaitWatermark(0, advancedTimestamp);
            assertFalse(MINI_DFS.getFileSystem().exists(processing));
            assertTrue(MINI_DFS.getFileSystem().exists(ready));
        }
    }

    @Test
    @Order(2)
    void sharesWatermarksAcrossReplicasAndRestoresThemWithFreshReader() throws Exception {
        Instant fastReplicaTimestamp = Instant.parse("2026-09-29T09:00:00Z");
        Instant slowReplicaTimestamp = Instant.parse("2026-09-29T08:00:00Z");
        var replicaOne = newWatermarkStore();
        var replicaTwo = newWatermarkStore();

        try {
            replicaOne.save(COORDINATION_TOPIC, 0, fastReplicaTimestamp, 10L);
            assertNull(replicaOne.minimumMaxTimestamp(COORDINATION_TOPIC));

            replicaTwo.save(COORDINATION_TOPIC, 1, slowReplicaTimestamp, 20L);
            assertEquals(slowReplicaTimestamp, replicaOne.minimumMaxTimestamp(COORDINATION_TOPIC));

            var restartedReplica = newWatermarkStore();
            try {
                assertEquals(slowReplicaTimestamp,
                        restartedReplica.minimumMaxTimestamp(COORDINATION_TOPIC));
            } finally {
                restartedReplica.close();
            }
        } finally {
            replicaOne.close();
            replicaTwo.close();
        }
    }

    private List<Path> parquetFiles() throws IOException {
        FileSystem fileSystem = MINI_DFS.getFileSystem();
        Path outputDirectory = new Path(TEST_OUTPUT_DIRECTORY);
        if (!fileSystem.exists(outputDirectory)) {
            return List.of();
        }

        List<Path> parquetFiles = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(outputDirectory, true);
        while (files.hasNext()) {
            Path path = files.next().getPath();
            if (path.getName().endsWith(".parquet")) {
                parquetFiles.add(path);
            }
        }
        return parquetFiles;
    }

    private boolean containsExpectedRecord(Path parquetFile, RecordMetadata expected) {
        try (ParquetReader<GenericRecord> reader =
                     AvroParquetReader.<GenericRecord>builder(
                                     HadoopInputFile.fromPath(
                                             parquetFile, MINI_DFS.getFileSystem().getConf()))
                             .withDataModel(GenericData.get())
                             .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                if (TEST_SOURCE_IP.contentEquals((CharSequence) record.get("IPV4_SRC_ADDR"))) {
                    return expected.topic().contentEquals((CharSequence) record.get("KAFKA_TOPIC"))
                            && expected.partition()
                            == ((Number) record.get("KAFKA_PARTITION")).intValue()
                            && expected.offset()
                            == ((Number) record.get("KAFKA_OFFSET")).longValue()
                            && expected.timestamp()
                            == ((Number) record.get("KAFKA_RECORD_TIMESTAMP")).longValue()
                            && "CREATE_TIME".contentEquals(
                            (CharSequence) record.get("KAFKA_TIMESTAMP_TYPE"));
                }
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    private void send(int partition, String key, Instant timestamp, byte[] value) throws Exception {
        kafkaTemplate.send(new ProducerRecord<>(
                        topic, partition, timestamp.toEpochMilli(), key, value))
                .get(10, TimeUnit.SECONDS);
    }

    private void awaitWatermark(int partition, Instant expectedTimestamp) {
        String key = topic + ":" + partition;
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Map<String, KafkaWatermarkStore.WatermarkState> watermarks = watermarkStore.loadAll();
            assertTrue(watermarks.containsKey(key));
            assertEquals(expectedTimestamp, watermarks.get(key).maxTimestamp());
        });
    }

    private Path hourPath(ZonedDateTime hour, String suffix) {
        return new Path(TEST_OUTPUT_DIRECTORY + "/" + hour.toLocalDate()
                + "/hour_" + String.format("%02d", hour.getHour()) + "_" + suffix);
    }

    private ZonedDateTime targetHour() {
        return ZonedDateTime.now(ZONE_ID)
                .minusDays(1)
                .withHour(10)
                .truncatedTo(ChronoUnit.HOURS);
    }

    private KafkaConsumer<String, byte[]> consumer(String destinationTopic) {
        Map<String, Object> properties = KafkaTestUtils.consumerProps(
                KAFKA.getBootstrapServers(), "test-" + UUID.randomUUID(), "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(destinationTopic));
        return consumer;
    }

    private byte[] serialize(GenericRecord record) {
        return AVRO_SERIALIZER.serialize(topic, record);
    }

    private static KafkaAvroSerializer createAvroSerializer() {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY_URL), false);
        return serializer;
    }

    private String header(ConsumerRecord<String, byte[]> record, String name) {
        return new String(record.headers().lastHeader(name).value(), UTF_8);
    }

    private KafkaWatermarkStore newWatermarkStore() {
        var store = new KafkaWatermarkStore(kafkaTemplate, consumerFactory);
        ReflectionTestUtils.setField(store, "watermarkTopic", watermarkTopic);
        return store;
    }

    @TestConfiguration
    static class TestTopics {
        @Bean
        NewTopic sourceTopic(@Value("${app.kafka.topic}") String topic) {
            return TopicBuilder.name(topic).partitions(2).replicas(1).build();
        }

        @Bean
        NewTopic coordinationTopic() {
            return TopicBuilder.name(COORDINATION_TOPIC).partitions(2).replicas(1).build();
        }
    }
}
