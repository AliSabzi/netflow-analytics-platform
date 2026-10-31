package com.novindata.netflow.writer;

import com.novindata.netflow.writer.config.WriterConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.novindata.netflow.support.KafkaTestExtension;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import com.novindata.netflow.core.avro.NetFlowV2Record;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class StarRocksWriterIT {
    private static final String DATABASE = "netflow_db";
    private static final String TABLE = "netflow";
    private static final String SOURCE_TOPIC = "netflow-it-source";
    private static final String DLQ_TOPIC = "netflow-it-dlq";
    private static final String CONSUMER_GROUP = "netflow-it-writer";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(60);

    @RegisterExtension
    private static final KafkaTestExtension KAFKA = new KafkaTestExtension();

    private static final GenericContainer<?> STARROCKS =
            new GenericContainer<>(DockerImageName.parse("starrocks/allin1-ubuntu:4.0.12"))
                    .withExposedPorts(8030, 8040, 9030)
                    .waitingFor(Wait.forHttp("/api/health")
                            .forPort(8030)
                            .forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(5)));

    @BeforeAll
    static void startStarRocks() {
        STARROCKS.start();
    }

    @AfterAll
    static void stopStarRocks() {
        STARROCKS.stop();
    }

    @Test
    void loadsValidRecordRoutesInvalidRecordToDlqAndCommitsOffsets() throws Exception {
        String kafkaAddress = KAFKA.getBootstrapServers();
        String starRocksAddress = "http://" + STARROCKS.getHost()
                + ":" + STARROCKS.getMappedPort(8040);

        try (Connection database = connectToStarRocks();
             Admin admin = AdminClient.create(adminSettings(kafkaAddress))) {
            createTopics(admin);
            runScenario(kafkaAddress, starRocksAddress, database, admin);
        }
    }

    private static Connection connectToStarRocks() throws Exception {
        String jdbcUrl = "jdbc:mysql://" + STARROCKS.getHost() + ":"
                + STARROCKS.getMappedPort(9030)
                + "/?useSSL=false&allowPublicKeyRetrieval=true";
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        SQLException lastFailure = null;

        while (System.nanoTime() < deadline) {
            try {
                Connection connection = DriverManager.getConnection(jdbcUrl, "root", "");
                if (hasAliveBackend(connection)) {
                    return connection;
                }
                connection.close();
            } catch (SQLException notReady) {
                lastFailure = notReady;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("StarRocks container did not become ready", lastFailure);
    }

    private static boolean hasAliveBackend(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet backends = statement.executeQuery("SHOW BACKENDS")) {
            return backends.next() && backends.getBoolean("Alive");
        }
    }

    private static void runScenario(
            String kafkaAddress,
            String starRocksAddress,
            Connection database,
            Admin admin) throws Exception {
        WriterConfig config = writerConfig(kafkaAddress, starRocksAddress);
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();

        try (WriterApplication writer = new WriterApplication(config);
             KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerSettings(kafkaAddress));
             KafkaConsumer<byte[], byte[]> dlqConsumer =
                     new KafkaConsumer<>(dlqConsumerSettings(kafkaAddress))) {
            Thread writerThread = new Thread(
                    () -> runWriter(writer, writerFailure), "starrocks-writer-it");
            dlqConsumer.subscribe(List.of(DLQ_TOPIC));
            writerThread.start();

            await("StarRocks schema", writerFailure,
                    () -> tableExists(database));
            try {
                long timestamp = System.currentTimeMillis();
                RecordMetadata valid = producer.send(new ProducerRecord<>(
                        SOURCE_TOPIC, 0, timestamp, null, validAvroRecord()))
                        .get(30, TimeUnit.SECONDS);
                RecordMetadata invalid = producer.send(new ProducerRecord<>(
                        SOURCE_TOPIC, 0, timestamp, null, new byte[]{1, 2, 3}))
                        .get(30, TimeUnit.SECONDS);

                String recordId = SOURCE_TOPIC + ":0:" + valid.offset();
                await("valid row in StarRocks", writerFailure,
                        () -> hasLoadedRow(database, recordId));

                AtomicReference<ConsumerRecord<byte[], byte[]>> dlqRecord = new AtomicReference<>();
                await("invalid row in DLQ", writerFailure,
                        () -> pollDlq(dlqConsumer, dlqRecord));
                assertDlqRecord(dlqRecord.get(), invalid.offset());

                await("committed Kafka offset", writerFailure,
                        () -> isOffsetCommitted(admin, invalid.offset()));
            } finally {
                writer.stop();
                writerThread.join(5_000);
            }

            assertFalse(writerThread.isAlive(), "Writer did not stop within five seconds");
            failIfWriterStoppedUnexpectedly(writerFailure.get());
        }
    }

    private static void runWriter(
            WriterApplication writer,
            AtomicReference<Throwable> writerFailure) {
        try {
            writer.run();
        } catch (Throwable failure) {
            writerFailure.set(failure);
        }
    }

    private static void failIfWriterStoppedUnexpectedly(Throwable failure) {
        if (failure != null) {
            fail("Writer stopped unexpectedly", failure);
        }
    }

    private static WriterConfig writerConfig(String kafkaAddress, String starRocksAddress) {
        WriterConfig.KafkaConfig kafka = new WriterConfig.KafkaConfig(
                kafkaAddress,
                SOURCE_TOPIC,
                CONSUMER_GROUP,
                DLQ_TOPIC,
                100,
                100,
                300_000,
                5_242_880,
                "earliest");
        WriterConfig.StarRocksConfig starRocks = new WriterConfig.StarRocksConfig(
                starRocksAddress,
                "jdbc:mysql://" + STARROCKS.getHost() + ":"
                        + STARROCKS.getMappedPort(9030)
                        + "/?useSSL=false&allowPublicKeyRetrieval=true",
                DATABASE,
                TABLE,
                "root",
                "",
                3,
                100);
        return new WriterConfig(kafka, starRocks);
    }

    private static void createTopics(Admin admin) throws Exception {
        admin.createTopics(List.of(
                        new NewTopic(SOURCE_TOPIC, 1, (short) 1),
                        new NewTopic(DLQ_TOPIC, 1, (short) 1)))
                .all().get(30, TimeUnit.SECONDS);
    }

    private static boolean hasLoadedRow(Connection database, String recordId) throws Exception {
        String sql = "SELECT IPV4_SRC_ADDR FROM " + DATABASE + "." + TABLE
                + " WHERE record_id = ? LIMIT 1";
        try (PreparedStatement statement = database.prepareStatement(sql)) {
            statement.setString(1, recordId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() && "10.0.0.1".equals(rows.getString(1));
            }
        }
    }

    private static boolean pollDlq(
            KafkaConsumer<byte[], byte[]> consumer,
            AtomicReference<ConsumerRecord<byte[], byte[]>> result) {
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(250))) {
            result.set(record);
            return true;
        }
        return false;
    }

    private static void assertDlqRecord(
            ConsumerRecord<byte[], byte[]> record,
            long sourceOffset) throws Exception {
        assertNotNull(record);
        assertEquals(SOURCE_TOPIC + ":0:" + sourceOffset,
                new String(record.key(), StandardCharsets.UTF_8));
        JsonNode message = JSON.readTree(record.value());
        assertEquals(SOURCE_TOPIC, message.path("source_topic").asText());
        assertEquals(sourceOffset, message.path("source_offset").asLong());
        assertEquals("INVALID_AVRO", message.path("reason").asText());
    }

    private static boolean isOffsetCommitted(Admin admin, long invalidOffset) throws Exception {
        TopicPartition partition = new TopicPartition(SOURCE_TOPIC, 0);
        Map<TopicPartition, OffsetAndMetadata> offsets = admin
                .listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS);
        OffsetAndMetadata committed = offsets.get(partition);
        return committed != null && committed.offset() >= invalidOffset + 1;
    }

    private static byte[] validAvroRecord() throws Exception {
        Schema schema = NetFlowV2Record.getClassSchema();
        GenericData.Record record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object value = switch (field.schema().getType()) {
                case STRING -> "value";
                case INT -> 1;
                case LONG -> 1L;
                case DOUBLE -> 1.0;
                default -> throw new IllegalArgumentException("Unsupported field: " + field.name());
            };
            record.put(field.name(), value);
        }
        record.put("IPV4_SRC_ADDR", "10.0.0.1");
        record.put("IPV4_DST_ADDR", "10.0.0.2");
        record.put("FTP_COMMAND_RET_CODE", 0);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(schema);
        var encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        writer.write(record, encoder);
        encoder.flush();
        return bytes.toByteArray();
    }

    private static Properties adminSettings(String kafkaAddress) {
        Properties settings = new Properties();
        settings.put("bootstrap.servers", kafkaAddress);
        return settings;
    }

    private static Properties producerSettings(String kafkaAddress) {
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        return settings;
    }

    private static Properties dlqConsumerSettings(String kafkaAddress) {
        Properties settings = new Properties();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "netflow-it-dlq-reader");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return settings;
    }

    private static void await(
            String description,
            AtomicReference<Throwable> writerFailure,
            CheckedCondition condition) throws Exception {
        long deadline = System.nanoTime() + TEST_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            failIfWriterStoppedUnexpectedly(writerFailure.get());
            if (condition.evaluate()) {
                return;
            }
            Thread.sleep(250);
        }
        fail("Timed out waiting for " + description);
    }

    private static boolean tableExists(Connection connection) throws SQLException {
        String sql = """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            LIMIT 1
            """;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, DATABASE);
            statement.setString(2, TABLE);

            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }
}
