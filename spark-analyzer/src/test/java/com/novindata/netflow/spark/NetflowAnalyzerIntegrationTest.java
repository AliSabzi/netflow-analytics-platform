package com.novindata.netflow.spark;

import com.novindata.netflow.spark.config.SparkAppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class NetflowAnalyzerIntegrationTest {

    private static SparkSession spark;

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("netflow")
                    .withUsername("test")
                    .withPassword("test");


    @TempDir
    Path tempDir;


    @BeforeAll
    static void setUp() throws Exception {

        spark = SparkSession.builder()
                .appName("NetflowSparkAnalyzerIntegrationTest")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        createTables();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
     void shouldProcessReadyFolder() throws IOException,SQLException {
        Path root = tempDir.resolve("netflow");
        Path dateFolder = root.resolve("2026-01-01");
        Path oldProcessedFolder = dateFolder.resolve("hour_09_processed");
        Path readyFolder = dateFolder.resolve("hour_10_ready");
        Path newProcessedFolder = dateFolder.resolve("hour_10_processed");
        Path processingFolder = dateFolder.resolve("hour_11_processing");


        Files.createDirectories(oldProcessedFolder);
        createParquet(readyFolder);
        Files.createDirectories(processingFolder);


        SparkAppConfig config = getConfig(root);
        Properties jdbcProperties = getJdbcProperties(config);
        PostgresWriter postgresWriter = new PostgresWriter(config.getPostgresUrl(),jdbcProperties);
        HdfsFolderManager hdfsFolderManager = new HdfsFolderManager(spark.sparkContext().hadoopConfiguration());
        NetflowSparkAnalyzer analyzer = new NetflowSparkAnalyzer(spark,postgresWriter,hdfsFolderManager,config,null);

        analyzer.analyze();

        assertProtocolUsage();
        assertIpConnections();
        assertIpBandwidth();

        assertPartitionsPublished();
        assertNoCandidateTables();

        assertTrue(Files.exists(oldProcessedFolder));
        assertFalse(Files.exists(readyFolder));
        assertTrue(Files.exists(newProcessedFolder));
        assertTrue(Files.exists(processingFolder));
    }


    private static void createTables() throws Exception {

        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {

            statement.execute("""
                CREATE TABLE protocol_usage (
                    protocol INTEGER,
                    flow_count BIGINT,
                    total_bytes BIGINT,
                    bucket_hour TIMESTAMP NOT NULL
                ) PARTITION BY RANGE (bucket_hour)
                """);

            statement.execute("""
                CREATE TABLE ip_connections (
                    src_ip VARCHAR(50),
                    dst_ip VARCHAR(50),
                    flow_count BIGINT,
                    in_bytes BIGINT,
                    out_bytes BIGINT,
                    bucket_hour TIMESTAMP NOT NULL
                ) PARTITION BY RANGE (bucket_hour)
                """);

            statement.execute("""
                CREATE TABLE ip_bandwidth (
                    ip VARCHAR(50),
                    in_bytes BIGINT,
                    out_bytes BIGINT,
                    bucket_hour TIMESTAMP NOT NULL
                ) PARTITION BY RANGE (bucket_hour)
                """);
        }
    }

    private static void createParquet(Path path)
    {
        StructType schema = new StructType()
                .add("PROTOCOL", DataTypes.IntegerType)
                .add("IPV4_SRC_ADDR", DataTypes.StringType)
                .add("IPV4_DST_ADDR", DataTypes.StringType)
                .add("IN_BYTES", DataTypes.LongType)
                .add("OUT_BYTES", DataTypes.LongType);

        Dataset<Row> dataset = spark.createDataFrame(
                List.of(
                        RowFactory.create(6,"10.0.0.1","10.0.0.2",100L,50L),
                        RowFactory.create(6,"10.0.0.1","10.0.0.2",200L,100L),
                        RowFactory.create(17,"10.0.0.2","10.0.0.1",50L,25L)
                ),
                schema
        );

        dataset.write()
                .mode("overwrite")
                .parquet(path.toString());
    }


    private static void assertProtocolUsage() throws SQLException {

        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("""
                        SELECT *
                        FROM protocol_usage
                        ORDER BY protocol
                        """)
        ) {

            assertTrue(result.next());

            assertEquals(6, result.getInt("protocol"));
            assertEquals(2L, result.getLong("flow_count"));
            assertEquals(450L, result.getLong("total_bytes"));
            assertEquals(
                    LocalDateTime.of(2026, 1, 1, 10, 0),
                    result.getTimestamp("bucket_hour").toLocalDateTime()
            );

            assertTrue(result.next());

            assertEquals(17, result.getInt("protocol"));
            assertEquals(1L, result.getLong("flow_count"));
            assertEquals(75L, result.getLong("total_bytes"));
            assertEquals(
                    LocalDateTime.of(2026, 1, 1, 10, 0),
                    result.getTimestamp("bucket_hour").toLocalDateTime()
            );

            assertFalse(result.next());
        }
    }

    private static void assertIpConnections() throws SQLException {

        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("""
                        SELECT *
                        FROM ip_connections
                        ORDER BY src_ip
                        """)
        ) {

            assertTrue(result.next());

            assertEquals("10.0.0.1", result.getString("src_ip"));
            assertEquals("10.0.0.2", result.getString("dst_ip"));
            assertEquals(2L, result.getLong("flow_count"));
            assertEquals(300L, result.getLong("in_bytes"));
            assertEquals(150L, result.getLong("out_bytes"));
            assertEquals(
                    LocalDateTime.of(2026, 1, 1, 10, 0),
                    result.getTimestamp("bucket_hour").toLocalDateTime()
            );

            assertTrue(result.next());

            assertEquals("10.0.0.2", result.getString("src_ip"));
            assertEquals("10.0.0.1", result.getString("dst_ip"));
            assertEquals(1L, result.getLong("flow_count"));
            assertEquals(50L, result.getLong("in_bytes"));
            assertEquals(25L, result.getLong("out_bytes"));
            assertEquals(
                    LocalDateTime.of(2026, 1, 1, 10, 0),
                    result.getTimestamp("bucket_hour").toLocalDateTime()
            );

            assertFalse(result.next());
        }
    }

    private static void assertIpBandwidth() throws SQLException {

        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("""
                        SELECT *
                        FROM ip_bandwidth
                        ORDER BY ip
                        """)
        ) {

            assertTrue(result.next());

            assertEquals("10.0.0.1", result.getString("ip"));
            assertEquals(200L, result.getLong("in_bytes"));
            assertEquals(325L, result.getLong("out_bytes"));
            assertEquals(
                    LocalDateTime.of(2026, 1, 1, 10, 0),
                    result.getTimestamp("bucket_hour").toLocalDateTime()
            );

            assertTrue(result.next());

            assertEquals("10.0.0.2", result.getString("ip"));
            assertEquals(325L, result.getLong("in_bytes"));
            assertEquals(200L, result.getLong("out_bytes"));
            assertEquals(
                    LocalDateTime.of(2026, 1, 1, 10, 0),
                    result.getTimestamp("bucket_hour").toLocalDateTime()
            );

            assertFalse(result.next());
        }
    }

    private static void assertPartitionsPublished() throws SQLException {

        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {

            ResultSet result = statement.executeQuery("""
                SELECT child.relname
                FROM pg_inherits
                JOIN pg_class parent
                    ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child
                    ON pg_inherits.inhrelid = child.oid
                WHERE parent.relname IN (
                    'ip_connections',
                    'protocol_usage',
                    'ip_bandwidth'
                )
                ORDER BY child.relname
                """);

            assertTrue(result.next());
            assertEquals("ip_bandwidth_20260101_10", result.getString(1));

            assertTrue(result.next());
            assertEquals("ip_connections_20260101_10", result.getString(1));

            assertTrue(result.next());
            assertEquals("protocol_usage_20260101_10", result.getString(1));

            assertFalse(result.next());
        }
    }

    private static void assertNoCandidateTables() throws SQLException {

        try (
                Connection connection = getConnection();
                PreparedStatement statement = connection.prepareStatement("""
                    SELECT COUNT(*)
                    FROM pg_tables
                    WHERE schemaname = 'public'
                      AND tablename LIKE '%_candidate'
                    """)
        ) {

            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(0, result.getInt(1));
            }
        }
    }

    private static Connection getConnection() throws SQLException
    {
        return DriverManager.getConnection
                (postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword());
    }

    private SparkAppConfig getConfig(Path rootPath)
    {
        Properties properties = new Properties();
        properties.put("hdfs.parquet.path",rootPath.toString());
        properties.put("postgres.url",postgres.getJdbcUrl());
        properties.setProperty("postgres.username",postgres.getUsername());
        properties.setProperty("postgres.password", postgres.getPassword());
        properties.setProperty("postgres.driver","org.postgresql.Driver");
        properties.setProperty("postgres.table.protocol_usage","protocol_usage");
        properties.setProperty("postgres.table.ip_connections","ip_connections");
        properties.setProperty("postgres.table.ip_bandwidth","ip_bandwidth");
        return new SparkAppConfig(properties);
    }

    private Properties getJdbcProperties(SparkAppConfig config)
    {
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("user",config.getPostgresUser());
        jdbcProperties.put("password",config.getPostgresPassword());
        jdbcProperties.put("driver",config.getPostgresDriver());
        return jdbcProperties;
    }
}
