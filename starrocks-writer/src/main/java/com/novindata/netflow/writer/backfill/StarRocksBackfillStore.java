package com.novindata.netflow.writer.backfill;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class StarRocksBackfillStore {
    private static final int BACKFILL_NETWORK_TIMEOUT_MS = 15 * 60 * 1000;

    private final ConnectionFactory connectionFactory;
    private final String table;
    private final String stagingTable;

    @FunctionalInterface
    interface ConnectionFactory {
        Connection open() throws SQLException;
    }

    public record DataSummary(
            long rowCount,
            long totalInBytes,
            long totalOutBytes,
            long totalInPackets,
            long totalOutPackets,
            LocalDateTime firstRecordTime,
            LocalDateTime lastRecordTime) {

        public static DataSummary empty() {
            return new DataSummary(0, 0, 0, 0, 0, null, null);
        }

        public DataSummary plus(DataSummary other) {
            LocalDateTime first = firstRecordTime == null ? other.firstRecordTime
                    : other.firstRecordTime == null ? firstRecordTime
                    : firstRecordTime.isBefore(other.firstRecordTime) ? firstRecordTime : other.firstRecordTime;
            LocalDateTime last = lastRecordTime == null ? other.lastRecordTime
                    : other.lastRecordTime == null ? lastRecordTime
                    : lastRecordTime.isAfter(other.lastRecordTime) ? lastRecordTime : other.lastRecordTime;
            return new DataSummary(
                    rowCount + other.rowCount,
                    totalInBytes + other.totalInBytes,
                    totalOutBytes + other.totalOutBytes,
                    totalInPackets + other.totalInPackets,
                    totalOutPackets + other.totalOutPackets,
                    first,
                    last);
        }
    }

    public StarRocksBackfillStore(String jdbcUrl, String user, String password, String table) {
        this(table, () -> openConfiguredConnection(jdbcUrl, user, password));
    }

    StarRocksBackfillStore(String table, ConnectionFactory connectionFactory) {
        if (!table.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("STARROCKS_TABLE contains invalid characters");
        }
        this.connectionFactory = connectionFactory;
        this.table = table;
        this.stagingTable = table + "_backfill_staging";
    }

    public void prepareStaging() throws SQLException {
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS `" + stagingTable
                    + "` LIKE `" + table + "`");
            statement.execute("TRUNCATE TABLE `" + stagingTable + "`");
        }
    }

    public void loadFolderIntoStaging(String folder) throws SQLException {
        validateFolder(folder);
        String sql = loadSqlTemplate()
                .replace("${STAGING_TABLE}", "`" + stagingTable + "`")
                .replace("${HDFS_FOLDER}", folder);

        try (Connection connection = openConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    public long countStagingRows() throws SQLException {
        return summarizeStaging().rowCount();
    }

    public DataSummary summarizeStaging() throws SQLException {
        String sql = summarySelect("kafka_record_time")
                + " FROM `" + stagingTable + "`";
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return readSummary(result);
        }
    }

    public long countStagingRowsOutside(BackfillWindow window) throws SQLException {
        String sql = "SELECT COUNT(*) AS row_count FROM `" + stagingTable + "` "
                + "WHERE kafka_record_time < ? OR kafka_record_time >= ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.valueOf(window.start().toLocalDateTime()));
            statement.setTimestamp(2, Timestamp.valueOf(window.end().toLocalDateTime()));
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getLong("row_count");
            }
        }
    }

    public void replaceRangeFromStaging(BackfillWindow window) throws SQLException {
        String deleteSql = "DELETE FROM `" + table + "` "
                + "WHERE kafka_record_time >= ? AND kafka_record_time < ?";
        String insertSql = "INSERT INTO `" + table + "` SELECT * FROM `"
                + stagingTable + "`";

        try (Connection connection = openConnection();
             PreparedStatement delete = connection.prepareStatement(deleteSql);
             Statement insert = connection.createStatement()) {
            delete.setTimestamp(1, Timestamp.valueOf(window.start().toLocalDateTime()));
            delete.setTimestamp(2, Timestamp.valueOf(window.end().toLocalDateTime()));
            delete.executeUpdate();
            insert.execute(insertSql);
        }
    }

    public long countRows(BackfillWindow window) throws SQLException {
        return summarizeRows(window).rowCount();
    }

    public DataSummary summarizeRows(BackfillWindow window) throws SQLException {
        String sql = summarySelect("kafka_record_time")
                + " FROM `" + table + "` "
                + "WHERE kafka_record_time >= ? AND kafka_record_time < ?";

        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.valueOf(window.start().toLocalDateTime()));
            statement.setTimestamp(2, Timestamp.valueOf(window.end().toLocalDateTime()));

            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return readSummary(result);
            }
        }
    }

    public long countParquetRows(String folder) throws SQLException {
        return summarizeParquet(folder).rowCount();
    }

    public DataSummary summarizeParquet(String folder) throws SQLException {
        validateFolder(folder);

        String sql = summarySelect("DATE_TRUNC('second', KAFKA_RECORD_TIMESTAMP)") + " " + """
                FROM FILES(
                    "path" = "%s/*.parquet",
                    "format" = "parquet",
                    "hadoop.security.authentication" = "simple",
                    "username" = "hadoop"
                )
                """.formatted(folder);

        try (Connection connection = openConnection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return readSummary(result);
        }
    }

    private String summarySelect(String timeColumn) {
        return "SELECT COUNT(*) AS row_count, "
                + "COALESCE(SUM(IN_BYTES), 0) AS total_in_bytes, "
                + "COALESCE(SUM(OUT_BYTES), 0) AS total_out_bytes, "
                + "COALESCE(SUM(IN_PKTS), 0) AS total_in_packets, "
                + "COALESCE(SUM(OUT_PKTS), 0) AS total_out_packets, "
                + "MIN(" + timeColumn + ") AS first_record_time, "
                + "MAX(" + timeColumn + ") AS last_record_time";
    }

    private DataSummary readSummary(ResultSet result) throws SQLException {
        Timestamp first = result.getTimestamp("first_record_time");
        Timestamp last = result.getTimestamp("last_record_time");
        return new DataSummary(
                result.getLong("row_count"),
                result.getLong("total_in_bytes"),
                result.getLong("total_out_bytes"),
                result.getLong("total_in_packets"),
                result.getLong("total_out_packets"),
                first == null ? null : first.toLocalDateTime(),
                last == null ? null : last.toLocalDateTime());
    }

    private void validateFolder(String folder) {
        if (!folder.matches("[A-Za-z0-9_./:-]+")) {
            throw new IllegalArgumentException("HDFS folder contains invalid characters: " + folder);
        }
    }

    private Connection openConnection() throws SQLException {
        return connectionFactory.open();
    }

    private static Connection openConfiguredConnection(String jdbcUrl, String user, String password) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
        try {
            connection.setNetworkTimeout(Runnable::run, BACKFILL_NETWORK_TIMEOUT_MS);
            return connection;
        } catch (SQLException | RuntimeException exception) {
            try {
                connection.close();
            } catch (SQLException closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }
    }

    private String loadSqlTemplate() {
        try (InputStream input = StarRocksBackfillStore.class
                .getResourceAsStream("/backfill/load_staging.sql")) {
            if (input == null) {
                throw new IllegalStateException("Backfill load SQL resource is missing");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Could not read backfill load SQL", e);
        }
    }
}
