package com.novindata.netflow.writer.backfill;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StarRocksBackfillStoreTest {

    private static final BackfillWindow WINDOW = BackfillWindow.parse(
            "2026-09-22T21:00", "2026-09-22T22:00");

    @Test
    void preparesAndLoadsStaging() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        StarRocksBackfillStore store = store(connection);

        store.prepareStaging();
        store.loadFolderIntoStaging(
                "hdfs://namenode:8020/data/netflow/2026-09-22/hour_21_processed");

        verify(statement).execute("CREATE TABLE IF NOT EXISTS `netflow_backfill_staging` LIKE `netflow`");
        verify(statement).execute("TRUNCATE TABLE `netflow_backfill_staging`");
        verify(statement).execute(argThat(sql ->
                sql.contains("INSERT INTO `netflow_backfill_staging`")
                        && sql.contains("hour_21_processed/*.parquet")
                        && sql.contains("KAFKA_RECORD_TIMESTAMP")
                        && sql.contains("DATE_TRUNC('second', KAFKA_RECORD_TIMESTAMP)")
                        && sql.contains("KAFKA_TOPIC")
                        && sql.contains("KAFKA_PARTITION")
                        && sql.contains("KAFKA_OFFSET")
                        && !sql.contains("'hdfs-backfill'")
                        && !sql.contains("PROCESSING_TIMESTAMP")));
    }

    @Test
    void readsSummariesFromStagingAndParquet() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet result = summaryResult();
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(result);
        StarRocksBackfillStore store = store(connection);

        assertEquals(12, store.countStagingRows());
        assertEquals(12, store.countParquetRows(
                "hdfs://namenode:8020/data/netflow/2026-09-22/hour_21_processed"));

        verify(statement).executeQuery(argThat(sql ->
                sql.contains("FROM `netflow_backfill_staging`")));
        verify(statement).executeQuery(argThat(sql ->
                sql.contains("MIN(DATE_TRUNC('second', KAFKA_RECORD_TIMESTAMP))")
                        && sql.contains("FROM FILES(")));
    }

    @Test
    void readsSummaryAndOutsideCountForWindow() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet result = summaryResult();
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(result);
        StarRocksBackfillStore store = store(connection);

        assertEquals(12, store.countRows(WINDOW));
        assertEquals(12, store.countStagingRowsOutside(WINDOW));

        verify(statement, times(2)).setTimestamp(
                1, Timestamp.valueOf("2026-09-22 21:00:00"));
        verify(statement, times(2)).setTimestamp(
                2, Timestamp.valueOf("2026-09-22 22:00:00"));
    }

    @Test
    void replacesOnlyRequestedRange() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement delete = mock(PreparedStatement.class);
        Statement insert = mock(Statement.class);
        when(connection.prepareStatement(anyString())).thenReturn(delete);
        when(connection.createStatement()).thenReturn(insert);
        StarRocksBackfillStore store = store(connection);

        store.replaceRangeFromStaging(WINDOW);

        verify(delete).setTimestamp(1, Timestamp.valueOf("2026-09-22 21:00:00"));
        verify(delete).setTimestamp(2, Timestamp.valueOf("2026-09-22 22:00:00"));
        verify(delete).executeUpdate();
        verify(insert).execute("INSERT INTO `netflow` SELECT * FROM `netflow_backfill_staging`");
    }

    @Test
    void combinesSummaries() {
        LocalDateTime first = LocalDateTime.parse("2026-09-22T21:00:01");
        LocalDateTime last = LocalDateTime.parse("2026-09-22T21:59:59");
        StarRocksBackfillStore.DataSummary summary = new StarRocksBackfillStore.DataSummary(
                12, 100, 200, 10, 20, first, last);

        assertEquals(summary, StarRocksBackfillStore.DataSummary.empty().plus(summary));
        assertEquals(new StarRocksBackfillStore.DataSummary(
                        24, 200, 400, 20, 40, first, last),
                summary.plus(summary));
    }

    @Test
    void rejectsUnsafeTableAndFolderNames() {
        assertThrows(IllegalArgumentException.class,
                () -> new StarRocksBackfillStore("netflow; DROP TABLE netflow", () -> null));

        StarRocksBackfillStore store = new StarRocksBackfillStore("netflow", () -> null);
        assertThrows(IllegalArgumentException.class,
                () -> store.loadFolderIntoStaging("hdfs://namenode/path\" OR 1=1"));
        assertThrows(IllegalArgumentException.class,
                () -> store.summarizeParquet("hdfs://namenode/path;DROP"));
    }

    @Test
    void propagatesConnectionFailure() {
        StarRocksBackfillStore store = new StarRocksBackfillStore(
                "netflow", () -> { throw new SQLException("database unavailable"); });

        SQLException error = assertThrows(SQLException.class, store::prepareStaging);

        assertEquals("database unavailable", error.getMessage());
    }

    private StarRocksBackfillStore store(Connection connection) {
        return new StarRocksBackfillStore("netflow", () -> connection);
    }

    private ResultSet summaryResult() throws SQLException {
        ResultSet result = mock(ResultSet.class);
        when(result.getLong("row_count")).thenReturn(12L);
        when(result.getLong("total_in_bytes")).thenReturn(100L);
        when(result.getLong("total_out_bytes")).thenReturn(200L);
        when(result.getLong("total_in_packets")).thenReturn(10L);
        when(result.getLong("total_out_packets")).thenReturn(20L);
        when(result.getTimestamp("first_record_time"))
                .thenReturn(Timestamp.valueOf("2026-09-22 21:00:01"));
        when(result.getTimestamp("last_record_time"))
                .thenReturn(Timestamp.valueOf("2026-09-22 21:59:59"));
        return result;
    }
}
