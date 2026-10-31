package com.novindata.netflow.api.repository;

import com.novindata.netflow.api.monitoring.ApiMetrics;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Repository
public class JdbcStarRocksNetflowRepository implements StarRocksNetflowRepository {

    private final JdbcTemplate jdbcTemplate;
    private final String table;

    public JdbcStarRocksNetflowRepository(
            @Qualifier("starRocksJdbcTemplate") JdbcTemplate jdbcTemplate,
            @Value("${starrocks.table}") String table) {
        this.jdbcTemplate = jdbcTemplate;
        this.table = "`" + table + "`";
    }

    @Override
    public List<Map<String, Object>> getTopProtocols(LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
                SELECT
                    PROTOCOL AS protocol,
                    COUNT(*) AS total_flow_count
                FROM %s
                WHERE kafka_record_time >= ?
                  AND kafka_record_time < ?
                GROUP BY PROTOCOL
                ORDER BY total_flow_count DESC
                """.formatted(table);

        return ApiMetrics.query("starrocks", "protocols", () -> jdbcTemplate.queryForList(sql, startTime, endTime));
    }

    @Override
    public List<Map<String, Object>> getConnections(String ip, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
                SELECT
                    IPV4_SRC_ADDR AS src_ip,
                    IPV4_DST_ADDR AS dst_ip,
                    COUNT(*) AS total_flow_count,
                    SUM(IN_BYTES) AS total_in_bytes,
                    SUM(OUT_BYTES) AS total_out_bytes,
                    SUM(IN_BYTES + OUT_BYTES) AS total_bytes
                FROM %s
                WHERE kafka_record_time >= ?
                  AND kafka_record_time < ?
                  AND (IPV4_SRC_ADDR = ? OR IPV4_DST_ADDR = ?)
                GROUP BY IPV4_SRC_ADDR, IPV4_DST_ADDR
                ORDER BY total_flow_count DESC
                """.formatted(table);

        return ApiMetrics.query("starrocks", "connections", () -> jdbcTemplate.queryForList(sql, startTime, endTime, ip, ip));
    }

    @Override
    public List<Map<String, Object>> getTopBandwidth(LocalDateTime startTime, LocalDateTime endTime, int limit) {
        String sql = """
                WITH flows AS (
                    SELECT IPV4_SRC_ADDR AS ip, OUT_BYTES AS in_bytes, IN_BYTES AS out_bytes
                    FROM %s
                    WHERE kafka_record_time >= ?
                      AND kafka_record_time < ?
                    UNION ALL
                    SELECT IPV4_DST_ADDR AS ip, IN_BYTES AS in_bytes, OUT_BYTES AS out_bytes
                    FROM %s
                    WHERE kafka_record_time >= ?
                      AND kafka_record_time < ?
                )
                SELECT
                    ip,
                    SUM(in_bytes) AS total_in_bytes,
                    SUM(out_bytes) AS total_out_bytes,
                    SUM(in_bytes + out_bytes) AS total_bytes
                FROM flows
                GROUP BY ip
                ORDER BY total_bytes DESC
                LIMIT ?
                """.formatted(table, table);

        return ApiMetrics.query("starrocks", "bandwidth", () -> jdbcTemplate.queryForList(sql, startTime, endTime, startTime, endTime, limit));
    }

    @Override
    public List<Map<String, Object>> getMaxUsageHours(String ip, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
                WITH flows AS (
                    SELECT kafka_record_time, OUT_BYTES AS in_bytes, IN_BYTES AS out_bytes
                    FROM %s
                    WHERE IPV4_SRC_ADDR = ?
                      AND kafka_record_time >= ?
                      AND kafka_record_time < ?
                    UNION ALL
                    SELECT kafka_record_time, IN_BYTES AS in_bytes, OUT_BYTES AS out_bytes
                    FROM %s
                    WHERE IPV4_DST_ADDR = ?
                      AND kafka_record_time >= ?
                      AND kafka_record_time < ?
                ),
                hourly AS (
                    SELECT
                        date_trunc('hour', kafka_record_time) AS bucket_hour,
                        SUM(in_bytes) AS in_bytes,
                        SUM(out_bytes) AS out_bytes
                    FROM flows
                    GROUP BY bucket_hour
                )
                (SELECT
                    'IN' AS type,
                    date_format(bucket_hour, '%%Y-%%m-%%d %%H:%%i:%%s') AS bucket_hour,
                    in_bytes AS bytes
                 FROM hourly
                 ORDER BY in_bytes DESC
                 LIMIT 1)
                UNION ALL
                (SELECT
                    'OUT' AS type,
                    date_format(bucket_hour, '%%Y-%%m-%%d %%H:%%i:%%s') AS bucket_hour,
                    out_bytes AS bytes
                 FROM hourly
                 ORDER BY out_bytes DESC
                 LIMIT 1)
                """.formatted(table, table);

        return ApiMetrics.query("starrocks", "max_usage", () -> jdbcTemplate.queryForList(sql, ip, startTime, endTime, ip, startTime, endTime));
    }
}
