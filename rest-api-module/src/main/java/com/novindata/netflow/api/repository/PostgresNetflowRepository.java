package com.novindata.netflow.api.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import com.novindata.netflow.api.monitoring.ApiMetrics;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Repository
public class PostgresNetflowRepository implements NetflowRepository {

    private final JdbcTemplate jdbcTemplate;

    public PostgresNetflowRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<Map<String, Object>> getTopProtocols(LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
                SELECT
                    protocol,
                    SUM(flow_count) AS total_flow_count
                FROM protocol_usage
                WHERE bucket_hour >= ?
                  AND bucket_hour < ?
                GROUP BY protocol
                ORDER BY total_flow_count DESC
                """;

        return ApiMetrics.query("postgres", "protocols", () -> jdbcTemplate.queryForList(sql, startTime, endTime));
    }

    @Override
    public List<Map<String, Object>> getConnections(String ip, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
                SELECT
                    host(src_ip) AS src_ip,
                    host(dst_ip) AS dst_ip,
                    SUM(flow_count) AS total_flow_count,
                    SUM(in_bytes) AS total_in_bytes,
                    SUM(out_bytes) AS total_out_bytes,
                    SUM(in_bytes + out_bytes) AS total_bytes
                FROM ip_connections
                WHERE bucket_hour >= ?
                  AND bucket_hour < ?
                  AND (
                      src_ip = CAST(? AS inet) OR
                      dst_ip = CAST(? AS inet)
                      )
                GROUP BY src_ip, dst_ip
                ORDER BY total_flow_count DESC
                """;

        return ApiMetrics.query("postgres", "connections", () -> jdbcTemplate.queryForList(sql, startTime, endTime, ip, ip));
    }

    @Override
    public List<Map<String, Object>> getTopBandwidth(LocalDateTime startTime, LocalDateTime endTime, int limit) {
        String sql = """
                SELECT
                    host(ip) AS ip,
                    SUM(in_bytes) AS total_in_bytes,
                    SUM(out_bytes) AS total_out_bytes,
                    SUM(in_bytes + out_bytes) AS total_bytes
                FROM ip_bandwidth
                WHERE bucket_hour >= ?
                  AND bucket_hour < ?
                GROUP BY ip
                ORDER BY total_bytes DESC
                LIMIT ?
                """;

        return ApiMetrics.query("postgres", "bandwidth", () -> jdbcTemplate.queryForList(sql, startTime, endTime, limit));
    }

    @Override
    public List<Map<String, Object>> getMaxUsageHours(String ip, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
                (
                    SELECT
                        'IN' AS type,
                        TO_CHAR(bucket_hour, 'YYYY-MM-DD HH24:MI:SS') AS bucket_hour,
                        in_bytes AS bytes
                    FROM ip_bandwidth
                    WHERE ip = CAST(? AS inet)
                      AND bucket_hour >= ?
                      AND bucket_hour < ?
                    ORDER BY in_bytes DESC
                    LIMIT 1
                )
                UNION ALL
                (
                    SELECT
                        'OUT' AS type,
                        TO_CHAR(bucket_hour, 'YYYY-MM-DD HH24:MI:SS') AS bucket_hour,
                        out_bytes AS bytes
                    FROM ip_bandwidth
                    WHERE ip = CAST(? AS inet)
                      AND bucket_hour >= ?
                      AND bucket_hour < ?
                    ORDER BY out_bytes DESC
                    LIMIT 1
                )
                """;

        return ApiMetrics.query("postgres", "max_usage", () -> jdbcTemplate.queryForList(sql, ip, startTime, endTime, ip, startTime, endTime));
    }
}
