package com.novindata.netflow.api.repository;
import com.novindata.netflow.api.monitoring.ApiMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.LocalDateTime;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
class MonitoringQueryTest {
    private static double value(String name, String... labels) {
        for (var metric : io.prometheus.metrics.model.registry.PrometheusRegistry.defaultRegistry.scrape()) {
            if (!metric.getMetadata().getPrometheusName().equals(name)) continue;
            for (var point : metric.getDataPoints()) {
                boolean match = true;
                for (int i = 0; i < labels.length; i += 2)
                    match &= labels[i + 1].equals(point.getLabels().get(labels[i]));
                if (match) {
                    if (point instanceof io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot c) return c.getValue();
                    if (point instanceof io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot g) return g.getValue();
                }
            }
        }
        return 0;
    }

    @Test void repositoryRecordsQueryOutcomeAndPropagatesFailure() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        LocalDateTime start = LocalDateTime.of(2026, 1, 1, 0, 0), end = start.plusHours(1);
        when(jdbc.queryForList(anyString(), eq(start), eq(end)))
            .thenReturn(List.of(Map.of("protocol", 6))).thenThrow(new IllegalStateException("database unavailable"));
        PostgresNetflowRepository repository = new PostgresNetflowRepository(jdbc);
        assertEquals(1, repository.getTopProtocols(start, end).size());
        assertThrows(IllegalStateException.class, () -> repository.getTopProtocols(start, end));
        assertEquals(1, value("netflow_api_queries", "backend", "postgres", "operation", "protocols", "outcome", "error"));
        assertEquals(0, value("netflow_api_queries_in_flight", "backend", "postgres", "operation", "protocols"));
    }
}
