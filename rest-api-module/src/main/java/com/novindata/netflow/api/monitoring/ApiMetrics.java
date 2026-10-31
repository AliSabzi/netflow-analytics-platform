package com.novindata.netflow.api.monitoring;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerMapping;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Component
public class ApiMetrics extends OncePerRequestFilter {
    private static final Counter QUERIES = Counter.builder()
            .name("netflow_api_queries_total")
            .help("Repository query attempts by backend and outcome")
            .labelNames("backend", "operation", "outcome")
            .register();

    private static final Gauge QUERIES_IN_FLIGHT = Gauge.builder()
            .name("netflow_api_queries_in_flight")
            .help("Repository queries currently executing")
            .labelNames("backend", "operation")
            .register();

    private static final Histogram QUERY_DURATION = Histogram.builder()
            .name("netflow_api_query_duration_seconds")
            .help("Repository query duration in seconds")
            .labelNames("backend", "operation")
            .classicOnly()
            .register();

    private static final Counter HTTP_REQUESTS = Counter.builder()
            .name("netflow_api_http_requests_total")
            .help("HTTP requests by route and response status class")
            .labelNames("route", "status_class")
            .register();

    private static final Histogram HTTP_DURATION = Histogram.builder()
            .name("netflow_api_http_request_duration_seconds")
            .help("HTTP request duration in seconds")
            .labelNames("route")
            .classicOnly()
            .register();

    private static final Gauge HTTP_REQUESTS_IN_FLIGHT = Gauge.builder()
            .name("netflow_api_http_requests_in_flight")
            .help("HTTP requests currently executing")
            .register();

    private final HTTPServer server;

    public ApiMetrics() throws IOException {
        int port = Integer.parseInt(System.getenv().getOrDefault("METRICS_PORT", "9403"));
        server = HTTPServer.builder().port(port).buildAndStart();
    }

    public static List<Map<String, Object>> query(String backend, String operation, Supplier<List<Map<String, Object>>> action) {
        long startedAt = System.nanoTime();
        QUERIES_IN_FLIGHT.labelValues(backend, operation).inc();
        try {
            List<Map<String, Object>> result = action.get();
            QUERIES.labelValues(backend, operation, "success").inc();
            return result;
        } catch (RuntimeException failure) {
            QUERIES.labelValues(backend, operation, "error").inc();
            throw failure;
        } finally {
            QUERIES_IN_FLIGHT.labelValues(backend, operation).dec();
            QUERY_DURATION.labelValues(backend, operation).observe((System.nanoTime() - startedAt) / 1_000_000_000.0);
        }
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path.equals("/health") || path.equals("/v3/api-docs");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long startedAt = System.nanoTime();
        int status = 500;
        HTTP_REQUESTS_IN_FLIGHT.inc();
        try {
            chain.doFilter(request, response);
            status = response.getStatus();
        } finally {
            Object pattern = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
            String route = pattern == null ? "unmatched" : pattern.toString();
            HTTP_REQUESTS.labelValues(route, status >= 100 && status < 600 ? status / 100 + "xx" : "other").inc();
            HTTP_DURATION.labelValues(route).observe((System.nanoTime() - startedAt) / 1_000_000_000.0);
            HTTP_REQUESTS_IN_FLIGHT.dec();
        }
    }

    @PreDestroy
    public void stop() {
        server.close();
    }
}
