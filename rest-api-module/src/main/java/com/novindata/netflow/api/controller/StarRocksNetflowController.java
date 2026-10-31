package com.novindata.netflow.api.controller;

import com.novindata.netflow.api.repository.StarRocksNetflowRepository;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/starrocks")
@Tag(name = "StarRocks Netflow Analytics")
public class StarRocksNetflowController {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final StarRocksNetflowRepository repository;

    public StarRocksNetflowController(StarRocksNetflowRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/top-bandwidth")
    public ResponseEntity<List<Map<String, Object>>> getTopBandwidth(
            @Parameter(example = "2026-08-27 13:00:00")
            @RequestParam("startTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime startTime,

            @Parameter(example = "2026-08-27 20:00:00")
            @RequestParam("endTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime endTime,

            @RequestParam(name = "limit", defaultValue = "10")
            int limit
    ) {
        return ResponseEntity.ok(
                repository.getTopBandwidth(startTime, endTime, limit)
        );
    }

    @GetMapping("/connections")
    public ResponseEntity<List<Map<String, Object>>> getConnections(
            @Parameter(description = "IP address", example = "198.51.100.12")
            @RequestParam("ip")
            String ip,

            @Parameter(example = "2026-08-27 13:00:00")
            @RequestParam("startTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime startTime,

            @Parameter(example = "2026-08-27 20:00:00")
            @RequestParam("endTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime endTime
    ) {
        return ResponseEntity.ok(
                repository.getConnections(ip, startTime, endTime)
        );
    }

    @GetMapping("/protocols")
    public ResponseEntity<List<Map<String, Object>>> getProtocols(
            @Parameter(example = "2026-08-27 14:00:00")
            @RequestParam("startTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime startTime,

            @Parameter(example = "2026-08-27 18:00:00")
            @RequestParam("endTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime endTime
    ) {
        return ResponseEntity.ok(
                repository.getTopProtocols(startTime, endTime)
        );
    }

    @GetMapping("/max-usage")
    public ResponseEntity<List<Map<String, Object>>> getMaxUsage(
            @Parameter(description = "IP address", example = "198.51.100.12")
            @RequestParam("ip")
            String ip,

            @Parameter(example = "2026-08-27 13:00:00")
            @RequestParam("startTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime startTime,

            @Parameter(example = "2026-08-27 20:00:00")
            @RequestParam("endTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime endTime
    ) {
        return ResponseEntity.ok(
                repository.getMaxUsageHours(ip, startTime, endTime)
        );
    }
}
