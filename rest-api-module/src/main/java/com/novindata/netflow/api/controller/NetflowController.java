package com.novindata.netflow.api.controller;

import com.novindata.netflow.api.repository.NetflowRepository;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@RestController
@Tag(name = "Netflow Analytics")
public class NetflowController {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final NetflowRepository repository;

    public NetflowController(NetflowRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }

    @GetMapping("/top-bandwidth")
    public ResponseEntity<List<Map<String, Object>>> getTopBandwidth(
            @Parameter(example = "2026-10-05 09:00:00")
            @RequestParam("startTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime startTime,

            @Parameter(example = "2026-10-05 10:00:00")
            @RequestParam("endTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime endTime,

            @RequestParam(name = "limit", defaultValue = "10")
            int limit
    )
    {
        validateTimeRange(startTime, endTime);
        return ResponseEntity.ok(repository.getTopBandwidth(startTime, endTime, limit));
    }

    @GetMapping("/connections")
    public ResponseEntity<List<Map<String, Object>>> getConnections(
            @Parameter(description = "IP address", example = "198.51.100.10")
            @RequestParam("ip")
            String ip,

            @Parameter(example = "2026-10-05 00:00:00")
            @RequestParam("startTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime startTime,

            @Parameter(example = "2026-10-08 23:00:00")
            @RequestParam("endTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime endTime
    ) {
        validateTimeRange(startTime, endTime);
        return ResponseEntity.ok(repository.getConnections(ip, startTime, endTime));
    }

    @GetMapping("/protocols")
    public ResponseEntity<List<Map<String, Object>>> getProtocols(
            @Parameter(example = "2026-10-05 00:00:00")
            @RequestParam("startTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime startTime,

            @Parameter(example = "2026-10-08 23:00:00")
            @RequestParam("endTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime endTime
    ) {
        validateTimeRange(startTime, endTime);
        return ResponseEntity.ok(repository.getTopProtocols(startTime, endTime));
    }

    @GetMapping("/max-usage")
    public ResponseEntity<List<Map<String, Object>>> getMaxUsage(
            @Parameter(description = "IP address", example = "198.51.100.11")
            @RequestParam("ip")
            String ip,

            @Parameter(example = "2026-10-05 00:00:00")
            @RequestParam("startTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime startTime,

            @Parameter(example = "2026-10-08 23:00:00")
            @RequestParam("endTime")
            @DateTimeFormat(pattern = DATE_FORMAT)
            LocalDateTime endTime
    ) {
        validateTimeRange(startTime, endTime);
        return ResponseEntity.ok(repository.getMaxUsageHours(ip, startTime, endTime));
    }

    private void validateTimeRange(LocalDateTime startTime, LocalDateTime endTime) {
        if (!startTime.isBefore(endTime)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"startTime must be before endTime");
        }
    }
}
