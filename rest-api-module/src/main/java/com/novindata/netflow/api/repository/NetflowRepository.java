package com.novindata.netflow.api.repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface NetflowRepository {

    List<Map<String, Object>> getTopProtocols
            (LocalDateTime startTime, LocalDateTime endTime);

    List<Map<String, Object>> getConnections
            (String ip, LocalDateTime startTime, LocalDateTime endTime);

    List<Map<String, Object>> getTopBandwidth
            (LocalDateTime startTime, LocalDateTime endTime, int limit);

    List<Map<String, Object>> getMaxUsageHours
            (String ip, LocalDateTime startTime, LocalDateTime endTime);
}