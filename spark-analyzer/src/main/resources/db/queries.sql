select protocol ,
       sum (flow_count) as total_flow_count
from protocol_usage
where bucket_hour >= '2026-10-05 00:00:00'
  AND bucket_hour <  '2026-10-08 23:00:00'
group by protocol
order by total_flow_count desc


SELECT src_ip, dst_ip,
       SUM(flow_count) AS total_flow_count,
       SUM(in_bytes) AS total_in_bytes,
       SUM(out_bytes) AS total_out_bytes,
       SUM(in_bytes + out_bytes) AS total_bytes
FROM ip_connections
WHERE bucket_hour >= '2026-10-05 00:00:00'
  AND bucket_hour <  '2026-10-08 23:00:00'
  AND (src_ip = '198.51.100.10' OR dst_ip = '198.51.100.10')
GROUP BY src_ip, dst_ip
ORDER BY total_flow_count DESC;


SELECT ip ,
       sum(in_bytes) as total_in_bytes ,
       sum(out_bytes) as total_out_bytes ,
       sum(in_bytes + out_bytes) as total_bytes
from ip_bandwidth
WHERE bucket_hour >= '2026-10-05 09:00:00'
  AND bucket_hour <  '2026-10-05 10:00:00'
group by ip
order by total_bytes desc
limit 10


(
    SELECT 'IN' AS type, bucket_hour, in_bytes AS bytes
    FROM ip_bandwidth
    WHERE ip = '198.51.100.11'
      AND bucket_hour >= '2026-10-05 00:00:00'
      AND bucket_hour <  '2026-10-08 23:00:00'
    ORDER BY in_bytes DESC
    LIMIT 1
)
UNION
(
SELECT 'OUT' AS type, bucket_hour, out_bytes AS bytes
FROM ip_bandwidth
WHERE ip = '198.51.100.11'
  AND bucket_hour >= '2026-10-05 00:00:00'
  AND bucket_hour <  '2026-10-08 23:00:00'
ORDER BY out_bytes DESC
    LIMIT 1
);
