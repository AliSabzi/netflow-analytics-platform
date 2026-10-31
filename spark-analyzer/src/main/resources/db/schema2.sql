CREATE DATABASE netflow;

CREATE TABLE ip_bandwidth (
    bucket_hour TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ip inet NOT NULL,
    in_bytes BIGINT NOT NULL,
    out_bytes BIGINT NOT NULL
) PARTITION BY RANGE (bucket_hour);

CREATE INDEX idx_bw_ip
    ON ip_bandwidth(ip);


CREATE TABLE ip_connections (
    bucket_hour TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    src_ip inet NOT NULL,
    dst_ip inet NOT NULL,
    flow_count INTEGER NOT NULL,
    in_bytes BIGINT NOT NULL,
    out_bytes BIGINT NOT NULL
) PARTITION BY RANGE (bucket_hour);

CREATE INDEX idx_conn_src
    ON ip_connections(src_ip);

CREATE INDEX idx_conn_dst
    ON ip_connections(dst_ip);


CREATE TABLE protocol_usage (
    bucket_hour TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    protocol INTEGER NOT NULL,
    flow_count BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL
) PARTITION BY RANGE (bucket_hour);