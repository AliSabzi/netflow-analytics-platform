CREATE DATABASE netflow;

CREATE TABLE ip_bandwidth (
    bucket_hour TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ip VARCHAR NOT NULL,
    in_bytes BIGINT NOT NULL,
    out_bytes BIGINT NOT NULL
);

CREATE INDEX idx_bw_ip_hour
ON ip_bandwidth(ip, bucket_hour);


CREATE TABLE ip_connections (
    bucket_hour TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    src_ip VARCHAR NOT NULL,
    dst_ip VARCHAR NOT NULL,
    flow_count INTEGER NOT NULL,
    in_bytes BIGINT NOT NULL,
    out_bytes BIGINT NOT NULL
);

CREATE INDEX idx_conn_src_hour
ON ip_connections(src_ip, bucket_hour);

CREATE INDEX idx_conn_dst_hour
ON ip_connections(dst_ip, bucket_hour);


CREATE TABLE protocol_usage (
    bucket_hour TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    protocol INTEGER NOT NULL,
    flow_count BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    PRIMARY KEY (bucket_hour, protocol)
);