CREATE TABLE IF NOT EXISTS {table}
(
    kafka_record_time     DATETIME NOT NULL COMMENT 'Kafka record timestamp represented in Asia/Tehran local time',
    IPV4_SRC_ADDR         VARCHAR(45) NOT NULL,
    IPV4_DST_ADDR         VARCHAR(45) NOT NULL,
    PROTOCOL              INT NOT NULL,

    kafka_timestamp_ms    BIGINT NOT NULL COMMENT 'Original Kafka record timestamp in epoch milliseconds',
    kafka_timestamp_type  VARCHAR(32) NOT NULL,
    kafka_record_date     DATE NOT NULL COMMENT 'Calendar date derived from kafka_record_time',
    kafka_topic           VARCHAR(255) NOT NULL,
    kafka_partition       INT NOT NULL,
    kafka_offset          BIGINT NOT NULL,
    record_id             VARCHAR(300) NOT NULL COMMENT 'Kafka topic:partition:offset, used to detect replayed records',

    L4_SRC_PORT                    INT NOT NULL,
    L4_DST_PORT                    INT NOT NULL,
    L7_PROTO                       DOUBLE NOT NULL,
    IN_BYTES                       BIGINT NOT NULL,
    OUT_BYTES                      BIGINT NOT NULL,
    IN_PKTS                        BIGINT NOT NULL,
    OUT_PKTS                       BIGINT NOT NULL,
    FLOW_DURATION_MILLISECONDS     BIGINT NOT NULL,
    TCP_FLAGS                      INT NOT NULL,
    CLIENT_TCP_FLAGS               INT NOT NULL,
    SERVER_TCP_FLAGS               INT NOT NULL,
    DURATION_IN                    BIGINT NOT NULL,
    DURATION_OUT                   BIGINT NOT NULL,
    MIN_TTL                        INT NOT NULL,
    MAX_TTL                        INT NOT NULL,
    LONGEST_FLOW_PKT               INT NOT NULL,
    SHORTEST_FLOW_PKT              INT NOT NULL,
    MIN_IP_PKT_LEN                 INT NOT NULL,
    MAX_IP_PKT_LEN                 INT NOT NULL,
    SRC_TO_DST_SECOND_BYTES        BIGINT NOT NULL,
    DST_TO_SRC_SECOND_BYTES        BIGINT NOT NULL,
    RETRANSMITTED_IN_BYTES         BIGINT NOT NULL,
    RETRANSMITTED_IN_PKTS          BIGINT NOT NULL,
    RETRANSMITTED_OUT_BYTES        BIGINT NOT NULL,
    RETRANSMITTED_OUT_PKTS         BIGINT NOT NULL,
    SRC_TO_DST_AVG_THROUGHPUT      BIGINT NOT NULL,
    DST_TO_SRC_AVG_THROUGHPUT      BIGINT NOT NULL,
    NUM_PKTS_UP_TO_128_BYTES       BIGINT NOT NULL,
    NUM_PKTS_128_TO_256_BYTES      BIGINT NOT NULL,
    NUM_PKTS_256_TO_512_BYTES      BIGINT NOT NULL,
    NUM_PKTS_512_TO_1024_BYTES     BIGINT NOT NULL,
    NUM_PKTS_1024_TO_1514_BYTES    BIGINT NOT NULL,
    TCP_WIN_MAX_IN                 INT NOT NULL,
    TCP_WIN_MAX_OUT                INT NOT NULL,
    ICMP_TYPE                      INT NOT NULL,
    ICMP_IPV4_TYPE                 INT NOT NULL,
    DNS_QUERY_ID                   INT NOT NULL,
    DNS_QUERY_TYPE                 INT NOT NULL,
    DNS_TTL_ANSWER                 BIGINT NOT NULL,
    FTP_COMMAND_RET_CODE           INT NOT NULL
)
ENGINE=OLAP
DUPLICATE KEY(`kafka_record_time`, `IPV4_SRC_ADDR`, `IPV4_DST_ADDR`, `PROTOCOL`)
PARTITION BY date_trunc('day', kafka_record_time)
DISTRIBUTED BY RANDOM
PROPERTIES (
    "bloom_filter_columns" = "IPV4_DST_ADDR, IPV4_SRC_ADDR",
    "partition_live_number" = "90"
);
