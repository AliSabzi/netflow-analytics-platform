package com.novindata.netflow.spark.config;

import java.util.Properties;

public class SparkAppConfig {

    private String sparkDriver;
    private String sparkMaster;
    private String sparkTimezone;
    private String sparkExecutorCores;
    private String sparkExecutorMemory;

    private String hdfsParquetPath;

    private String protocolTable;
    private String connectionsTable;
    private String bandwidthTable;

    private String postgresUrl;
    private String postgresUser;
    private String postgresPassword;
    private String postgresDriver;


    public SparkAppConfig() {

        this.sparkDriver = getRequired("SPARK_DRIVER_HOST");
        this.sparkMaster = getRequired("SPARK_MASTER");
        this.sparkTimezone = getRequired("SPARK_TIMEZONE");
        this.sparkExecutorCores = getRequired("SPARK_EXECUTOR_CORES");
        this.sparkExecutorMemory = getRequired("SPARK_EXECUTOR_MEMORY");

        this.hdfsParquetPath = getRequired("HDFS_PARQUET_PATH");

        this.protocolTable = getRequired("POSTGRES_TABLE_PROTOCOL_USAGE");
        this.connectionsTable = getRequired("POSTGRES_TABLE_IP_CONNECTIONS");
        this.bandwidthTable = getRequired("POSTGRES_TABLE_IP_BANDWIDTH");

        this.postgresUrl = getRequired("POSTGRES_URL");
        this.postgresUser = getRequired("POSTGRES_USERNAME");
        this.postgresPassword = getRequired("POSTGRES_PASSWORD");
        this.postgresDriver = getRequired("POSTGRES_DRIVER");
    }


    public SparkAppConfig(Properties properties) {
        this.hdfsParquetPath = properties.getProperty("hdfs.parquet.path");

        this.protocolTable = properties.getProperty("postgres.table.protocol_usage");
        this.connectionsTable = properties.getProperty("postgres.table.ip_connections");
        this.bandwidthTable = properties.getProperty("postgres.table.ip_bandwidth");

        this.postgresUrl = properties.getProperty("postgres.url");
        this.postgresUser = properties.getProperty("postgres.username");
        this.postgresPassword = properties.getProperty("postgres.password");
        this.postgresDriver = properties.getProperty("postgres.driver");
    }


    private String getRequired(String key) {

        String value = System.getenv(key);

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing environment variable: " + key
            );
        }
        return value;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }
    public String getSparkDriver() {
        return sparkDriver;
    }

    public String getSparkTimezone() {
        return sparkTimezone;
    }

    public String getSparkExecutorCores() {
        return sparkExecutorCores;
    }

    public String getSparkExecutorMemory() {
        return sparkExecutorMemory;
    }

    public String getHdfsParquetPath() {
        return hdfsParquetPath;
    }

    public String getProtocolTable() {
        return protocolTable;
    }

    public String getConnectionsTable() {
        return connectionsTable;
    }

    public String getBandwidthTable() {
        return bandwidthTable;
    }

    public String getPostgresUrl() {
        return postgresUrl;
    }

    public String getPostgresUser() {
        return postgresUser;
    }

    public String getPostgresPassword() {
        return postgresPassword;
    }

    public String getPostgresDriver() {
        return postgresDriver;
    }
}