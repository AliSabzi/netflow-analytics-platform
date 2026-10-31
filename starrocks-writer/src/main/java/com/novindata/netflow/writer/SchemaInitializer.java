package com.novindata.netflow.writer;

import com.novindata.netflow.writer.config.WriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class SchemaInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaInitializer.class);
    private static final String SCHEMA_RESOURCE = "/schema.sql";
    private static final String TABLE_PLACEHOLDER = "{table}";

    private final String queryUrl;
    private final String database;
    private final String table;
    private final String user;
    private final String password;

    public SchemaInitializer(WriterConfig.StarRocksConfig starRocks) {
        this.queryUrl = starRocks.feQueryUrl();
        this.database = starRocks.database();
        this.table = starRocks.table();
        this.user = starRocks.user();
        this.password = starRocks.password();
    }

    public void initialize() throws IOException, SQLException {
        String qualifiedTable = "`" + database + "`.`" + table + "`";
        String tableDdl = readSchemaResource().replace(TABLE_PLACEHOLDER, qualifiedTable);

        try (Connection connection = DriverManager.getConnection(queryUrl, user, password);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS `" + database + "`");

            long deadline = System.currentTimeMillis() + 60_000;
            while (true) {
                try {
                    statement.execute(tableDdl);
                    break;
                } catch (SQLException e) {
                    if (e.getMessage().contains("available backends") && System.currentTimeMillis() < deadline) {
                        LOGGER.warn("StarRocks backend is not fully ready, retrying in 1 second...");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted while waiting for StarRocks backends", ie);
                        }                    }
                    else {
                        throw e;
                    }
                }
            }
        }
        LOGGER.info("ensured StarRocks schema exists: database={}, table={}", database, table);
    }

    private static String readSchemaResource() throws IOException {
        try (InputStream input = SchemaInitializer.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (input == null) {
                throw new IOException("Missing schema resource: " + SCHEMA_RESOURCE);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
