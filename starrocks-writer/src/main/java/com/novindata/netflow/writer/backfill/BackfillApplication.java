package com.novindata.netflow.writer.backfill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public final class BackfillApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackfillApplication.class);

    private BackfillApplication() {
        throw new AssertionError("BackfillApplication must not be instantiated");
    }

    public static void main(String[] args) {
        try {
            String action = parseAction(args);
            BackfillWindow window = BackfillWindow.parse(
                    System.getenv("BACKFILL_START"),
                    System.getenv("BACKFILL_END"));
            String hdfsRoot = requiredEnvironmentVariable("HDFS_ROOT");

            List<String> finalizedFolders = new HdfsBackfillSource(hdfsRoot)
                    .findFinalizedFolders(window);

            LOGGER.info("Found {} finalized HDFS hour folder(s)", finalizedFolders.size());
            finalizedFolders.forEach(folder -> LOGGER.info("HDFS source folder: {}", folder));

            StarRocksBackfillStore store = new StarRocksBackfillStore(
                    requiredEnvironmentVariable("STARROCKS_JDBC_URL"),
                    requiredEnvironmentVariable("STARROCKS_USER"),
                    requiredEnvironmentVariable("STARROCKS_PASSWORD"),
                    requiredEnvironmentVariable("STARROCKS_TABLE"));
            execute(action, window, finalizedFolders, store);
        } catch (IllegalArgumentException e) {
            LOGGER.error("Backfill input is not valid: {}", e.getMessage());
            System.exit(1);
        } catch (IllegalStateException e) {
            LOGGER.error("Backfill cannot continue: {}", e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            LOGGER.error("Could not inspect HDFS", e);
            System.exit(1);
        } catch (SQLException e) {
            LOGGER.error("StarRocks operation failed", e);
            System.exit(1);
        }
    }

    static void execute(
            String action,
            BackfillWindow window,
            List<String> finalizedFolders,
            StarRocksBackfillStore store) throws SQLException {
        StarRocksBackfillStore.DataSummary hdfsSummary =
                StarRocksBackfillStore.DataSummary.empty();
        for (String folder : finalizedFolders) {
            hdfsSummary = hdfsSummary.plus(store.summarizeParquet(folder));
        }
        long hdfsRows = hdfsSummary.rowCount();
        LOGGER.info("HDFS has {} row(s) in total", hdfsRows);
        if (hdfsRows == 0) {
            throw new IllegalStateException("HDFS source is empty; recovery was not started");
        }
        StarRocksBackfillStore.DataSummary starRocksSummary = store.summarizeRows(window);
        LOGGER.info("StarRocks has {} row(s) in total", starRocksSummary.rowCount());

        if ("--recover".equals(action) && hdfsSummary.equals(starRocksSummary)) {
            LOGGER.info("HDFS and StarRocks summaries match; recovery was skipped");
            return;
        }

        if ("--load-staging".equals(action) || "--recover".equals(action)) {
            store.prepareStaging();
            for (String folder : finalizedFolders) {
                LOGGER.info("Loading HDFS folder into staging: {}", folder);
                store.loadFolderIntoStaging(folder);
            }
            StarRocksBackfillStore.DataSummary stagingSummary = store.summarizeStaging();
            long stagingRows = stagingSummary.rowCount();
            if (!stagingSummary.equals(hdfsSummary)) {
                throw new IllegalStateException(
                        "Staging does not match HDFS: HDFS=" + hdfsRows
                                + " row(s), staging=" + stagingRows + " row(s)");
            }
            LOGGER.info("Staging verified: {} row(s), summary matches HDFS", stagingRows);

            if ("--recover".equals(action)) {
                long outsideRows = store.countStagingRowsOutside(window);
                if (outsideRows != 0) {
                    throw new IllegalStateException(
                            "Staging contains " + outsideRows
                                    + " row(s) outside the requested window");
                }

                LOGGER.info("Replacing StarRocks range [{}, {})",
                        window.start().toLocalDateTime(),
                        window.end().toLocalDateTime());
                store.replaceRangeFromStaging(window);

                StarRocksBackfillStore.DataSummary recoveredSummary =
                        store.summarizeRows(window);
                long recoveredRows = recoveredSummary.rowCount();
                if (!recoveredSummary.equals(stagingSummary)) {
                    throw new IllegalStateException(
                            "Recovered data does not match staging: staging=" + stagingRows
                                    + " row(s), StarRocks=" + recoveredRows + " row(s)");
                }
                LOGGER.info("Recovery verified: {} row(s), summary matches staging",
                        recoveredRows);
            }
        } else if ("--prepare-staging".equals(action)) {
            store.prepareStaging();
            LOGGER.info("Staging table is ready and empty");
        } else {
            LOGGER.info("Dry-run complete; staging and main tables were not changed");
        }
    }

    static String requiredEnvironmentVariable(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    static String parseAction(String[] args) {
        if (args.length == 0) {
            return "--dry-run";
        }
        if (args.length == 1 && ("--prepare-staging".equals(args[0])
                || "--load-staging".equals(args[0])
                || "--recover".equals(args[0]))) {
            return args[0];
        }
        throw new IllegalArgumentException(
                "Supported arguments: --prepare-staging, --load-staging or --recover");
    }
}
