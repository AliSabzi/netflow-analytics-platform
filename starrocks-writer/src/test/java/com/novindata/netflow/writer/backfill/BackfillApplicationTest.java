package com.novindata.netflow.writer.backfill;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BackfillApplicationTest {

    private static final BackfillWindow WINDOW = BackfillWindow.parse(
            "2026-09-22T21:00", "2026-09-22T22:00");
    private static final String FOLDER =
            "hdfs://namenode:8020/data/netflow/2026-09-22/hour_21_processed";
    private static final StarRocksBackfillStore.DataSummary HDFS_SUMMARY = summary(12);
    private static final StarRocksBackfillStore.DataSummary DIFFERENT_SUMMARY = summary(10);

    @Test
    void dryRunDoesNotChangeTables() throws Exception {
        StarRocksBackfillStore store = storeWithSourceAndMain(DIFFERENT_SUMMARY);

        BackfillApplication.execute("--dry-run", WINDOW, List.of(FOLDER), store);

        verify(store, never()).prepareStaging();
        verify(store, never()).replaceRangeFromStaging(WINDOW);
    }

    @Test
    void skipsRecoveryWhenSummariesAlreadyMatch() throws Exception {
        StarRocksBackfillStore store = storeWithSourceAndMain(HDFS_SUMMARY);

        BackfillApplication.execute("--recover", WINDOW, List.of(FOLDER), store);

        verify(store, never()).prepareStaging();
        verify(store, never()).replaceRangeFromStaging(WINDOW);
    }

    @Test
    void preparesStagingWithoutLoadingData() throws Exception {
        StarRocksBackfillStore store = storeWithSourceAndMain(DIFFERENT_SUMMARY);

        BackfillApplication.execute("--prepare-staging", WINDOW, List.of(FOLDER), store);

        verify(store).prepareStaging();
        verify(store, never()).loadFolderIntoStaging(FOLDER);
    }

    @Test
    void loadsAndReplacesRangeAfterAllChecksPass() throws Exception {
        StarRocksBackfillStore store = mock(StarRocksBackfillStore.class);
        when(store.summarizeParquet(FOLDER)).thenReturn(HDFS_SUMMARY);
        when(store.summarizeRows(WINDOW)).thenReturn(DIFFERENT_SUMMARY, HDFS_SUMMARY);
        when(store.summarizeStaging()).thenReturn(HDFS_SUMMARY);

        BackfillApplication.execute("--recover", WINDOW, List.of(FOLDER), store);

        verify(store).prepareStaging();
        verify(store).loadFolderIntoStaging(FOLDER);
        verify(store).countStagingRowsOutside(WINDOW);
        verify(store).replaceRangeFromStaging(WINDOW);
    }

    @Test
    void rejectsEmptyHdfsSource() throws Exception {
        StarRocksBackfillStore store = mock(StarRocksBackfillStore.class);
        when(store.summarizeParquet(FOLDER))
                .thenReturn(StarRocksBackfillStore.DataSummary.empty());

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> BackfillApplication.execute(
                        "--recover", WINDOW, List.of(FOLDER), store));

        assertEquals("HDFS source is empty; recovery was not started", error.getMessage());
        verify(store, never()).prepareStaging();
    }

    @Test
    void rejectsStagingThatDoesNotMatchHdfs() throws Exception {
        StarRocksBackfillStore store = storeWithSourceAndMain(DIFFERENT_SUMMARY);
        when(store.summarizeStaging()).thenReturn(DIFFERENT_SUMMARY);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> BackfillApplication.execute(
                        "--recover", WINDOW, List.of(FOLDER), store));

        assertEquals("Staging does not match HDFS: HDFS=12 row(s), staging=10 row(s)",
                error.getMessage());
        verify(store, never()).replaceRangeFromStaging(WINDOW);
    }

    @Test
    void rejectsRowsOutsideRequestedWindow() throws Exception {
        StarRocksBackfillStore store = storeWithSourceAndMain(DIFFERENT_SUMMARY);
        when(store.summarizeStaging()).thenReturn(HDFS_SUMMARY);
        when(store.countStagingRowsOutside(WINDOW)).thenReturn(2L);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> BackfillApplication.execute(
                        "--recover", WINDOW, List.of(FOLDER), store));

        assertEquals("Staging contains 2 row(s) outside the requested window",
                error.getMessage());
        verify(store, never()).replaceRangeFromStaging(WINDOW);
    }

    @Test
    void detectsMismatchAfterReplacement() throws Exception {
        StarRocksBackfillStore store = mock(StarRocksBackfillStore.class);
        when(store.summarizeParquet(FOLDER)).thenReturn(HDFS_SUMMARY);
        when(store.summarizeRows(WINDOW))
                .thenReturn(DIFFERENT_SUMMARY, DIFFERENT_SUMMARY);
        when(store.summarizeStaging()).thenReturn(HDFS_SUMMARY);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> BackfillApplication.execute(
                        "--recover", WINDOW, List.of(FOLDER), store));

        assertEquals(
                "Recovered data does not match staging: staging=12 row(s), StarRocks=10 row(s)",
                error.getMessage());
        verify(store).replaceRangeFromStaging(WINDOW);
    }

    @Test
    void parsesSupportedActionsAndDefaultsToDryRun() {
        assertEquals("--dry-run", BackfillApplication.parseAction(new String[0]));
        assertEquals("--prepare-staging",
                BackfillApplication.parseAction(new String[]{"--prepare-staging"}));
        assertEquals("--load-staging",
                BackfillApplication.parseAction(new String[]{"--load-staging"}));
        assertEquals("--recover",
                BackfillApplication.parseAction(new String[]{"--recover"}));
        assertThrows(IllegalArgumentException.class,
                () -> BackfillApplication.parseAction(new String[]{"--unknown"}));
    }

    @Test
    void validatesRequiredEnvironmentVariables() {
        assertEquals(System.getenv("PATH"),
                BackfillApplication.requiredEnvironmentVariable("PATH"));
        assertThrows(IllegalArgumentException.class,
                () -> BackfillApplication.requiredEnvironmentVariable(
                        "BACKFILL_TEST_VARIABLE_THAT_DOES_NOT_EXIST"));
    }

    private StarRocksBackfillStore storeWithSourceAndMain(
            StarRocksBackfillStore.DataSummary mainSummary) throws Exception {
        StarRocksBackfillStore store = mock(StarRocksBackfillStore.class);
        when(store.summarizeParquet(FOLDER)).thenReturn(HDFS_SUMMARY);
        when(store.summarizeRows(WINDOW)).thenReturn(mainSummary);
        return store;
    }

    private static StarRocksBackfillStore.DataSummary summary(long rows) {
        return new StarRocksBackfillStore.DataSummary(
                rows,
                rows * 100,
                rows * 200,
                rows * 10,
                rows * 20,
                LocalDateTime.parse("2026-09-22T21:00:01"),
                LocalDateTime.parse("2026-09-22T21:59:59"));
    }
}
