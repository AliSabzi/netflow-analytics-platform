package com.novindata.netflow.writer.backfill;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HdfsBackfillSourceTest {
    @TempDir
    Path root;

    @Test
    void findsProcessedAndReadyFolders() throws Exception {
        Files.createDirectories(root.resolve("2026-09-28/hour_01_processed"));
        Files.createDirectories(root.resolve("2026-09-28/hour_02_ready"));

        List<String> folders = new HdfsBackfillSource(root.toUri().toString())
                .findFinalizedFolders(BackfillWindow.parse(
                        "2026-09-28T01:00", "2026-09-28T03:00"));

        assertEquals(2, folders.size());
        assertTrue(folders.get(0).endsWith("/2026-09-28/hour_01_processed"));
        assertTrue(folders.get(1).endsWith("/2026-09-28/hour_02_ready"));
    }

    @Test
    void rejectsProcessingFolder() throws Exception {
        Files.createDirectories(root.resolve("2026-09-28/hour_01_processing"));

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> new HdfsBackfillSource(root.toUri().toString())
                        .findFinalizedFolders(BackfillWindow.parse(
                                "2026-09-28T01:00", "2026-09-28T02:00")));

        assertEquals("HDFS hour is still processing: 2026-09-28/hour_01", error.getMessage());
    }

    @Test
    void findsFoldersAcrossDates() throws Exception {
        Files.createDirectories(root.resolve("2026-09-28/hour_23_ready"));
        Files.createDirectories(root.resolve("2026-09-29/hour_00_ready"));

        List<String> folders = new HdfsBackfillSource(root.toUri().toString())
                .findFinalizedFolders(BackfillWindow.parse(
                        "2026-09-28T23:00", "2026-09-29T01:00"));

        assertTrue(folders.get(0).endsWith("/2026-09-28/hour_23_ready"));
        assertTrue(folders.get(1).endsWith("/2026-09-29/hour_00_ready"));
    }
}
