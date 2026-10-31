package com.novindata.netflow.writer.backfill;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BackfillWindowTest {

    @Test
    void parsesStartAndEndInTehranTime() {
        BackfillWindow window = BackfillWindow.parse(
                "2026-09-28T01:00", "2026-09-28T04:00");

        assertEquals("Asia/Tehran", window.start().getZone().getId());
        assertEquals(1, window.start().getHour());
        assertEquals(4, window.end().getHour());
    }

    @Test
    void rejectsEndBeforeStart() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> BackfillWindow.parse("2026-09-28T04:00", "2026-09-28T01:00"));

        assertEquals("BACKFILL_START must be before BACKFILL_END", error.getMessage());
    }

    @Test
    void rejectsPartialHours() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> BackfillWindow.parse("2026-09-28T01:30", "2026-09-28T04:00"));

        assertEquals("Backfill times must be full hours, for example 2026-09-28T01:00",
                error.getMessage());
    }

    @Test
    void rejectsMissingStart() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> BackfillWindow.parse(null, "2026-09-28T04:00"));

        assertEquals("BACKFILL_START is required; expected value like 2026-09-28T01:00",
                error.getMessage());
    }

    @Test
    void createsOnePathForEachHour() {
        BackfillWindow window = BackfillWindow.parse(
                "2026-09-28T01:00", "2026-09-28T04:00");

        assertEquals(List.of(
                "2026-09-28/hour_01",
                "2026-09-28/hour_02",
                "2026-09-28/hour_03"
        ), window.hourPaths());
    }

    @Test
    void createsPathsAcrossMidnight() {
        BackfillWindow window = BackfillWindow.parse(
                "2026-09-28T23:00", "2026-09-29T02:00");

        assertEquals(List.of(
                "2026-09-28/hour_23",
                "2026-09-29/hour_00",
                "2026-09-29/hour_01"
        ), window.hourPaths());
    }
}
