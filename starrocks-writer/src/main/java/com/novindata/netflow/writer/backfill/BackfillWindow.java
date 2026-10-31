package com.novindata.netflow.writer.backfill;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public record BackfillWindow(ZonedDateTime start, ZonedDateTime end) {
    private static final ZoneId TEHRAN = ZoneId.of("Asia/Tehran");

    public BackfillWindow {
        validate(start, end);
    }

    private static void validate(ZonedDateTime start, ZonedDateTime end) {
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("BACKFILL_START must be before BACKFILL_END");
        }
        if (!isFullHour(start) || !isFullHour(end)) {
            throw new IllegalArgumentException(
                    "Backfill times must be full hours, for example 2026-09-28T01:00");
        }
    }

    public static BackfillWindow parse(String start, String end) {
        return new BackfillWindow(parseTehranTime("BACKFILL_START", start), parseTehranTime("BACKFILL_END", end));
    }

    public List<String> hourPaths() {
        List<String> paths = new ArrayList<>();
        for (ZonedDateTime hour = start; hour.isBefore(end); hour = hour.plusHours(1)) {
            paths.add(String.format("%s/hour_%02d", hour.toLocalDate(), hour.getHour()));
        }
        return List.copyOf(paths);
    }

    private static ZonedDateTime parseTehranTime(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    name + " is required; expected value like 2026-09-28T01:00");
        }
        try {
            return LocalDateTime.parse(value).atZone(TEHRAN);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    name + " has invalid format; expected value like 2026-09-28T01:00", e);
        }
    }

    private static boolean isFullHour(ZonedDateTime time) {
        return time.getMinute() == 0 && time.getSecond() == 0 && time.getNano() == 0;
    }
}
