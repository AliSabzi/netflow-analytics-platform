package com.novindata.netflow.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class HdfsDirectoryManager {
    private static final Logger log = LoggerFactory.getLogger(HdfsDirectoryManager.class);
    private static final ZoneId ZONE_ID = ZoneId.of("Asia/Tehran");
    private static final Pattern PROCESSING_HOUR = Pattern.compile("hour_(\\d{2})_processing");
    private final Configuration conf = new Configuration();
    private final HdfsPathHelper pathHelper;

    public HdfsDirectoryManager(HdfsPathHelper pathHelper) {
        this.pathHelper = pathHelper;
    }

    public void markHourReady(String hourPath) throws IOException {
        Path processingPath = pathHelper.hourDirectory(hourPath);
        String readyHourPath = hourPath.replace("_processing", "_ready");
        Path readyPath = pathHelper.hourDirectory(readyHourPath);
        FileSystem fs = processingPath.getFileSystem(conf);

        if (fs.rename(processingPath, readyPath)) {
            log.info("Hour folder marked as ready: {} -> {}", processingPath, readyPath);
            return;
        }
        if (!fs.exists(processingPath) && fs.exists(readyPath)) {
            log.info("Hour folder is already ready: {}", readyPath);
            return;
        }
        if (fs.exists(processingPath)) {
            throw new IOException("Failed to rename " + processingPath + " to " + readyPath);
        }
        throw new IOException("Hour folder disappeared before it could be marked ready: " + processingPath);
    }

    public void markHoursReadyBefore(Instant watermark) throws IOException {
        Path root = new Path(pathHelper.getHdfsOutputPath());
        FileSystem fs = root.getFileSystem(conf);
        if (!fs.exists(root)) return;

        for (FileStatus dateFolder : fs.listStatus(root)) {
            if (!dateFolder.isDirectory()) continue;
            LocalDate date = parseDate(dateFolder.getPath().getName());
            if (date == null) continue;

            for (FileStatus hourFolder : fs.listStatus(dateFolder.getPath())) {
                String name = hourFolder.getPath().getName();
                Matcher matcher = PROCESSING_HOUR.matcher(name);
                if (!hourFolder.isDirectory() || !matcher.matches()) continue;

                int hour = Integer.parseInt(matcher.group(1));
                if (hour > 23) continue;
                Instant hourEnd = date.atTime(hour, 0).atZone(ZONE_ID).plusHours(1).toInstant();
                if (!hourEnd.isAfter(watermark)) {
                    markHourReady(date + "/" + name);
                }
            }
        }
    }

    private LocalDate parseDate(String value) {
        try {
            return LocalDate.parse(value);
        } catch (Exception ignored) {
            return null;
        }
    }
}
