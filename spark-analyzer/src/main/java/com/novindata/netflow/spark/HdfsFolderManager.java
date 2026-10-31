package com.novindata.netflow.spark;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HdfsFolderManager {

    private final Configuration configuration;

    public HdfsFolderManager(Configuration configuration) {
        this.configuration = configuration;
    }

    public List<Path> getReadyFolders(String rootPath) throws IOException {

        List<Path> readyFolders = new ArrayList<>();
        Path root = new Path(rootPath);
        FileSystem fs = root.getFileSystem(configuration);

        if (!fs.exists(root)) {
            throw new IOException("Root path does not exist: " + root);
        }

        for (FileStatus dateFolder : fs.listStatus(root)) {

            if (!dateFolder.isDirectory()) {
                continue;
            }

            for (FileStatus hourFolder : fs.listStatus(dateFolder.getPath())) {

                if (hourFolder.isDirectory() && hourFolder.getPath().getName().endsWith("_ready")) {
                    readyFolders.add(hourFolder.getPath());
                }
            }
        }
        Collections.sort(readyFolders);
        return readyFolders;
    }

    public void markProcessed(Path readyPath) throws IOException {

        FileSystem fs = readyPath.getFileSystem(configuration);
        String processedName = readyPath.getName().replace("_ready","_processed");
        Path processedPath = new Path(readyPath.getParent(),processedName);

        if (!fs.rename(readyPath,processedPath)) {
            throw new IOException("Failed to rename " + readyPath + " to "+ processedPath);
        }
    }

    public static LocalDateTime getBucketHour(Path readyPath) {
        String date = readyPath.getParent().getName();
        String hourFolder = readyPath.getName();
        int hour = Integer.parseInt(hourFolder.split("_")[1]);
        return LocalDate.parse(date).atTime(hour, 0);
    }

    public boolean isEmpty(Path path) throws IOException {
        FileSystem fs = path.getFileSystem(configuration);
        for (FileStatus file : fs.listStatus(path)) {
            String name = file.getPath().getName();
            if (!name.endsWith(".tmp")) {
                return false;
            }
        }
        return true;
    }

    public int countParquetFiles(Path path) throws IOException {
        int count = 0;
        for (FileStatus file : path.getFileSystem(configuration).listStatus(path)) {
            if (file.isFile() && file.getPath().getName().endsWith(".parquet")) {
                count++;
            }
        }
        return count;
    }
}
