package com.novindata.netflow.writer.backfill;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HdfsBackfillSource {
    private final Configuration configuration = new Configuration();
    private final Path rootPath;

    public HdfsBackfillSource(String rootPath) {
        this.rootPath = new Path(rootPath);
    }

    public List<String> findFinalizedFolders(BackfillWindow window) throws IOException {
        FileSystem fileSystem = rootPath.getFileSystem(configuration);
        List<String> folders = new ArrayList<>();
        Map<String, Set<String>> folderNamesByDate = new HashMap<>();

        for (String hourPath : window.hourPaths()) {
            String datePart = hourPath.substring(0, hourPath.indexOf('/'));
            if (!folderNamesByDate.containsKey(datePart)) {
                folderNamesByDate.put(datePart, listFolderNames(fileSystem, datePart));
            }
            folders.add(resolveFinalizedFolder(hourPath, folderNamesByDate.get(datePart)));
        }

        return List.copyOf(folders);
    }

    private Set<String> listFolderNames(FileSystem fileSystem, String datePart) throws IOException {
        Set<String> names = new HashSet<>();
        for (FileStatus status : fileSystem.listStatus(new Path(rootPath, datePart))) {
            if (status.isDirectory()) {
                names.add(status.getPath().getName());
            }
        }
        return names;
    }

    private String resolveFinalizedFolder(String hourPath, Set<String> folderNames) {
        String hourName = hourPath.substring(hourPath.indexOf('/') + 1);
        if (folderNames.contains(hourName + "_processed")) {
            return new Path(rootPath, hourPath + "_processed").toString();
        }
        if (folderNames.contains(hourName + "_ready")) {
            return new Path(rootPath, hourPath + "_ready").toString();
        }

        if (folderNames.contains(hourName + "_processing")) {
            throw new IllegalStateException("HDFS hour is still processing: " + hourPath);
        }

        throw new IllegalStateException("No finalized HDFS folder found for hour: " + hourPath);
    }
}
