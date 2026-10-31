package com.novindata.netflow.service;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class HdfsPathHelper {

    @Value("${app.hdfs.output-path}")
    private String hdfsOutputPath;

    public Path tempPath(String hourPath, String batchId) {
        return new Path(hourDirectory(hourPath), "." + batchId + ".parquet.tmp");
    }

    public Path finalPath(String hourPath, String batchId) {
        return new Path(hourDirectory(hourPath), batchId + ".parquet");
    }

    public Path hourDirectory(String hourPath) {
        return new Path(hdfsOutputPath, hourPath);
    }

    public String getHdfsOutputPath() {
        return hdfsOutputPath;
    }
}