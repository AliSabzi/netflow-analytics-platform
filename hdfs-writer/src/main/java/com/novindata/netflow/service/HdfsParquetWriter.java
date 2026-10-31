package com.novindata.netflow.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.UUID;

@Service
public class HdfsParquetWriter {
    private static final Logger log = LoggerFactory.getLogger(HdfsParquetWriter.class);
    private final Configuration conf = new Configuration();
    private final HdfsPathHelper pathHelper;

    public HdfsParquetWriter(HdfsPathHelper pathHelper) {
        this.pathHelper = pathHelper;
    }

    public String newBatchId() {
        return "netflow_batch_" + UUID.randomUUID();
    }

    public ParquetWriter<GenericRecord> openWriter(String hourPath, String batchId , Schema schema) throws IOException {
        Path directory = pathHelper.hourDirectory(hourPath);
        FileSystem fs = directory.getFileSystem(conf);
        if (!fs.exists(directory)) fs.mkdirs(directory);

        return AvroParquetWriter.<GenericRecord>builder(pathHelper.tempPath(hourPath, batchId))
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(128 * 1024 * 1024)
                .withPageSize(1024 * 1024)
                .withDictionaryEncoding(true)
                .build();
    }

    public void commit(String hourPath, String batchId, int writtenCount) throws IOException {
        Path tempPath = pathHelper.tempPath(hourPath, batchId);
        FileSystem fs = tempPath.getFileSystem(conf);

        if (!fs.exists(tempPath)) {
            hourPath = hourPath.replace("_processing", "_ready");
            tempPath = pathHelper.tempPath(hourPath, batchId);
        }

        if (writtenCount == 0) {
            fs.delete(tempPath, false);
            log.warn("No records were written; discarded temp file {}", tempPath);
            return;
        }

        Path finalPath = pathHelper.finalPath(hourPath, batchId);

        if (fs.rename(tempPath, finalPath)) {
            log.info("Successfully committed {} records to HDFS at {}", writtenCount, finalPath);
        } else {
            throw new IOException("Failed to rename temporary file " + tempPath + " to " + finalPath);
        }
    }

    public void abort(String hourPath, String batchId) {
        try {
            Path tempPath = pathHelper.tempPath(hourPath, batchId);
            FileSystem fs = tempPath.getFileSystem(conf);
            fs.delete(tempPath, false);
            log.warn("Aborted batch {}; temp file deleted.", batchId);
        } catch (IOException e) {
            log.error("Failed to delete temp file for aborted batch {}", batchId, e);
        }
    }
}
