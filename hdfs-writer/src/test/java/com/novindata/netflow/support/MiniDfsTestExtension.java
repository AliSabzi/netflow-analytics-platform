package com.novindata.netflow.support;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.util.FileSystemUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class MiniDfsTestExtension implements BeforeAllCallback, AfterAllCallback {

    private MiniDFSCluster cluster;
    private Path baseDirectory;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        baseDirectory = Files.createTempDirectory("mini-dfs-");

        Configuration configuration = new HdfsConfiguration(false);
        configuration.addResource("core-default.xml");
        configuration.addResource("hdfs-default.xml");
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDirectory.toString());

        cluster = new MiniDFSCluster.Builder(configuration)
                .numDataNodes(1)
                .build();
        cluster.waitClusterUp();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        try {
            if (cluster != null) {
                cluster.shutdown();
            }
        } finally {
            if (baseDirectory != null) {
                FileSystemUtils.deleteRecursively(baseDirectory);
            }
        }
    }

    public String getUri() {
        return requireRunningCluster().getURI().toString();
    }

    public FileSystem getFileSystem() throws IOException {
        return requireRunningCluster().getFileSystem();
    }

    private MiniDFSCluster requireRunningCluster() {
        if (cluster == null || !cluster.isClusterUp()) {
            throw new IllegalStateException("MiniDFS test cluster is not running");
        }
        return cluster;
    }
}
