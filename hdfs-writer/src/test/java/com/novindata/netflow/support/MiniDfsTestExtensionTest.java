package com.novindata.netflow.support;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.URI;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MiniDfsTestExtensionTest {

    @RegisterExtension
    static final MiniDfsTestExtension MINI_DFS = new MiniDfsTestExtension();

    @Test
    void startsMiniDfsCluster() {
        URI fileSystemUri = URI.create(MINI_DFS.getUri());

        assertEquals("hdfs", fileSystemUri.getScheme());
    }

    @Test
    void writesAndReadsFile() throws IOException {
        FileSystem fileSystem = MINI_DFS.getFileSystem();
        Path file = new Path("/message.txt");

        try (FSDataOutputStream output = fileSystem.create(file)) {
            output.write("hello MiniDFS".getBytes(UTF_8));
        }

        try (FSDataInputStream input = fileSystem.open(file)) {
            assertEquals("hello MiniDFS", new String(input.readAllBytes(), UTF_8));
        }
    }
}
