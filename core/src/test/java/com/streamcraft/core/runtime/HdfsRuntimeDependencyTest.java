package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

class HdfsRuntimeDependencyTest {

    @Test
    void hdfsSchemeResolvesWithoutExternalHadoopInstallation() throws Exception {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://127.0.0.1:8020/"));
        assertEquals("hdfs", fileSystem.getUri().getScheme());
    }

    @Test
    @EnabledIfSystemProperty(named = "streamcraft.hdfs.integration", matches = "true")
    void liveHdfsWriteReadAndRename() throws Exception {
        String endpoint = System.getProperty("streamcraft.hdfs.endpoint", "hdfs://192.168.31.191:8020");
        Path directory = new Path(endpoint + "/tmp/streamcraft_review_" + UUID.randomUUID());
        FileSystem fileSystem = directory.getFileSystem();
        Path temporary = new Path(directory, "pending.json");
        Path committed = new Path(directory, "committed.json");
        try {
            fileSystem.mkdirs(directory);
            try (var output = fileSystem.create(temporary, FileSystem.WriteMode.NO_OVERWRITE)) {
                output.write("{\"id\":1}\n".getBytes(StandardCharsets.UTF_8));
            }
            assertTrue(fileSystem.rename(temporary, committed));
            try (var input = fileSystem.open(committed)) {
                assertEquals("{\"id\":1}\n", new String(input.readAllBytes(), StandardCharsets.UTF_8));
            }
        } finally {
            assertTrue(fileSystem.delete(directory, true));
        }
    }
}
