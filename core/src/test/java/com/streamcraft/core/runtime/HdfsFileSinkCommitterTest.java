package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.junit.jupiter.api.Test;

class HdfsFileSinkCommitterTest {

    @Test
    void commitRenamesTemporaryFileAndCanBeRetried() throws Exception {
        java.nio.file.Path root = Files.createTempDirectory("streamcraft-hdfs-commit-");
        try {
            String tmpRoot = root.resolve("tmp").toUri().toString();
            String finalRoot = root.resolve("final").toUri().toString();
            HdfsFileSinkTransaction transaction = new HdfsFileSinkTransaction("job", 0, 4);
            String temporary = transaction.directory(tmpRoot).toString() + "/part-0.json";
            String target = finalRoot + "/part-0.json";
            write(temporary, "{\"id\":1}\n");
            HdfsFileSinkCommittable committable = new HdfsFileSinkCommittable(
                    transaction,
                    List.of(new HdfsFileSinkCommittable.FileCommit(transaction, temporary, target, "")));
            HdfsFileSinkCommitter committer = new HdfsFileSinkCommitter(tmpRoot);

            committer.commit(committable);
            assertTrue(Files.exists(java.nio.file.Path.of(java.net.URI.create(target))));
            assertFalse(Files.exists(java.nio.file.Path.of(java.net.URI.create(temporary))));

            committer.commit(committable);
            assertTrue(Files.exists(java.nio.file.Path.of(java.net.URI.create(target))));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void abortRemovesOnlyTheTransactionTemporaryDirectory() throws Exception {
        java.nio.file.Path root = Files.createTempDirectory("streamcraft-hdfs-abort-");
        try {
            String tmpRoot = root.resolve("tmp").toUri().toString();
            HdfsFileSinkTransaction aborted = new HdfsFileSinkTransaction("job", 0, 5);
            HdfsFileSinkTransaction retained = new HdfsFileSinkTransaction("job", 0, 6);
            String abortedPath = aborted.directory(tmpRoot).toString() + "/part-a.txt";
            String retainedPath = retained.directory(tmpRoot).toString() + "/part-b.txt";
            write(abortedPath, "aborted");
            write(retainedPath, "retained");

            HdfsFileSinkCommitter committer = new HdfsFileSinkCommitter(tmpRoot);
            committer.abort(new HdfsFileSinkCommittable(
                    aborted,
                    List.of(new HdfsFileSinkCommittable.FileCommit(aborted, abortedPath, "/unused", ""))));

            assertFalse(Files.exists(java.nio.file.Path.of(java.net.URI.create(abortedPath))));
            assertTrue(Files.exists(java.nio.file.Path.of(java.net.URI.create(retainedPath))));
        } finally {
            deleteRecursively(root);
        }
    }

    private static void write(String path, String value) throws Exception {
        org.apache.flink.core.fs.Path file = new org.apache.flink.core.fs.Path(path);
        FileSystem fileSystem = file.getFileSystem();
        fileSystem.mkdirs(file.getParent());
        try (FSDataOutputStream output = fileSystem.create(file, FileSystem.WriteMode.OVERWRITE)) {
            output.write(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    private static void deleteRecursively(java.nio.file.Path path) throws Exception {
        if (Files.exists(path)) {
            try (var stream = Files.walk(path)) {
                stream.sorted(java.util.Comparator.reverseOrder()).forEach(item -> {
                    try {
                        Files.deleteIfExists(item);
                    } catch (java.io.IOException exception) {
                        throw new RuntimeException(exception);
                    }
                });
            }
        }
    }
}
