package com.streamcraft.core.runtime;

import java.io.IOException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Publishes prepared HDFS files after a checkpoint has completed. */
final class HdfsFileSinkCommitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileSinkCommitter.class);

    private final String tmpRoot;

    HdfsFileSinkCommitter(String tmpRoot) {
        if (tmpRoot == null || tmpRoot.isBlank()) {
            throw new IllegalArgumentException("HDFS sink committer tmpRoot must not be blank.");
        }
        this.tmpRoot = tmpRoot.trim();
    }

    void commit(HdfsFileSinkCommittable committable) throws IOException {
        if (committable == null) {
            throw new IllegalArgumentException("HDFS sink committable must not be null.");
        }
        for (HdfsFileSinkCommittable.FileCommit file : committable.files()) {
            renameIdempotently(file);
        }
        cleanup(committable.transaction());
    }

    void abort(HdfsFileSinkCommittable committable) throws IOException {
        if (committable == null) {
            return;
        }
        cleanup(committable.transaction());
    }

    private void renameIdempotently(HdfsFileSinkCommittable.FileCommit file) throws IOException {
        Path temporary = new Path(file.temporaryPath());
        Path target = new Path(file.finalPath());
        FileSystem temporaryFileSystem = temporary.getFileSystem();
        FileSystem targetFileSystem = target.getFileSystem();
        boolean temporaryExists = temporaryFileSystem.exists(temporary);
        boolean targetExists = targetFileSystem.exists(target);
        if (!temporaryExists && targetExists) {
            return;
        }
        if (temporaryExists && targetExists) {
            if (!temporaryFileSystem.delete(temporary, false)) {
                throw new IOException("HDFS sink could not remove duplicate temporary file: " + temporary);
            }
            return;
        }
        if (!temporaryExists) {
            throw new IOException("HDFS sink temporary file is missing before commit: " + temporary);
        }
        Path parent = target.getParent();
        if (parent != null) {
            targetFileSystem.mkdirs(parent);
        }
        if (!temporaryFileSystem.rename(temporary, target)) {
            if (!targetFileSystem.exists(target)) {
                throw new IOException("HDFS sink rename failed: " + temporary + " -> " + target);
            }
        }
    }

    private void cleanup(HdfsFileSinkTransaction transaction) throws IOException {
        Path transactionDirectory = transaction.directory(tmpRoot);
        FileSystem fileSystem = transactionDirectory.getFileSystem();
        if (!fileSystem.exists(transactionDirectory)) {
            return;
        }
        try {
            if (!fileSystem.delete(transactionDirectory, true) && fileSystem.exists(transactionDirectory)) {
                throw new IOException("HDFS sink transaction cleanup returned false: " + transactionDirectory);
            }
        } catch (IOException exception) {
            LOGGER.warn("HDFS sink committed transaction cleanup failed: {}", transactionDirectory, exception);
            // Data is already committed. The next retry can safely attempt cleanup again.
        }
    }
}
