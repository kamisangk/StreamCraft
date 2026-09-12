package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.List;

/** Immutable checkpoint output description consumed by the HDFS committer. */
record HdfsFileSinkCommittable(
        HdfsFileSinkTransaction transaction,
        List<FileCommit> files) implements Serializable {

    private static final long serialVersionUID = 1L;

    HdfsFileSinkCommittable {
        if (transaction == null) {
            throw new IllegalArgumentException("HDFS sink committable transaction must not be null.");
        }
        files = files == null ? List.of() : List.copyOf(files);
        for (FileCommit file : files) {
            if (file == null || !transaction.equals(file.transaction())) {
                throw new IllegalArgumentException("HDFS sink committable file transaction does not match.");
            }
        }
    }

    record FileCommit(
            HdfsFileSinkTransaction transaction,
            String temporaryPath,
            String finalPath,
            String partition) implements Serializable {

        private static final long serialVersionUID = 1L;

        FileCommit {
            if (transaction == null) {
                throw new IllegalArgumentException("HDFS sink file transaction must not be null.");
            }
            if (temporaryPath == null || temporaryPath.isBlank()) {
                throw new IllegalArgumentException("HDFS sink temporary path must not be blank.");
            }
            if (finalPath == null || finalPath.isBlank()) {
                throw new IllegalArgumentException("HDFS sink final path must not be blank.");
            }
            temporaryPath = temporaryPath.trim();
            finalPath = finalPath.trim();
            partition = partition == null ? "" : partition.trim();
        }
    }
}
