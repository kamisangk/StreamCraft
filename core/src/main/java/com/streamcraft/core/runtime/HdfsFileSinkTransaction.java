package com.streamcraft.core.runtime;

import java.io.Serializable;
import org.apache.flink.core.fs.Path;

/** Identifies one checkpoint-scoped HDFS transaction for one sink subtask. */
record HdfsFileSinkTransaction(String jobId, int subtaskId, long checkpointId) implements Serializable {

    private static final long serialVersionUID = 1L;

    HdfsFileSinkTransaction {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("HDFS sink transaction jobId must not be blank.");
        }
        if (subtaskId < 0) {
            throw new IllegalArgumentException("HDFS sink transaction subtaskId must not be negative.");
        }
        if (checkpointId < 0) {
            throw new IllegalArgumentException("HDFS sink transaction checkpointId must not be negative.");
        }
        jobId = jobId.trim();
    }

    String transactionId() {
        return jobId + "-" + subtaskId + "-" + checkpointId;
    }

    Path directory(String tmpRoot) {
        if (tmpRoot == null || tmpRoot.isBlank()) {
            throw new IllegalArgumentException("HDFS sink transaction tmpRoot must not be blank.");
        }
        String root = tmpRoot.trim();
        return new Path(root.endsWith("/") ? root + transactionId() : root + "/" + transactionId());
    }
}
