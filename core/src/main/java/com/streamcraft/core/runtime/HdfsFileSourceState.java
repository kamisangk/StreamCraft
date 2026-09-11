package com.streamcraft.core.runtime;
import java.io.Serializable;

record HdfsFileSourceState(String path,String fingerprint,long byteOffset,long lineNumber,String format,boolean headerSeen,boolean completed) implements Serializable {
    private static final long serialVersionUID=1L;

    HdfsFileSourceState {
        if(path==null||path.isBlank()||fingerprint==null||fingerprint.isBlank()||byteOffset<0||lineNumber<0) {
            throw new IllegalArgumentException("Invalid HDFS file source state.");
        }
        format=format==null?"":format.trim();
    }

    void validateFor(String sourceName, String expectedFormat) {
        String normalizedExpected = expectedFormat == null ? "" : expectedFormat.trim();
        if (format.isBlank() || !format.equalsIgnoreCase(normalizedExpected)) {
            throw new IllegalArgumentException(sourceName + " checkpoint state format changed from "
                    + (format.isBlank() ? "<none>" : format) + " to "
                    + (normalizedExpected.isBlank() ? "<none>" : normalizedExpected) + ".");
        }
    }
}
