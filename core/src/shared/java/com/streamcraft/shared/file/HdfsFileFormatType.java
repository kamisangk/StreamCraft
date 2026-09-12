package com.streamcraft.shared.file;

public enum HdfsFileFormatType {
    JSON,
    CSV,
    TEXT,
    PARQUET,
    ORC,
    EXCEL,
    XML,
    BINARY;

    public boolean runtimeSupported() {
        return this == JSON || this == CSV || this == TEXT;
    }
}
