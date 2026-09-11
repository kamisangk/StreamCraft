package com.streamcraft.core.runtime;
import java.io.Serializable; import java.util.Objects; import org.apache.flink.api.connector.source.SourceSplit;
record HdfsFileSourceSplit(String splitId,HdfsFileSourceState state) implements SourceSplit,Serializable { private static final long serialVersionUID=1L; HdfsFileSourceSplit{splitId=Objects.requireNonNull(splitId);state=Objects.requireNonNull(state);if(splitId.isBlank())throw new IllegalArgumentException("HDFS split id must not be blank.");} }
