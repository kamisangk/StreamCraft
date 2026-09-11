package com.streamcraft.core.runtime;
import java.io.Serializable; import java.util.List; import java.util.Set;
record HdfsFileEnumeratorState(List<HdfsFileSourceSplit> pendingSplits,Set<String> knownSplitIds) implements Serializable { private static final long serialVersionUID=1L; HdfsFileEnumeratorState{pendingSplits=pendingSplits==null?List.of():List.copyOf(pendingSplits);knownSplitIds=knownSplitIds==null?Set.of():Set.copyOf(knownSplitIds);} }
