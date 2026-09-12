package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HdfsFileSourceSplitRecoveryTest {
    @Test
    void splitSerializerPreservesFingerprintAndOffsets() throws Exception {
        HdfsFileSourceSplit original = new HdfsFileSourceSplit(
                "hdfs://nn/data/events-1:42:100",
                new HdfsFileSourceState(
                        "hdfs://nn/data/events-1", "hdfs://nn/data/events-1:42:100", 42, 7,
                        "JSON", true, false));
        HdfsFileSourceSplitSerializer serializer = new HdfsFileSourceSplitSerializer();

        HdfsFileSourceSplit restored = serializer.deserialize(
                serializer.getVersion(), serializer.serialize(original));

        assertEquals(original, restored);
        assertEquals(42, restored.state().byteOffset());
        assertEquals(7, restored.state().lineNumber());
    }

    @Test
    void completedSplitHasStableIdentityForRestoreFiltering() {
        HdfsFileSourceSplit completed = new HdfsFileSourceSplit(
                "file-a:10:20", new HdfsFileSourceState("file-a", "file-a:10:20", 20, 2,
                "CSV", true, true));
        HdfsFileEnumeratorState state = new HdfsFileEnumeratorState(List.of(completed), Set.of(completed.splitId()));

        assertEquals("file-a:10:20", state.pendingSplits().get(0).splitId());
        assertEquals(true, state.pendingSplits().get(0).state().completed());
        assertEquals(true, state.knownSplitIds().contains(completed.splitId()));
    }
}
