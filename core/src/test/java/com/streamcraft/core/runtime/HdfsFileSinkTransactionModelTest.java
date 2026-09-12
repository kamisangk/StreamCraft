package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class HdfsFileSinkTransactionModelTest {

    @Test
    void transactionDirectoryIsDeterministicAndCheckpointScoped() {
        HdfsFileSinkTransaction first = new HdfsFileSinkTransaction("job-a", 2, 17);
        HdfsFileSinkTransaction same = new HdfsFileSinkTransaction("job-a", 2, 17);
        HdfsFileSinkTransaction next = new HdfsFileSinkTransaction("job-a", 2, 18);

        assertEquals(first, same);
        assertEquals("job-a-2-17", first.transactionId());
        assertEquals("/tmp/streamcraft/job-a-2-17", first.directory("/tmp/streamcraft").toString());
        assertNotEquals(first.transactionId(), next.transactionId());
    }

    @Test
    void committableAndStateAreImmutableAndKeepTempToFinalMapping() {
        HdfsFileSinkTransaction transaction = new HdfsFileSinkTransaction("job-a", 0, 3);
        HdfsFileSinkCommittable.FileCommit file = new HdfsFileSinkCommittable.FileCommit(
                transaction, "/tmp/job-a-0-3/part-0.csv", "/warehouse/orders/part-0.csv", "dt=2026-09-12");
        HdfsFileSinkCommittable committable = new HdfsFileSinkCommittable(transaction, List.of(file));
        HdfsFileSinkState state = new HdfsFileSinkState(List.of(committable));

        assertEquals(file, committable.files().get(0));
        assertEquals(List.of(committable), state.pendingTransactions());
        assertThrows(UnsupportedOperationException.class, () -> state.pendingTransactions().add(committable));
        assertThrows(UnsupportedOperationException.class, () -> committable.files().clear());
    }

    @Test
    void stateSerializerRoundTripsTransactionMappings() throws Exception {
        HdfsFileSinkTransaction transaction = new HdfsFileSinkTransaction("job-a", 1, 9);
        HdfsFileSinkCommittable stateEntry = new HdfsFileSinkCommittable(
                transaction,
                List.of(new HdfsFileSinkCommittable.FileCommit(
                        transaction, "hdfs://nn/tmp/part", "hdfs://nn/final/part", "region=cn")));
        HdfsFileSinkState state = new HdfsFileSinkState(List.of(stateEntry));
        HdfsFileSinkStateSerializer serializer = new HdfsFileSinkStateSerializer();

        HdfsFileSinkState restored = serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertEquals(state, restored);
    }
}
