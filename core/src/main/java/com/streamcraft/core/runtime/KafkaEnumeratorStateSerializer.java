package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class KafkaEnumeratorStateSerializer implements SimpleVersionedSerializer<KafkaEnumeratorState> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(KafkaEnumeratorState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            List<KafkaPartitionSplit> pendingSplits = state.pendingSplits().stream()
                    .sorted(Comparator.comparing(KafkaPartitionSplit::topic)
                            .thenComparingInt(KafkaPartitionSplit::partition)
                            .thenComparingLong(KafkaPartitionSplit::nextOffset))
                    .toList();
            output.writeInt(pendingSplits.size());
            for (KafkaPartitionSplit split : pendingSplits) {
                KafkaPartitionSplitSerializer.writeString(output, split.topic());
                output.writeInt(split.partition());
                output.writeLong(split.nextOffset());
            }
            List<String> knownSplitIds = state.knownSplitIds().stream().sorted().toList();
            output.writeInt(knownSplitIds.size());
            for (String splitId : knownSplitIds) {
                KafkaPartitionSplitSerializer.writeString(output, splitId);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public KafkaEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        KafkaPartitionSplitSerializer.requireVersion(version);
        if (serialized == null) {
            throw new IOException("Kafka enumerator state must not be null.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            int splitCount = readCount(input, "split");
            List<KafkaPartitionSplit> splits = new ArrayList<>(splitCount);
            for (int index = 0; index < splitCount; index++) {
                splits.add(new KafkaPartitionSplit(
                        KafkaPartitionSplitSerializer.inputString(input), input.readInt(), input.readLong()));
            }
            int knownCount = readCount(input, "known split");
            Set<String> knownIds = new LinkedHashSet<>();
            for (int index = 0; index < knownCount; index++) {
                knownIds.add(KafkaPartitionSplitSerializer.inputString(input));
            }
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in Kafka enumerator state.");
            }
            return new KafkaEnumeratorState(splits, knownIds);
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid Kafka enumerator state.", exception);
        }
    }

    private static int readCount(DataInputStream input, String name) throws IOException {
        int count = input.readInt();
        if (count < 0 || count > input.available()) {
            throw new IOException("Invalid " + name + " count in Kafka enumerator state.");
        }
        return count;
    }
}
