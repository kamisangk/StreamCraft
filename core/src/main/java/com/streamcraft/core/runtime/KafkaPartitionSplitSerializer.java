package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class KafkaPartitionSplitSerializer implements SimpleVersionedSerializer<KafkaPartitionSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(KafkaPartitionSplit split) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeString(output, split.topic());
            output.writeInt(split.partition());
            output.writeLong(split.nextOffset());
        }
        return bytes.toByteArray();
    }

    @Override
    public KafkaPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        requireVersion(version);
        if (serialized == null) {
            throw new IOException("Kafka partition split state must not be null.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            KafkaPartitionSplit split = new KafkaPartitionSplit(inputString(input), input.readInt(), input.readLong());
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in Kafka partition split state.");
            }
            return split;
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid Kafka partition split state.", exception);
        }
    }

    static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    static String inputString(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > input.available()) {
            throw new IOException("Invalid string length in Kafka source state.");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException("Truncated string in Kafka source state.");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static void requireVersion(int version) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported Kafka source state version: " + version);
        }
    }
}
