package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class JdbcSourceSplitSerializer implements SimpleVersionedSerializer<JdbcSourceSplit> {

    private static final int VERSION = 2;
    private static final int LEGACY_VERSION = 1;
    private final IncrementalSourceStateSerializer stateSerializer = new IncrementalSourceStateSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(JdbcSourceSplit split) throws IOException {
        if (split == null) {
            throw new IOException("JDBC source split must not be null.");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(VERSION);
            writeString(output, split.splitId());
            byte[] state = stateSerializer.serialize(split.state());
            output.writeInt(state.length);
            output.write(state);
        }
        return bytes.toByteArray();
    }

    @Override
    public JdbcSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != LEGACY_VERSION && version != VERSION) {
            throw new IOException("Unsupported JDBC source split version: " + version);
        }
        if (serialized == null) {
            throw new IOException("JDBC source split must not be null.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            if (input.readUnsignedByte() != version) {
                throw new IOException("Invalid JDBC source split version marker.");
            }
            String splitId = IncrementalSourceStateSerializerSupport.readString(input, "JDBC source split id");
            int stateLength = input.readInt();
            if (stateLength < 0 || stateLength > input.available()) {
                throw new IOException("Invalid JDBC source split state length.");
            }
            byte[] stateBytes = input.readNBytes(stateLength);
            if (stateBytes.length != stateLength) {
                throw new IOException("Truncated JDBC source split state.");
            }
            JdbcSourceSplit split = new JdbcSourceSplit(
                    splitId, stateSerializer.deserialize(stateVersion(stateBytes), stateBytes));
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in JDBC source split.");
            }
            return split;
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid JDBC source split.", exception);
        }
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static int stateVersion(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IOException("Missing JDBC source split state version.");
        }
        return bytes[0] & 0xff;
    }
}
