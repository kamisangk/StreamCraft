package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class JdbcSourceEnumeratorStateSerializer implements SimpleVersionedSerializer<JdbcSourceEnumeratorState> {

    private static final int VERSION = 2;
    private static final int LEGACY_VERSION = 1;
    private final JdbcSourceSplitSerializer splitSerializer = new JdbcSourceSplitSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(JdbcSourceEnumeratorState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(VERSION);
            if (state == null || state.pendingSplit() == null) {
                output.writeBoolean(false);
            } else {
                output.writeBoolean(true);
                byte[] split = splitSerializer.serialize(state.pendingSplit());
                output.writeInt(split.length);
                output.write(split);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public JdbcSourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != LEGACY_VERSION && version != VERSION) {
            throw new IOException("Unsupported JDBC source enumerator state version: " + version);
        }
        if (serialized == null) {
            throw new IOException("JDBC source enumerator state must not be null.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            if (input.readUnsignedByte() != version) {
                throw new IOException("Invalid JDBC source enumerator state version marker.");
            }
            JdbcSourceSplit pending = null;
            if (input.readBoolean()) {
                int length = input.readInt();
                if (length < 0 || length > input.available()) {
                    throw new IOException("Invalid JDBC source enumerator split length.");
                }
                byte[] split = input.readNBytes(length);
                if (split.length != length) {
                    throw new IOException("Truncated JDBC source enumerator split.");
                }
                pending = splitSerializer.deserialize(splitVersion(split), split);
            }
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in JDBC source enumerator state.");
            }
            return new JdbcSourceEnumeratorState(pending);
        }
    }

    private static int splitVersion(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IOException("Missing JDBC source enumerator split version.");
        }
        return bytes[0] & 0xff;
    }
}
