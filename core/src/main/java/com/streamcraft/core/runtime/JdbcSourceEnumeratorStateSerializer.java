package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class JdbcSourceEnumeratorStateSerializer implements SimpleVersionedSerializer<JdbcSourceEnumeratorState> {

    private static final int VERSION = 3;
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
            List<JdbcSourceSplit> pending = state == null ? List.of() : state.pendingSplits();
            output.writeInt(pending.size());
            for (JdbcSourceSplit candidate : pending) {
                byte[] split = splitSerializer.serialize(candidate);
                output.writeInt(split.length);
                output.write(split);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public JdbcSourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version < 1 || version > VERSION) {
            throw new IOException("Unsupported JDBC source enumerator state version: " + version);
        }
        if (serialized == null) {
            throw new IOException("JDBC source enumerator state must not be null.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            int marker = input.readUnsignedByte();
            if (marker != version && !(version == 2 && marker == VERSION)) {
                throw new IOException("Invalid JDBC source enumerator state version marker.");
            }
            List<JdbcSourceSplit> pending = new ArrayList<>();
            if (marker < VERSION) {
                if (input.readBoolean()) {
                    pending.add(readSplit(input));
                }
            } else {
                int count = input.readInt();
                if (count < 0 || count > input.available()) {
                    throw new IOException("Invalid JDBC enumerator split count.");
                }
                for (int index = 0; index < count; index++) {
                    pending.add(readSplit(input));
                }
            }
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in JDBC enumerator state.");
            }
            return new JdbcSourceEnumeratorState(pending);
        }
    }

    private JdbcSourceSplit readSplit(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > input.available()) {
            throw new IOException("Invalid JDBC enumerator split length.");
        }
        byte[] split = input.readNBytes(length);
        if (split.length != length) {
            throw new IOException("Truncated JDBC enumerator split.");
        }
        return splitSerializer.deserialize(splitVersion(split), split);
    }

    private static int splitVersion(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IOException("Missing JDBC source enumerator split version.");
        }
        return bytes[0] & 0xff;
    }
}
