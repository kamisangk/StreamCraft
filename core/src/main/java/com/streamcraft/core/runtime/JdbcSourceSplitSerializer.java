package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class JdbcSourceSplitSerializer implements SimpleVersionedSerializer<JdbcSourceSplit> {

    private static final int VERSION = 3;
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
            writeString(output, split.rangeColumn());
            writeString(output, split.lowerBound());
            writeString(output, split.upperBound());
            output.writeBoolean(split.upperInclusive());
        }
        return bytes.toByteArray();
    }

    @Override
    public JdbcSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version < 1 || version > VERSION) {
            throw new IOException("Unsupported JDBC source split version: " + version);
        }
        if (serialized == null) {
            throw new IOException("JDBC source split must not be null.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            int marker = input.readUnsignedByte();
            if (marker != version && !(version == 2 && marker == VERSION)) {
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
            JdbcSourceSplit split;
            if (marker == VERSION) {
                String rangeColumn = readString(input, "JDBC range column");
                String lowerBound = readString(input, "JDBC lower bound");
                String upperBound = readString(input, "JDBC upper bound");
                boolean upperInclusive = input.readBoolean();
                split = new JdbcSourceSplit(splitId, stateSerializer.deserialize(stateVersion(stateBytes), stateBytes),
                        rangeColumn, lowerBound, upperBound, upperInclusive);
            } else {
                split = new JdbcSourceSplit(splitId, stateSerializer.deserialize(stateVersion(stateBytes), stateBytes));
            }
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

    private static String readString(DataInputStream input, String field) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > input.available()) {
            throw new IOException("Invalid " + field + " length.");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException("Truncated " + field + ".");
        }
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static int stateVersion(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IOException("Missing JDBC source split state version.");
        }
        return bytes[0] & 0xff;
    }
}
