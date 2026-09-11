package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** Explicit binary serializer for cursor checkpoint state. */
final class IncrementalSourceStateSerializer implements SimpleVersionedSerializer<IncrementalSourceState> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    byte[] encode(IncrementalSourceState state) throws IOException {
        return serialize(state);
    }

    IncrementalSourceState decode(int version, byte[] serialized) throws IOException {
        return deserialize(version, serialized);
    }

    @Override
    public byte[] serialize(IncrementalSourceState state) throws IOException {
        if (state == null) {
            throw new IOException("Incremental source state must not be null.");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(VERSION);
            writeNullableString(output, state.cursorType());
            writeNullableString(output, state.cursorValue());
            writeNullableString(output, state.uniqueIdField());
            writeNullableString(output, state.uniqueIdValue());
        }
        return bytes.toByteArray();
    }

    @Override
    public IncrementalSourceState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported incremental source state version: " + version);
        }
        if (serialized == null) {
            throw new IOException("Incremental source state must not be null.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            if (input.readUnsignedByte() != VERSION) {
                throw new IOException("Invalid incremental source state version marker.");
            }
            IncrementalSourceState state = new IncrementalSourceState(
                    readNullableString(input),
                    readNullableString(input),
                    readNullableString(input),
                    readNullableString(input));
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in incremental source state.");
            }
            return state;
        } catch (IllegalArgumentException | CharacterCodingException exception) {
            throw new IOException("Invalid incremental source state.", exception);
        }
    }

    private static void writeNullableString(DataOutputStream output, String value) throws IOException {
        if (value == null) {
            output.writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readNullableString(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > input.available()) {
            throw new IOException("Invalid string length in incremental source state.");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException("Truncated string in incremental source state.");
        }
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }
}
