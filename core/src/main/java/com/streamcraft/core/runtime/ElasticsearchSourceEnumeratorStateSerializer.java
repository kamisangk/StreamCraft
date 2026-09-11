package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class ElasticsearchSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<ElasticsearchSourceEnumeratorState> {

    private static final int VERSION = 1;
    private final ElasticsearchSourceSplitSerializer splitSerializer = new ElasticsearchSourceSplitSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ElasticsearchSourceEnumeratorState state) throws IOException {
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
    public ElasticsearchSourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION || serialized == null) {
            throw new IOException("Invalid Elasticsearch source enumerator state version or bytes.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            if (input.readUnsignedByte() != VERSION) {
                throw new IOException("Invalid Elasticsearch source enumerator state version marker.");
            }
            ElasticsearchSourceSplit pending = null;
            if (input.readBoolean()) {
                int length = input.readInt();
                if (length < 0 || length > input.available()) {
                    throw new IOException("Invalid Elasticsearch enumerator split length.");
                }
                byte[] split = input.readNBytes(length);
                if (split.length != length) {
                    throw new IOException("Truncated Elasticsearch enumerator split.");
                }
                pending = splitSerializer.deserialize(1, split);
            }
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in Elasticsearch source enumerator state.");
            }
            return new ElasticsearchSourceEnumeratorState(pending);
        }
    }
}
