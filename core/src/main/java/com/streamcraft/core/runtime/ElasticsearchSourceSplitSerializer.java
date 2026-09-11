package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class ElasticsearchSourceSplitSerializer implements SimpleVersionedSerializer<ElasticsearchSourceSplit> {

    private static final int VERSION = 2;
    private static final int LEGACY_VERSION = 1;
    private final IncrementalSourceStateSerializer stateSerializer = new IncrementalSourceStateSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ElasticsearchSourceSplit split) throws IOException {
        if (split == null) {
            throw new IOException("Elasticsearch source split must not be null.");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(VERSION);
            writeString(output, split.splitId());
            byte[] state = stateSerializer.serialize(split.state());
            output.writeInt(state.length);
            output.write(state);
            writeString(output, split.scrollId());
            output.writeLong(split.pageIndex());
        }
        return bytes.toByteArray();
    }

    @Override
    public ElasticsearchSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != LEGACY_VERSION && version != VERSION) {
            throw new IOException("Unsupported Elasticsearch source split version: " + version);
        }
        if (serialized == null) {
            throw new IOException("Elasticsearch source split must not be null.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            if (input.readUnsignedByte() != version) {
                throw new IOException("Invalid Elasticsearch source split version marker.");
            }
            String splitId = IncrementalSourceStateSerializerSupport.readString(input, "Elasticsearch split id");
            int stateLength = input.readInt();
            if (stateLength < 0 || stateLength > input.available()) {
                throw new IOException("Invalid Elasticsearch split state length.");
            }
            byte[] stateBytes = input.readNBytes(stateLength);
            IncrementalSourceState state = stateSerializer.deserialize(stateVersion(stateBytes), stateBytes);
            String scrollId = IncrementalSourceStateSerializerSupport.readString(input, "Elasticsearch scroll id");
            long pageIndex = input.readLong();
            ElasticsearchSourceSplit result = new ElasticsearchSourceSplit(splitId, state, scrollId, pageIndex);
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in Elasticsearch source split.");
            }
            return result;
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid Elasticsearch source split.", exception);
        }
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static int stateVersion(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IOException("Missing Elasticsearch source split state version.");
        }
        return bytes[0] & 0xff;
    }
}
