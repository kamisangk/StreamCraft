package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class ElasticsearchSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<ElasticsearchSourceEnumeratorState> {
    private static final int VERSION = 3;
    private final ElasticsearchSourceSplitSerializer splitSerializer = new ElasticsearchSourceSplitSerializer();

    @Override public int getVersion() { return VERSION; }

    @Override
    public byte[] serialize(ElasticsearchSourceEnumeratorState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(VERSION);
            List<ElasticsearchSourceSplit> pending = state == null ? List.of() : state.pendingSplits();
            output.writeInt(pending.size());
            for (ElasticsearchSourceSplit candidate : pending) {
                byte[] split = splitSerializer.serialize(candidate);
                output.writeInt(split.length); output.write(split);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public ElasticsearchSourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version < 1 || version > VERSION || serialized == null) {
            throw new IOException("Invalid Elasticsearch source enumerator state version or bytes.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            int marker = input.readUnsignedByte();
            if (marker != version && !(version == 2 && marker == VERSION)) {
                throw new IOException("Invalid Elasticsearch source enumerator state version marker.");
            }
            List<ElasticsearchSourceSplit> pending = new ArrayList<>();
            if (marker < VERSION) {
                if (input.readBoolean()) pending.add(readSplit(input));
            } else {
                int count = input.readInt();
                if (count < 0 || count > input.available()) throw new IOException("Invalid Elasticsearch split count.");
                for (int index = 0; index < count; index++) pending.add(readSplit(input));
            }
            if (input.available() != 0) throw new IOException("Trailing bytes in Elasticsearch source enumerator state.");
            return new ElasticsearchSourceEnumeratorState(pending);
        }
    }

    private ElasticsearchSourceSplit readSplit(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > input.available()) throw new IOException("Invalid Elasticsearch split length.");
        byte[] split = input.readNBytes(length);
        if (split.length != length) throw new IOException("Truncated Elasticsearch split.");
        return splitSerializer.deserialize(split[0] & 0xff, split);
    }
}
