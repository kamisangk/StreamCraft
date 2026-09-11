package com.streamcraft.core.runtime;

import java.io.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class InfluxDbSourceSplitSerializer implements SimpleVersionedSerializer<InfluxDbSourceSplit> {
    private static final int VERSION = 1;
    private final IncrementalSourceStateSerializer stateSerializer = new IncrementalSourceStateSerializer();
    public int getVersion() { return VERSION; }
    public byte[] serialize(InfluxDbSourceSplit split) throws IOException {
        if (split == null) throw new IOException("InfluxDB source split must not be null.");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(VERSION); writeString(out, split.splitId());
            byte[] state = stateSerializer.serialize(split.state()); out.writeInt(state.length); out.write(state);
            out.writeLong(split.pageIndex());
        }
        return bytes.toByteArray();
    }
    public InfluxDbSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION || serialized == null) throw new IOException("Invalid InfluxDB source split version or bytes.");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            if (in.readUnsignedByte() != VERSION) throw new IOException("Invalid InfluxDB source split version marker.");
            String id = IncrementalSourceStateSerializerSupport.readString(in, "InfluxDB split id");
            int length = in.readInt(); if (length < 0 || length > in.available()) throw new IOException("Invalid InfluxDB split state length.");
            byte[] state = in.readNBytes(length); if (state.length != length) throw new IOException("Truncated InfluxDB split state.");
            InfluxDbSourceSplit result = new InfluxDbSourceSplit(id, stateSerializer.deserialize(1, state), in.readLong());
            if (in.available() != 0) throw new IOException("Trailing bytes in InfluxDB source split.");
            return result;
        } catch (IllegalArgumentException ex) { throw new IOException("Invalid InfluxDB source split.", ex); }
    }
    private static void writeString(DataOutputStream out, String value) throws IOException { byte[] b = value.getBytes(java.nio.charset.StandardCharsets.UTF_8); out.writeInt(b.length); out.write(b); }
}
