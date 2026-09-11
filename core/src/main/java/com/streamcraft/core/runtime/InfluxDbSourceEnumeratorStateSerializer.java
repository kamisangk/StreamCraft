package com.streamcraft.core.runtime;
import java.io.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
final class InfluxDbSourceEnumeratorStateSerializer implements SimpleVersionedSerializer<InfluxDbSourceEnumeratorState> {
    private static final int VERSION = 1; private final InfluxDbSourceSplitSerializer splitSerializer = new InfluxDbSourceSplitSerializer();
    public int getVersion() { return VERSION; }
    public byte[] serialize(InfluxDbSourceEnumeratorState state) throws IOException { ByteArrayOutputStream b = new ByteArrayOutputStream(); try (DataOutputStream o = new DataOutputStream(b)) { o.writeByte(VERSION); o.writeBoolean(state != null && state.pendingSplit() != null); if (state != null && state.pendingSplit() != null) { byte[] s = splitSerializer.serialize(state.pendingSplit()); o.writeInt(s.length); o.write(s); } } return b.toByteArray(); }
    public InfluxDbSourceEnumeratorState deserialize(int version, byte[] data) throws IOException { if (version != VERSION || data == null) throw new IOException("Invalid InfluxDB enumerator state."); try (DataInputStream i = new DataInputStream(new ByteArrayInputStream(data))) { if (i.readUnsignedByte() != VERSION) throw new IOException("Invalid InfluxDB enumerator state marker."); InfluxDbSourceSplit p = null; if (i.readBoolean()) { int n=i.readInt(); if(n<0||n>i.available()) throw new IOException("Invalid InfluxDB enumerator split length."); byte[] s=i.readNBytes(n); if(s.length!=n) throw new IOException("Truncated InfluxDB enumerator split."); p=splitSerializer.deserialize(1,s);} if(i.available()!=0) throw new IOException("Trailing bytes in InfluxDB enumerator state."); return new InfluxDbSourceEnumeratorState(p); } }
}
