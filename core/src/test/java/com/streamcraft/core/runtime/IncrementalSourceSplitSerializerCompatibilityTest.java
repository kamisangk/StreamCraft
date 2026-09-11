package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class IncrementalSourceSplitSerializerCompatibilityTest {

    @Test
    void jdbcSplitReadsVersionOneFixtureAndRoundTripsVersionTwoState() throws Exception {
        JdbcSourceSplit restored = new JdbcSourceSplitSerializer().deserialize(1, jdbcSplitFixture());
        assertEquals(1, restored.state().stateVersion());
        assertEquals(null, restored.state().cursorField());

        JdbcSourceSplit current = new JdbcSourceSplit(
                "jdbc", new IncrementalSourceState("LONG", "updated_at", "10", "id", "42"));
        JdbcSourceSplit roundTrip = new JdbcSourceSplitSerializer().deserialize(2, new JdbcSourceSplitSerializer().serialize(current));
        assertEquals("updated_at", roundTrip.state().cursorField());
    }

    @Test
    void elasticsearchSplitReadsVersionOneFixtureAndRoundTripsVersionTwoState() throws Exception {
        ElasticsearchSourceSplit restored = new ElasticsearchSourceSplitSerializer().deserialize(1, elasticsearchSplitFixture());
        assertEquals(1, restored.state().stateVersion());
        assertEquals("", restored.scrollId());

        ElasticsearchSourceSplit current = new ElasticsearchSourceSplit(
                "es", new IncrementalSourceState("LONG", "updated_at", "10", "id", "42"), "scroll", 3);
        ElasticsearchSourceSplit roundTrip = new ElasticsearchSourceSplitSerializer().deserialize(2, new ElasticsearchSourceSplitSerializer().serialize(current));
        assertEquals("updated_at", roundTrip.state().cursorField());
        assertEquals(3, roundTrip.pageIndex());
    }

    @Test
    void influxSplitReadsVersionOneFixtureAndRoundTripsVersionTwoState() throws Exception {
        InfluxDbSourceSplit restored = new InfluxDbSourceSplitSerializer().deserialize(1, influxSplitFixture());
        assertEquals(1, restored.state().stateVersion());
        assertEquals(0, restored.pageIndex());

        InfluxDbSourceSplit current = new InfluxDbSourceSplit(
                "influx", new IncrementalSourceState("TIMESTAMP", "time", "10", "host", "a"), 2);
        InfluxDbSourceSplit roundTrip = new InfluxDbSourceSplitSerializer().deserialize(2, new InfluxDbSourceSplitSerializer().serialize(current));
        assertEquals("time", roundTrip.state().cursorField());
        assertEquals(2, roundTrip.pageIndex());
    }

    @Test
    void enumeratorSerializersReadVersionOneFixtures() throws Exception {
        JdbcSourceSplit jdbc = new JdbcSourceEnumeratorStateSerializer().deserialize(1, enumeratorFixture(jdbcSplitFixture())).pendingSplit();
        ElasticsearchSourceSplit elasticsearch = new ElasticsearchSourceEnumeratorStateSerializer().deserialize(1, enumeratorFixture(elasticsearchSplitFixture())).pendingSplit();
        InfluxDbSourceSplit influx = new InfluxDbSourceEnumeratorStateSerializer().deserialize(1, enumeratorFixture(influxSplitFixture())).pendingSplit();

        assertEquals(1, jdbc.state().stateVersion());
        assertEquals(1, elasticsearch.state().stateVersion());
        assertEquals(1, influx.state().stateVersion());
    }

    private static byte[] jdbcSplitFixture() throws IOException {
        return splitFixture("jdbc", legacyStateFixture(), null, 0, false, false);
    }

    private static byte[] elasticsearchSplitFixture() throws IOException {
        return splitFixture("es", legacyStateFixture(), "", 0, true, true);
    }

    private static byte[] influxSplitFixture() throws IOException {
        return splitFixture("influx", legacyStateFixture(), null, 0, false, true);
    }

    private static byte[] splitFixture(
            String splitId, byte[] state, String scrollId, long pageIndex, boolean elasticsearch, boolean hasPageIndex)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(1);
            writeString(output, splitId);
            output.writeInt(state.length);
            output.write(state);
            if (elasticsearch) {
                writeString(output, scrollId);
            }
            if (hasPageIndex) {
                output.writeLong(pageIndex);
            }
        }
        return bytes.toByteArray();
    }

    private static byte[] enumeratorFixture(byte[] split) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(1);
            output.writeBoolean(true);
            output.writeInt(split.length);
            output.write(split);
        }
        return bytes.toByteArray();
    }

    private static byte[] legacyStateFixture() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(1);
            writeNullableString(output, "LONG");
            writeNullableString(output, "10");
            writeNullableString(output, "id");
            writeNullableString(output, "42");
        }
        return bytes.toByteArray();
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void writeNullableString(DataOutputStream output, String value) throws IOException {
        if (value == null) {
            output.writeInt(-1);
            return;
        }
        writeString(output, value);
    }
}
