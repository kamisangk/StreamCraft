package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class IncrementalSourceStateSerializerTest {

    private final IncrementalSourceStateSerializer serializer = new IncrementalSourceStateSerializer();

    @Test
    void roundTripsACompositeCursorAndOptionalFields() throws Exception {
        IncrementalSourceState state = new IncrementalSourceState(
                "TIMESTAMP", "updated_at", "2026-09-09T10:15:30Z", "event_id", "evt-42");

        IncrementalSourceState restored = serializer.decode(serializer.getVersion(), serializer.encode(state));

        assertEquals(state, restored);
        assertEquals(2, serializer.getVersion());
        assertEquals(true, restored.hasUniqueId());
    }

    @Test
    void roundTripsNullCursorAndNoTieBreaker() throws Exception {
        IncrementalSourceState state = new IncrementalSourceState("STRING", null, null, null, null);

        assertEquals(state, serializer.deserialize(2, serializer.serialize(state)));
    }

    @Test
    void rejectsUnknownVersionNullBytesTrailingBytesAndMalformedFields() throws Exception {
        IncrementalSourceState state = new IncrementalSourceState("LONG", "10", null, null);
        byte[] encoded = serializer.serialize(state);

        assertThrows(IOException.class, () -> serializer.deserialize(3, encoded));
        assertThrows(IOException.class, () -> serializer.deserialize(1, null));
        assertThrows(IOException.class, () -> serializer.deserialize(2, Arrays.copyOf(encoded, encoded.length + 1)));

        byte[] invalidMarker = encoded.clone();
        invalidMarker[0] = 3;
        assertThrows(IOException.class, () -> serializer.deserialize(2, invalidMarker));
    }

    @Test
    void acceptsAnInitialCompositeCursorAndRejectsInvalidUtf8() throws Exception {
        IncrementalSourceState state = new IncrementalSourceState("LONG", null, "10", "id", null);
        assertEquals(state, serializer.deserialize(2, serializer.serialize(state)));
        byte[] encoded = serializer.serialize(state);
        encoded[5] = (byte) 0xC3;
        encoded[6] = (byte) 0x28;
        assertThrows(IOException.class, () -> serializer.deserialize(2, encoded));
    }

    @Test
    void rejectsBlankCursorTypeAndUnboundUniqueId() {
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceState(" ", "10", null, null));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceState("LONG", "10", "", "id"));
    }

    @Test
    void readsFixedVersionOneFixtureWithoutCursorField() throws Exception {
        byte[] fixture = legacyFixture("LONG", "10", "id", "42");
        IncrementalSourceState restored = serializer.deserialize(1, fixture);
        assertEquals(1, restored.stateVersion());
        assertEquals("LONG", restored.cursorType());
        assertEquals("10", restored.cursorValue());
        assertEquals("id", restored.uniqueIdField());
        assertEquals("42", restored.uniqueIdValue());
        assertEquals(null, restored.cursorField());
    }

    @Test
    void validatesCursorFieldAndAllowsLegacyStateUpgrade() throws Exception {
        IncrementalSourceState current = new IncrementalSourceState("LONG", "updated_at", "10", "id", "42");
        current.validateFor("JDBC Source", "LONG", "updated_at", "id");
        assertThrows(IllegalArgumentException.class,
                () -> current.validateFor("JDBC Source", "LONG", "created_at", "id"));

        IncrementalSourceState legacy = serializer.deserialize(1, legacyFixture("LONG", "10", "id", "42"));
        legacy.validateFor("JDBC Source", "LONG", "updated_at", "id");
        assertEquals(1, legacy.stateVersion());
        assertEquals(2, serializer.serialize(legacy)[0]);
    }

    private static byte[] legacyFixture(String cursorType, String cursorValue,
            String uniqueIdField, String uniqueIdValue) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(1);
            writeLegacyString(output, cursorType);
            writeLegacyString(output, cursorValue);
            writeLegacyString(output, uniqueIdField);
            writeLegacyString(output, uniqueIdValue);
        }
        return bytes.toByteArray();
    }

    private static void writeLegacyString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value == null ? null : value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        output.writeInt(bytes == null ? -1 : bytes.length);
        if (bytes != null) {
            output.write(bytes);
        }
    }
}
