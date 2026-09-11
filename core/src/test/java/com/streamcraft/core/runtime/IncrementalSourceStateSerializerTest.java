package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class IncrementalSourceStateSerializerTest {

    private final IncrementalSourceStateSerializer serializer = new IncrementalSourceStateSerializer();

    @Test
    void roundTripsACompositeCursorAndOptionalFields() throws Exception {
        IncrementalSourceState state = new IncrementalSourceState(
                "TIMESTAMP", "2026-09-09T10:15:30Z", "event_id", "evt-42");

        IncrementalSourceState restored = serializer.decode(serializer.getVersion(), serializer.encode(state));

        assertEquals(state, restored);
        assertEquals(1, serializer.getVersion());
        assertEquals(true, restored.hasUniqueId());
    }

    @Test
    void roundTripsNullCursorAndNoTieBreaker() throws Exception {
        IncrementalSourceState state = new IncrementalSourceState("STRING", null, null, null);

        assertEquals(state, serializer.deserialize(1, serializer.serialize(state)));
    }

    @Test
    void rejectsUnknownVersionNullBytesTrailingBytesAndMalformedFields() throws Exception {
        IncrementalSourceState state = new IncrementalSourceState("LONG", "10", null, null);
        byte[] encoded = serializer.serialize(state);

        assertThrows(IOException.class, () -> serializer.deserialize(2, encoded));
        assertThrows(IOException.class, () -> serializer.deserialize(1, null));
        assertThrows(IOException.class, () -> serializer.deserialize(1, Arrays.copyOf(encoded, encoded.length + 1)));

        byte[] invalidMarker = encoded.clone();
        invalidMarker[0] = 2;
        assertThrows(IOException.class, () -> serializer.deserialize(1, invalidMarker));
    }

    @Test
    void acceptsAnInitialCompositeCursorAndRejectsInvalidUtf8() throws Exception {
        IncrementalSourceState state = new IncrementalSourceState("LONG", "10", "id", null);
        assertEquals(state, serializer.deserialize(1, serializer.serialize(state)));
        byte[] encoded = serializer.serialize(state);
        encoded[5] = (byte) 0xC3;
        encoded[6] = (byte) 0x28;
        assertThrows(IOException.class, () -> serializer.deserialize(1, encoded));
    }

    @Test
    void rejectsBlankCursorTypeAndUnboundUniqueId() {
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceState(" ", "10", null, null));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceState("LONG", "10", "", "id"));
    }
}
