package com.streamcraft.core.runtime;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

final class IncrementalSourceStateSerializerSupport {

    private IncrementalSourceStateSerializerSupport() {
    }

    static String readString(DataInputStream input, String fieldName) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > input.available()) {
            throw new IOException("Invalid " + fieldName + " length.");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException("Truncated " + fieldName + ".");
        }
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }
}
