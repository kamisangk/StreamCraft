package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.Objects;

/**
 * Versioned progress shared by cursor based sources.
 *
 * <p>The values are stored in their connector configuration representation so
 * that checkpoint state does not depend on a JDBC driver or connector client
 * class being available during restore.</p>
 */
record IncrementalSourceState(
        String cursorType,
        String cursorValue,
        String uniqueIdField,
        String uniqueIdValue) implements Serializable {

    private static final long serialVersionUID = 1L;

    IncrementalSourceState {
        cursorType = Objects.requireNonNull(cursorType, "cursorType").trim();
        if (cursorType.isEmpty()) {
            throw new IllegalArgumentException("Incremental source cursorType must not be blank.");
        }
        if (uniqueIdField != null) {
            uniqueIdField = uniqueIdField.trim();
            if (uniqueIdField.isEmpty()) {
                if (uniqueIdValue != null) {
                    throw new IllegalArgumentException(
                            "Incremental source uniqueIdValue requires a uniqueIdField.");
                }
                uniqueIdField = null;
            }
        }
        if (uniqueIdValue != null && uniqueIdField == null) {
            throw new IllegalArgumentException("Incremental source uniqueIdValue requires a uniqueIdField.");
        }
    }

    boolean hasUniqueId() {
        return uniqueIdField != null;
    }
}
