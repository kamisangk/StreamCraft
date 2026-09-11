package com.streamcraft.core.runtime;

import java.io.Serializable;

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
        if (cursorType == null || cursorType.trim().isEmpty()) {
            throw new IllegalArgumentException("Incremental source cursorType must not be blank.");
        }
        cursorType = cursorType.trim();
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
        if (uniqueIdValue != null && cursorValue == null) {
            throw new IllegalArgumentException("Incremental source uniqueIdValue requires a cursorValue.");
        }
    }

    int stateVersion() {
        return 1;
    }

    boolean hasUniqueId() {
        return uniqueIdField != null;
    }
}
