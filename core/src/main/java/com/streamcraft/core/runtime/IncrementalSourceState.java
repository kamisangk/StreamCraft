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
        String cursorField,
        String cursorValue,
        String uniqueIdField,
        String uniqueIdValue,
        int schemaVersion) implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Compatibility constructor for checkpoints/tests created before cursorField was persisted. */
    IncrementalSourceState(String cursorType, String cursorValue, String uniqueIdField, String uniqueIdValue) {
        this(cursorType, null, cursorValue, uniqueIdField, uniqueIdValue, 1);
    }

    IncrementalSourceState(
            String cursorType, String cursorField, String cursorValue, String uniqueIdField, String uniqueIdValue) {
        this(cursorType, cursorField, cursorValue, uniqueIdField, uniqueIdValue, 2);
    }

    IncrementalSourceState {
        if (cursorType == null || cursorType.trim().isEmpty()) {
            throw new IllegalArgumentException("Incremental source cursorType must not be blank.");
        }
        if (schemaVersion != 1 && schemaVersion != 2) {
            throw new IllegalArgumentException("Incremental source state schemaVersion must be 1 or 2.");
        }
        cursorType = cursorType.trim();
        if (cursorField != null) {
            cursorField = cursorField.trim();
            if (cursorField.isEmpty()) {
                cursorField = null;
            }
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
        if (uniqueIdValue != null && cursorValue == null) {
            throw new IllegalArgumentException("Incremental source uniqueIdValue requires a cursorValue.");
        }
    }

    int stateVersion() {
        return schemaVersion;
    }

    boolean hasUniqueId() {
        return uniqueIdField != null;
    }

    void validateFor(String sourceName, String expectedCursorType, String expectedUniqueIdField) {
        validateFor(sourceName, expectedCursorType, null, expectedUniqueIdField);
    }

    void validateFor(
            String sourceName, String expectedCursorType, String expectedCursorField, String expectedUniqueIdField) {
        String normalizedExpectedType = expectedCursorType == null ? "" : expectedCursorType.trim();
        if (!cursorType.equalsIgnoreCase(normalizedExpectedType)) {
            throw new IllegalArgumentException(sourceName + " checkpoint state version " + stateVersion()
                    + " cursorType changed from " + cursorType + " to " + normalizedExpectedType + ".");
        }
        if (schemaVersion >= 2) {
            String normalizedExpectedCursorField = normalizeOptional(expectedCursorField);
            if (!Objects.equals(cursorField, normalizedExpectedCursorField)) {
                throw new IllegalArgumentException(sourceName + " checkpoint state version " + stateVersion()
                        + " cursorField changed from " + display(cursorField) + " to "
                        + display(normalizedExpectedCursorField) + ".");
            }
        }
        String normalizedExpectedId = normalizeOptional(expectedUniqueIdField);
        if (!Objects.equals(uniqueIdField, normalizedExpectedId)) {
            throw new IllegalArgumentException(sourceName + " checkpoint state version " + stateVersion()
                    + " idField changed from " + display(uniqueIdField) + " to "
                    + display(normalizedExpectedId) + ".");
        }
    }

    private static String normalizeOptional(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private static String display(String value) {
        return value == null ? "<none>" : value;
    }
}
