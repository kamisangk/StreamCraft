package com.streamcraft.core.runtime;

import java.util.Locale;

public enum ExecutionMode {
    RUN(false, false),
    PREVIEW(true, true);

    private final boolean forceMockSources;
    private final boolean interceptSinks;

    ExecutionMode(boolean forceMockSources, boolean interceptSinks) {
        this.forceMockSources = forceMockSources;
        this.interceptSinks = interceptSinks;
    }

    public boolean forceMockSources() {
        return forceMockSources;
    }

    public boolean interceptSinks() {
        return interceptSinks;
    }

    public static ExecutionMode parse(String value) {
        if (value == null || value.isBlank()) {
            return RUN;
        }
        return ExecutionMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
