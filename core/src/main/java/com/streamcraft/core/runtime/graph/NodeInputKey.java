package com.streamcraft.core.runtime.graph;

import java.util.Objects;

public record NodeInputKey(String nodeId, String inputPortId) {

    public NodeInputKey {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(inputPortId, "inputPortId");
    }
}
