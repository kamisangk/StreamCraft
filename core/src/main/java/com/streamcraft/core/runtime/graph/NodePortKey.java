package com.streamcraft.core.runtime.graph;

import java.util.Objects;

public record NodePortKey(String nodeId, String portId) {

    public NodePortKey {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(portId, "portId");
    }
}
