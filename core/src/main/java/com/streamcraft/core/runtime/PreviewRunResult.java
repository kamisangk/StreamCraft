package com.streamcraft.core.runtime;

import java.util.List;

public record PreviewRunResult(List<PreviewOutput> outputs) {

    public record PreviewOutput(String nodeId, List<String> records) {
    }
}
