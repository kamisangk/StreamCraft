package com.streamcraft.core.model;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;

public enum PipelineNodeType {
    SOURCE,
    TRANSFORM,
    SINK,
    @JsonEnumDefaultValue
    UNKNOWN
}
