package com.streamcraft.core.runtime;

import java.io.Serializable;

record ElasticsearchSourceEnumeratorState(ElasticsearchSourceSplit pendingSplit) implements Serializable {

    private static final long serialVersionUID = 1L;
}
