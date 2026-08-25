package com.streamcraft.service.flink;

public final class FlinkParallelismResolver {

    private static final int DEFAULT_PARALLELISM = 1;

    private FlinkParallelismResolver() {
    }

    public static int resolve(Integer requestedParallelism) {
        return requestedParallelism == null || requestedParallelism < DEFAULT_PARALLELISM
                ? DEFAULT_PARALLELISM
                : requestedParallelism;
    }
}
