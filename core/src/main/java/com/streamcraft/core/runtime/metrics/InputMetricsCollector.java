package com.streamcraft.core.runtime.metrics;

import com.streamcraft.core.model.DataEntity;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;

public class InputMetricsCollector extends RichMapFunction<DataEntity, DataEntity> {

    private final String nodeId;
    private transient Counter inputCounter;

    public InputMetricsCollector(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        inputCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("streamcraft")
                .addGroup("node", nodeId)
                .counter("input_records");
    }

    @Override
    public DataEntity map(DataEntity entity) {
        inputCounter.inc();
        return entity;
    }
}
