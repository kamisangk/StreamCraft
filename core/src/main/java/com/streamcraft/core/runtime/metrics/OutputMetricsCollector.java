package com.streamcraft.core.runtime.metrics;

import com.streamcraft.core.model.DataEntity;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;

public class OutputMetricsCollector extends RichMapFunction<DataEntity, DataEntity> {

    private final String nodeId;
    private transient Counter outputCounter;

    public OutputMetricsCollector(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        outputCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("streamcraft")
                .addGroup("node", nodeId)
                .counter("output_records");
    }

    @Override
    public DataEntity map(DataEntity entity) {
        outputCounter.inc();
        return entity;
    }
}
