package com.streamcraft.core.runtime.metrics;

import com.streamcraft.core.model.DataEntity;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;

public class MetricsCollector extends RichMapFunction<DataEntity, DataEntity> {

    private final String nodeId;
    private transient Counter inputCounter;
    private transient Counter outputCounter;

    public MetricsCollector(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.inputCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("streamcraft")
                .addGroup("node", nodeId)
                .counter("input_records");
        this.outputCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("streamcraft")
                .addGroup("node", nodeId)
                .counter("output_records");
    }

    @Override
    public DataEntity map(DataEntity entity) throws Exception {
        inputCounter.inc();
        outputCounter.inc();
        return entity;
    }
}
