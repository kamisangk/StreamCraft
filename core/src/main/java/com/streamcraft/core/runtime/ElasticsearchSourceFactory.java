package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfigParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ElasticsearchSourceFactory {

    public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
        ElasticsearchSourceConfig config = ElasticsearchSourceConfigParser.parse(
                sourceNode.config(),
                IllegalArgumentException::new);
        if (config.splitCount() > 1) {
            return env.fromSource(new ElasticsearchSource(config), WatermarkStrategy.noWatermarks(), sourceNode.name())
                    .setParallelism(config.splitCount());
        }
        return env.fromSource(new ElasticsearchSource(config), WatermarkStrategy.noWatermarks(), sourceNode.name())
                .setParallelism(1);
    }
}
