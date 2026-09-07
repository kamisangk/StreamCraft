package com.streamcraft.core.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSourceFactory {

    public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
        KafkaSourceConfig config = KafkaSourceConfig.from(sourceNode.config());
        return env.fromSource(new KafkaSource(config), WatermarkStrategy.noWatermarks(), sourceNode.name())
                .name(sourceNode.name());
    }

    static boolean resolveCommitOnCheckpoint(JsonNode config) {
        return KafkaSourceConfig.resolveCommitOnCheckpoint(config);
    }

    static String resolveAutoOffsetReset(String consumeMode) {
        return KafkaSourceConfig.autoOffsetResetFor(consumeMode);
    }

    static Map<String, Object> decodeFields(String format, String payload, ObjectMapper objectMapper) throws IOException {
        if ("TEXT".equals(format)) {
            return new HashMap<>(Map.of("_streamcraft_message", payload == null ? "" : payload));
        }

        JsonNode json = objectMapper.readTree(payload == null ? "null" : payload);
        if (json == null || !json.isObject()) {
            throw new IllegalArgumentException("Kafka Source JSON payload must be a JSON object.");
        }
        return objectMapper.convertValue(json, new TypeReference<Map<String, Object>>() {});
    }

}
