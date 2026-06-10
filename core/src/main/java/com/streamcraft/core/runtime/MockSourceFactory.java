package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MockSourceFactory.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
        JsonNode config = sourceNode.config();
        List<Map<String, Object>> sampleData = resolveSampleData(config);

        return env.addSource(new MockSourceFunction(sampleData)).name(sourceNode.name());
    }

    private static List<Map<String, Object>> resolveSampleData(JsonNode config) {
        JsonNode sampleDataNode = config.path("sampleData");
        String format = config.path("format").asText("JSON");
        List<Map<String, Object>> sampleData = new ArrayList<>();
        boolean hasConfiguredSamples = sampleDataNode.isArray() && sampleDataNode.size() > 0;

        if (sampleDataNode.isArray()) {
            int index = 0;
            for (JsonNode item : sampleDataNode) {
                String sample = item.isTextual() ? item.asText() : "";
                try {
                    sampleData.add(resolveFields(format, sample));
                } catch (Exception exception) {
                    LOG.warn("Skipping mock source sample at index {} because {}", index, exception.getMessage());
                }
                index++;
            }
        }

        if (sampleData.isEmpty() && !hasConfiguredSamples) {
            String fallbackSample = "TEXT".equals(format) ? "Sample data" : "{\"message\":\"Sample data\"}";
            sampleData.add(resolveFields(format, fallbackSample));
        }

        return sampleData;
    }

    private static Map<String, Object> resolveFields(String format, String sample) {
        try {
            return new HashMap<>(KafkaSourceFactory.decodeFields(format, sample, OBJECT_MAPPER));
        } catch (Exception exception) {
            throw new IllegalArgumentException("Failed to parse preview sample.", exception);
        }
    }

    private static final class MockSourceFunction implements SourceFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private final List<Map<String, Object>> sampleData;
        private volatile boolean running = true;

        private MockSourceFunction(List<Map<String, Object>> sampleData) {
            this.sampleData = List.copyOf(sampleData);
        }

        @Override
        public void run(SourceContext<DataEntity> ctx) throws Exception {
            for (Map<String, Object> record : sampleData) {
                if (!running) {
                    break;
                }

                String id = UUID.randomUUID().toString();
                long timestamp = System.currentTimeMillis();
                Map<String, Object> fields = new HashMap<>(record);
                Map<String, String> headers = new HashMap<>();
                headers.put("source", "mock");

                ctx.collect(new DataEntity(id, timestamp, fields, headers));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
