package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.aggregation.AggregateConfig.OutputMode;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

final class AggregateOutputSupport {

    private AggregateOutputSupport() {
    }

    static DataEntity output(
            String nodeId,
            AggregateRuntimeConfig.RuntimeAggregateConfig config,
            Map<String, Object> group,
            Map<String, Object> metrics,
            Map<String, Object> window) {
        Map<String, Object> fields = new LinkedHashMap<>();
        if (config.outputMode() == OutputMode.FLAT) {
            fields.put("windowType", window.get("type"));
            if (window.containsKey("start")) {
                fields.put(config.windowStartField(), window.get("start"));
            }
            if (window.containsKey("end")) {
                fields.put(config.windowEndField(), window.get("end"));
            }
            if (window.containsKey("size")) {
                fields.put("windowSize", window.get("size"));
            }
            fields.putAll(group);
            fields.putAll(metrics);
        } else {
            fields.put("window", window);
            fields.put("group", group);
            fields.put("metrics", metrics);
        }

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("operator", "AGGREGATE");
        headers.put("nodeId", nodeId);
        headers.put("windowType", config.windowType().name());
        return new DataEntity(outputId(nodeId, config, group, window),
                System.currentTimeMillis(),
                fields,
                headers);
    }

    static Map<String, Object> countWindow(AggregateRuntimeConfig.RuntimeAggregateConfig config) {
        Map<String, Object> window = new LinkedHashMap<>();
        window.put("type", config.windowType().name());
        window.put("size", config.countWindowSize());
        return window;
    }

    static Map<String, Object> timeWindow(
            AggregateRuntimeConfig.RuntimeAggregateConfig config,
            TimeWindow timeWindow) {
        Map<String, Object> window = new LinkedHashMap<>();
        window.put("type", config.windowType().name());
        window.put("timeMode", config.timeMode().name());
        window.put("start", timeWindow.getStart());
        window.put("end", timeWindow.getEnd());
        return window;
    }

    private static String outputId(
            String nodeId,
            AggregateRuntimeConfig.RuntimeAggregateConfig config,
            Map<String, Object> group,
            Map<String, Object> window) {
        if (config.timeWindow()) {
            return "aggregate:%s:%s:%s:%s".formatted(
                    nodeId,
                    window.get("start"),
                    window.get("end"),
                    groupIdentity(group));
        }
        return "aggregate:%s:count:%s".formatted(nodeId, UUID.randomUUID());
    }

    private static String groupIdentity(Map<String, Object> group) {
        if (group.isEmpty()) {
            return "global";
        }
        StringBuilder builder = new StringBuilder();
        group.forEach((key, value) -> builder.append(key.length())
                .append(':')
                .append(key)
                .append('=')
                .append(value == null ? "null" : value.getClass().getName())
                .append('#')
                .append(value == null ? -1 : Objects.toString(value).length())
                .append(':')
                .append(Objects.toString(value, ""))
                .append(';'));
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(builder.toString().getBytes(StandardCharsets.UTF_8));
    }
}
