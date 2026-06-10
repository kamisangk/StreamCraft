package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.junit.jupiter.api.Test;

class PipelineRuntimeAggregateExecutionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Map<String, List<DataEntity>> CAPTURED_RECORDS = new ConcurrentHashMap<>();
    private static final List<DataEntity> SOURCE_RECORDS = Collections.synchronizedList(new ArrayList<>());
    private static volatile long SOURCE_DELAY_AFTER_EACH_RECORD_MILLIS;
    private static volatile String SOURCE_WAIT_FOR_SINK_ID;

    @Test
    void groupedCountWindowEmitsCountSumAndAverageMetrics() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-grouped-count",
                new PipelineDefinition(
                        "pipeline-grouped-count",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-1", """
                                        {
                                          "mode": "GROUPED",
                                          "groupBy": ["customer.region"],
                                          "windowType": "COUNT",
                                          "countWindowSize": 2,
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"},
                                            {"function": "SUM", "field": "amount", "outputField": "amountSum"},
                                            {"function": "AVG", "field": "amount", "outputField": "amountAvg"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-grouped-count")),
                        List.of(
                                edge("source-1", "aggregate-1"),
                                edge("aggregate-1", "sink-grouped-count"))),
                List.of(
                        entity(1000L, Map.of("customer", Map.of("region", "east"), "amount", "10.5")),
                        entity(1001L, Map.of("customer", Map.of("region", "west"), "amount", 1)),
                        entity(1002L, Map.of("customer", Map.of("region", "east"), "amount", 4.5)),
                        entity(1003L, Map.of("customer", Map.of("region", "west"), "amount", "3"))),
                "aggregate-grouped-count-test");

        assertEquals(2, sinkRecords.size());

        Map<String, Object> eastRecord = recordForGroup(sinkRecords, "customer.region", "east");
        assertEquals(Set.of("window", "group", "metrics"), eastRecord.keySet());
        assertEquals(Map.of("customer.region", "east"), eastRecord.get("group"));
        assertEquals(Map.of("count", 2L, "amountSum", 15.0d, "amountAvg", 7.5d), eastRecord.get("metrics"));
        assertEquals(Map.of("type", "COUNT", "size", 2L), eastRecord.get("window"));

        List<String> ids = sinkRecords.stream().map(DataEntity::id).toList();
        assertEquals(ids.size(), ids.stream().distinct().count());
        assertTrue(ids.stream().allMatch(id -> id.startsWith("aggregate:aggregate-1:count:")));

        for (DataEntity record : sinkRecords) {
            assertEquals("AGGREGATE", record.headers().get("operator"));
            assertEquals("aggregate-1", record.headers().get("nodeId"));
            assertEquals("COUNT", record.headers().get("windowType"));
        }
    }

    @Test
    void globalCountWindowEmitsCountSumMinAndMaxWhileSkippingNonNumericInput() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-global-count",
                new PipelineDefinition(
                        "pipeline-global-count",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-1", """
                                        {
                                          "mode": "GLOBAL",
                                          "windowType": "COUNT",
                                          "countWindowSize": 4,
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"},
                                            {"function": "SUM", "field": "amount", "outputField": "amountSum"},
                                            {"function": "MIN", "field": "amount", "outputField": "amountMin"},
                                            {"function": "MAX", "field": "amount", "outputField": "amountMax"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-global-count")),
                        List.of(
                                edge("source-1", "aggregate-1"),
                                edge("aggregate-1", "sink-global-count"))),
                List.of(
                        entity(1000L, Map.of("amount", "7")),
                        entity(1001L, Map.of("amount", "not-a-number")),
                        entity(1002L, Map.of("amount", 3)),
                        entity(1003L, Map.of("amount", 11.5d))),
                "aggregate-global-count-test");

        assertEquals(1, sinkRecords.size());
        Map<String, Object> output = sinkRecords.get(0).fields();
        assertEquals(Map.of(), output.get("group"));
        assertEquals(Map.of("count", 4L, "amountSum", 21.5d, "amountMin", 3.0d, "amountMax", 11.5d), output.get("metrics"));
        assertEquals(Map.of("type", "COUNT", "size", 4L), output.get("window"));
    }

    @Test
    void countWindowEmitsEnhancedAggregationMetrics() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-enhanced-aggregate",
                new PipelineDefinition(
                        "pipeline-enhanced-aggregate",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-enhanced", """
                                        {
                                          "mode": "GLOBAL",
                                          "windowType": "COUNT",
                                          "countWindowSize": 4,
                                          "aggregations": [
                                            {"function": "COUNT_DISTINCT", "field": "userId", "outputField": "distinctUsers"},
                                            {"function": "FIRST_VALUE", "field": "status", "outputField": "firstStatus"},
                                            {"function": "LAST_VALUE", "field": "status", "outputField": "lastStatus"},
                                            {"function": "TOP_N", "field": "amount", "outputField": "topAmounts", "limit": 2},
                                            {"function": "COLLECT_LIST", "field": "sku", "outputField": "skuList"},
                                            {"function": "COLLECT_SET", "field": "sku", "outputField": "skuSet"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-enhanced-aggregate")),
                        List.of(
                                edge("source-1", "aggregate-enhanced"),
                                edge("aggregate-enhanced", "sink-enhanced-aggregate"))),
                List.of(
                        entity(1000L, Map.of("userId", "u-1", "status", "created", "amount", 7, "sku", "A")),
                        entity(1001L, Map.of("userId", "u-2", "status", "paid", "amount", 12, "sku", "B")),
                        entity(1002L, Map.of("userId", "u-1", "status", "shipped", "amount", 3, "sku", "A")),
                        entity(1003L, Map.of("userId", "u-3", "status", "done", "amount", 9, "sku", "C"))),
                "aggregate-enhanced-test");

        assertEquals(1, sinkRecords.size());
        Map<String, Object> metrics = metrics(sinkRecords.get(0).fields());
        assertEquals(3L, metrics.get("distinctUsers"));
        assertEquals("created", metrics.get("firstStatus"));
        assertEquals("done", metrics.get("lastStatus"));
        assertEquals(List.of(12, 9), metrics.get("topAmounts"));
        assertEquals(List.of("A", "B", "A", "C"), metrics.get("skuList"));
        assertEquals(List.of("A", "B", "C"), metrics.get("skuSet"));
    }

    @Test
    void topNCanSortBySeparateFieldAndReturnSourceValues() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-top-n-sort",
                new PipelineDefinition(
                        "pipeline-top-n-sort",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-top-n-sort", """
                                        {
                                          "mode": "GLOBAL",
                                          "windowType": "COUNT",
                                          "countWindowSize": 3,
                                          "aggregations": [
                                            {
                                              "function": "TOP_N",
                                              "field": "sku",
                                              "sortField": "score",
                                              "sortDirection": "DESC",
                                              "outputField": "topSkus",
                                              "limit": 2
                                            }
                                          ]
                                        }
                                        """),
                                sinkNode("sink-top-n-sort")),
                        List.of(
                                edge("source-1", "aggregate-top-n-sort"),
                                edge("aggregate-top-n-sort", "sink-top-n-sort"))),
                List.of(
                        entity(1000L, Map.of("sku", "A", "score", 10)),
                        entity(1001L, Map.of("sku", "B", "score", 30)),
                        entity(1002L, Map.of("sku", "C", "score", 20))),
                "aggregate-top-n-sort-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(List.of("B", "C"), metrics(sinkRecords.get(0).fields()).get("topSkus"));
    }

    @Test
    void eventTimeTumblingWindowEmitsWindowBoundaryFields() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-event-time",
                new PipelineDefinition(
                        "pipeline-event-time",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-1", """
                                        {
                                          "mode": "GLOBAL",
                                          "windowType": "TUMBLING_TIME",
                                          "timeMode": "EVENT_TIME",
                                          "timeUnit": "MILLISECONDS",
                                          "windowSize": 1000,
                                          "watermarkDelay": 0,
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-event-time")),
                        List.of(
                                edge("source-1", "aggregate-1"),
                                edge("aggregate-1", "sink-event-time"))),
                List.of(
                        entity(0L, Map.of("amount", 1)),
                        entity(999L, Map.of("amount", 2)),
                        entity(1000L, Map.of("amount", 3))),
                "aggregate-event-time-test");

        assertEquals(2, sinkRecords.size());
        List<Map<String, Object>> outputs = sinkRecords.stream()
                .map(DataEntity::fields)
                .sorted(Comparator.comparingLong(fields -> ((Number) window(fields).get("start")).longValue()))
                .toList();

        assertEquals(Map.of("type", "TUMBLING_TIME", "timeMode", "EVENT_TIME", "start", 0L, "end", 1000L), outputs.get(0).get("window"));
        assertEquals(Map.of("count", 2L), outputs.get(0).get("metrics"));
        assertEquals(Map.of("type", "TUMBLING_TIME", "timeMode", "EVENT_TIME", "start", 1000L, "end", 2000L), outputs.get(1).get("window"));
        assertEquals(Map.of("count", 1L), outputs.get(1).get("metrics"));

        List<String> ids = sinkRecords.stream().map(DataEntity::id).sorted().toList();
        assertTrue(ids.contains("aggregate:aggregate-1:0:1000:global"));
        assertTrue(ids.contains("aggregate:aggregate-1:1000:2000:global"));
    }

    @Test
    void eventTimeTumblingWindowCanUseConfiguredPayloadTimestampField() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-event-time-field",
                new PipelineDefinition(
                        "pipeline-event-time-field",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-event-field", """
                                        {
                                          "mode": "GLOBAL",
                                          "windowType": "TUMBLING_TIME",
                                          "timeMode": "EVENT_TIME",
                                          "timeUnit": "MILLISECONDS",
                                          "windowSize": 1000,
                                          "watermarkDelay": 0,
                                          "eventTimeField": "payload.eventTime",
                                          "eventTimeUnit": "MILLISECONDS",
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-event-time-field")),
                        List.of(
                                edge("source-1", "aggregate-event-field"),
                                edge("aggregate-event-field", "sink-event-time-field"))),
                List.of(
                        entity(10_000L, Map.of("payload", Map.of("eventTime", 0L))),
                        entity(10_000L, Map.of("payload", Map.of("eventTime", 999L))),
                        entity(10_000L, Map.of("payload", Map.of("eventTime", 1000L)))),
                "aggregate-event-time-field-test");

        assertEquals(2, sinkRecords.size());
        List<Map<String, Object>> windows = sinkRecords.stream()
                .map(DataEntity::fields)
                .map(PipelineRuntimeAggregateExecutionTest::window)
                .sorted(Comparator.comparingLong(window -> ((Number) window.get("start")).longValue()))
                .toList();

        assertEquals(Map.of("type", "TUMBLING_TIME", "timeMode", "EVENT_TIME", "start", 0L, "end", 1000L), windows.get(0));
        assertEquals(Map.of("type", "TUMBLING_TIME", "timeMode", "EVENT_TIME", "start", 1000L, "end", 2000L), windows.get(1));
    }

    @Test
    void flatOutputModeEmitsWindowAndMetricsAtTopLevel() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-flat-aggregate",
                new PipelineDefinition(
                        "pipeline-flat-aggregate",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-flat", """
                                        {
                                          "mode": "GLOBAL",
                                          "windowType": "TUMBLING_TIME",
                                          "timeMode": "EVENT_TIME",
                                          "timeUnit": "MILLISECONDS",
                                          "windowSize": 1000,
                                          "watermarkDelay": 0,
                                          "outputMode": "FLAT",
                                          "windowStartField": "window_start",
                                          "windowEndField": "window_end",
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "record_count"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-flat-aggregate")),
                        List.of(
                                edge("source-1", "aggregate-flat"),
                                edge("aggregate-flat", "sink-flat-aggregate"))),
                List.of(
                        entity(0L, Map.of("amount", 1)),
                        entity(999L, Map.of("amount", 2))),
                "aggregate-flat-output-test");

        assertEquals(1, sinkRecords.size());
        Map<String, Object> fields = sinkRecords.get(0).fields();
        assertEquals("TUMBLING_TIME", fields.get("windowType"));
        assertEquals(0L, fields.get("window_start"));
        assertEquals(1000L, fields.get("window_end"));
        assertEquals(2L, fields.get("record_count"));
        assertTrue(!fields.containsKey("window"));
        assertTrue(!fields.containsKey("metrics"));
    }

    @Test
    void groupedTimeWindowIdsDoNotCollideForHashCollidingGroups() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-group-id-collision",
                new PipelineDefinition(
                        "pipeline-group-id-collision",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-collision", """
                                        {
                                          "mode": "GROUPED",
                                          "groupBy": ["customer.region"],
                                          "windowType": "TUMBLING_TIME",
                                          "timeMode": "EVENT_TIME",
                                          "timeUnit": "MILLISECONDS",
                                          "windowSize": 1000,
                                          "watermarkDelay": 0,
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-group-id-collision")),
                        List.of(
                                edge("source-1", "aggregate-collision"),
                                edge("aggregate-collision", "sink-group-id-collision"))),
                List.of(
                        entity(0L, Map.of("customer", Map.of("region", "Aa"))),
                        entity(1L, Map.of("customer", Map.of("region", "BB")))),
                "aggregate-group-id-collision-test");

        assertEquals(2, sinkRecords.size());
        List<String> ids = sinkRecords.stream().map(DataEntity::id).toList();
        assertEquals(2, ids.stream().distinct().count());
        assertTrue(ids.stream().allMatch(id -> id.startsWith("aggregate:aggregate-collision:0:1000:")));
        assertEquals(Set.of("Aa", "BB"), sinkRecords.stream()
                .map(DataEntity::fields)
                .map(PipelineRuntimeAggregateExecutionTest::group)
                .map(group -> group.get("customer.region"))
                .collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    void groupedTimeWindowIdsDistinguishNumericAndStringGroupValues() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-group-type-id-collision",
                new PipelineDefinition(
                        "pipeline-group-type-id-collision",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-type-collision", """
                                        {
                                          "mode": "GROUPED",
                                          "groupBy": ["customer.region"],
                                          "windowType": "TUMBLING_TIME",
                                          "timeMode": "EVENT_TIME",
                                          "timeUnit": "MILLISECONDS",
                                          "windowSize": 1000,
                                          "watermarkDelay": 0,
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-group-type-id-collision")),
                        List.of(
                                edge("source-1", "aggregate-type-collision"),
                                edge("aggregate-type-collision", "sink-group-type-id-collision"))),
                List.of(
                        entity(0L, Map.of("customer", Map.of("region", 1))),
                        entity(1L, Map.of("customer", Map.of("region", "1")))),
                "aggregate-group-type-id-collision-test");

        assertEquals(2, sinkRecords.size());
        List<String> ids = sinkRecords.stream().map(DataEntity::id).toList();
        assertEquals(2, ids.stream().distinct().count());
        assertTrue(ids.stream().allMatch(id -> id.startsWith("aggregate:aggregate-type-collision:0:1000:")));
        assertEquals(Set.of(1, "1"), sinkRecords.stream()
                .map(DataEntity::fields)
                .map(PipelineRuntimeAggregateExecutionTest::group)
                .map(group -> group.get("customer.region"))
                .collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    void groupedCountWindowEmitsNullForMissingOrNullGroupFields() throws Exception {
        Map<String, Object> explicitNullRegion = new HashMap<>();
        explicitNullRegion.put("customer", Map.of("region", "north"));
        explicitNullRegion.put("missing", null);
        Map<String, Object> missingRegion = Map.of("customer", Map.of("region", "north"));

        List<DataEntity> sinkRecords = execute(
                "sink-null-group",
                new PipelineDefinition(
                        "pipeline-null-group",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-null", """
                                        {
                                          "mode": "GROUPED",
                                          "groupBy": ["customer.region", "missing"],
                                          "windowType": "COUNT",
                                          "countWindowSize": 2,
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-null-group")),
                        List.of(
                                edge("source-1", "aggregate-null"),
                                edge("aggregate-null", "sink-null-group"))),
                List.of(entity(1000L, explicitNullRegion), entity(1001L, missingRegion)),
                "aggregate-null-group-test");

        assertEquals(1, sinkRecords.size());
        Map<String, Object> group = group(sinkRecords.get(0).fields());
        assertEquals("north", group.get("customer.region"));
        assertTrue(group.containsKey("missing"));
        assertEquals(null, group.get("missing"));
        assertEquals(Map.of("count", 2L), sinkRecords.get(0).fields().get("metrics"));
    }

    @Test
    void processingTimeTumblingWindowExecutesTimeWindowPath() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-processing-time",
                new PipelineDefinition(
                        "pipeline-processing-time",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-processing", """
                                        {
                                          "mode": "GLOBAL",
                                          "windowType": "TUMBLING_TIME",
                                          "timeMode": "PROCESSING_TIME",
                                          "timeUnit": "MILLISECONDS",
                                          "windowSize": 1,
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-processing-time")),
                        List.of(
                                edge("source-1", "aggregate-processing"),
                                edge("aggregate-processing", "sink-processing-time"))),
                List.of(entity(1000L, Map.of("amount", 1))),
                "aggregate-processing-time-test",
                25L,
                "sink-processing-time");

        assertTrue(sinkRecords.size() >= 1);
        Map<String, Object> output = sinkRecords.get(0).fields();
        assertEquals(Map.of("count", 1L), output.get("metrics"));
        assertEquals("PROCESSING_TIME", window(output).get("timeMode"));
        assertEquals("TUMBLING_TIME", window(output).get("type"));
        assertInstanceOf(Number.class, window(output).get("start"));
        assertInstanceOf(Number.class, window(output).get("end"));
        assertTrue(sinkRecords.get(0).id().matches("aggregate:aggregate-processing:\\d+:\\d+:global"));
    }

    @Test
    void slidingEventTimeWindowExecutesSlidingWindowPath() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-sliding-time",
                new PipelineDefinition(
                        "pipeline-sliding-time",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-sliding", """
                                        {
                                          "mode": "GLOBAL",
                                          "windowType": "SLIDING_TIME",
                                          "timeMode": "EVENT_TIME",
                                          "timeUnit": "MILLISECONDS",
                                          "windowSize": 1000,
                                          "windowSlide": 500,
                                          "watermarkDelay": 0,
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-sliding-time")),
                        List.of(
                                edge("source-1", "aggregate-sliding"),
                                edge("aggregate-sliding", "sink-sliding-time"))),
                List.of(
                        entity(0L, Map.of("amount", 1)),
                        entity(500L, Map.of("amount", 2)),
                        entity(1000L, Map.of("amount", 3))),
                "aggregate-sliding-time-test");

        assertTrue(sinkRecords.size() >= 3);
        assertTrue(sinkRecords.stream()
                .map(DataEntity::fields)
                .map(PipelineRuntimeAggregateExecutionTest::window)
                .allMatch(window -> "SLIDING_TIME".equals(window.get("type"))));
        assertTrue(sinkRecords.stream().anyMatch(record -> Map.of("count", 2L).equals(record.fields().get("metrics"))));
    }

    @Test
    void lateEventTimeRecordIsDiscardedAfterWatermarkClosesWindow() throws Exception {
        List<DataEntity> sinkRecords = execute(
                "sink-late-event-time",
                new PipelineDefinition(
                        "pipeline-late-event-time",
                        List.of(
                                sourceNode(),
                                aggregateNode("aggregate-late", """
                                        {
                                          "mode": "GLOBAL",
                                          "windowType": "TUMBLING_TIME",
                                          "timeMode": "EVENT_TIME",
                                          "timeUnit": "MILLISECONDS",
                                          "windowSize": 1000,
                                          "watermarkDelay": 0,
                                          "aggregations": [
                                            {"function": "COUNT", "outputField": "count"}
                                          ]
                                        }
                                        """),
                                sinkNode("sink-late-event-time")),
                        List.of(
                                edge("source-1", "aggregate-late"),
                                edge("aggregate-late", "sink-late-event-time"))),
                List.of(
                        entity(0L, Map.of("amount", 1)),
                        entity(2000L, Map.of("amount", 2)),
                        entity(500L, Map.of("amount", 3))),
                "aggregate-late-event-time-test",
                25L);

        Map<String, Object> firstWindow = sinkRecords.stream()
                .map(DataEntity::fields)
                .filter(fields -> Long.valueOf(0L).equals(((Number) window(fields).get("start")).longValue()))
                .findFirst()
                .orElseThrow();
        assertEquals(Map.of("count", 1L), firstWindow.get("metrics"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> window(Map<String, Object> fields) {
        return (Map<String, Object>) fields.get("window");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> group(Map<String, Object> fields) {
        return (Map<String, Object>) fields.get("group");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> metrics(Map<String, Object> fields) {
        return (Map<String, Object>) fields.get("metrics");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> recordForGroup(List<DataEntity> records, String key, Object value) {
        return records.stream()
                .map(DataEntity::fields)
                .filter(fields -> value.equals(((Map<String, Object>) fields.get("group")).get(key)))
                .findFirst()
                .orElseThrow();
    }

    private List<DataEntity> execute(
            String sinkId,
            PipelineDefinition definition,
            List<DataEntity> sourceRecords,
            String jobName) throws Exception {
        return execute(sinkId, definition, sourceRecords, jobName, 0L);
    }

    private List<DataEntity> execute(
            String sinkId,
            PipelineDefinition definition,
            List<DataEntity> sourceRecords,
            String jobName,
            long sourceDelayAfterEachRecordMillis) throws Exception {
        return execute(sinkId, definition, sourceRecords, jobName, sourceDelayAfterEachRecordMillis, null);
    }

    private List<DataEntity> execute(
            String sinkId,
            PipelineDefinition definition,
            List<DataEntity> sourceRecords,
            String jobName,
            long sourceDelayAfterEachRecordMillis,
            String sourceWaitForSinkId) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1L);
        CAPTURED_RECORDS.clear();
        SOURCE_RECORDS.clear();
        SOURCE_RECORDS.addAll(sourceRecords);
        SOURCE_DELAY_AFTER_EACH_RECORD_MILLIS = sourceDelayAfterEachRecordMillis;
        SOURCE_WAIT_FOR_SINK_ID = sourceWaitForSinkId;

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new DataEntitySourceFactory(),
                new CapturingKafkaSinkFactory(),
                new TransformOperatorFactory(),
                true);

        try {
            runtime.run(definition);
            env.execute(jobName);
            return CAPTURED_RECORDS.getOrDefault(sinkId, List.of());
        } finally {
            SOURCE_DELAY_AFTER_EACH_RECORD_MILLIS = 0L;
            SOURCE_WAIT_FOR_SINK_ID = null;
        }
    }

    private PipelineNode sourceNode() {
        return new PipelineNode(
                "source-1",
                "Source",
                PipelineNodeType.SOURCE,
                PipelineOperator.KAFKA_SOURCE,
                jsonNode("""
                        {
                          "bootstrapServers": "127.0.0.1:9092",
                          "topics": ["input-topic"],
                          "groupId": "group-1",
                          "consumeMode": "earliest",
                          "authType": "NONE",
                          "format": "JSON"
                        }
                        """));
    }

    private PipelineNode aggregateNode(String id, String aggregateConfig) {
        return new PipelineNode(
                id,
                "Aggregate",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.AGGREGATE,
                jsonNode(aggregateConfig));
    }

    private PipelineNode sinkNode(String id) {
        return new PipelineNode(
                id,
                "Sink " + id,
                PipelineNodeType.SINK,
                PipelineOperator.KAFKA_SINK,
                jsonNode("""
                        {
                          "bootstrapServers": "127.0.0.1:9092",
                          "topic": "output-topic",
                          "deliveryGuarantee": "AT_LEAST_ONCE",
                          "authType": "NONE",
                          "format": "JSON"
                        }
                        """));
    }

    private PipelineEdge edge(String sourceNodeId, String targetNodeId) {
        return new PipelineEdge(sourceNodeId + "-" + targetNodeId, sourceNodeId, "output-0", targetNodeId, "input-0");
    }

    private DataEntity entity(long timestamp, Map<String, Object> fields) {
        return new DataEntity("entity-" + timestamp, timestamp, new HashMap<>(fields), Map.of("source", "test"));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static final class DataEntitySourceFactory extends MockSourceFactory {

        @Override
        public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
            return env.addSource(new DataEntitySourceFunction()).name(sourceNode.name());
        }
    }

    private static final class DataEntitySourceFunction implements SourceFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<DataEntity> ctx) {
            for (DataEntity record : SOURCE_RECORDS) {
                if (!running) {
                    break;
                }
                ctx.collect(record);
                if (SOURCE_DELAY_AFTER_EACH_RECORD_MILLIS > 0) {
                    try {
                        Thread.sleep(SOURCE_DELAY_AFTER_EACH_RECORD_MILLIS);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            waitForCapturedSinkRecord();
        }

        private void waitForCapturedSinkRecord() {
            String sinkId = SOURCE_WAIT_FOR_SINK_ID;
            if (sinkId == null || sinkId.isBlank()) {
                return;
            }
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (running && System.nanoTime() < deadline) {
                List<DataEntity> records = CAPTURED_RECORDS.get(sinkId);
                if (records != null && !records.isEmpty()) {
                    return;
                }
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class CapturingKafkaSinkFactory extends KafkaSinkFactory {

        @Override
        public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
            String sinkId = sinkNode.id();
            CAPTURED_RECORDS.computeIfAbsent(sinkId, ignored -> Collections.synchronizedList(new ArrayList<>()));
            stream.addSink(new CapturingSinkFunction(sinkId)).name("capture-" + sinkId);
        }
    }

    private static final class CapturingSinkFunction extends RichSinkFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String sinkId;

        private CapturingSinkFunction(String sinkId) {
            this.sinkId = sinkId;
        }

        @Override
        public void invoke(DataEntity value, Context context) {
            assertEquals("AGGREGATE", value.headers().get("operator"));
            assertNotNull(value.headers().get("nodeId"));
            assertNotNull(value.headers().get("windowType"));
            CAPTURED_RECORDS.get(sinkId).add(value);
        }
    }
}
