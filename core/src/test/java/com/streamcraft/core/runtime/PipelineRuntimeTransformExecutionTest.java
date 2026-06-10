package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.junit.jupiter.api.Test;

class PipelineRuntimeTransformExecutionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Map<String, List<Map<String, Object>>> CAPTURED_RECORDS = new ConcurrentHashMap<>();

    @Test
    void grokTransformExtractsFieldsFromTextMessage() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-grok",
                new PipelineDefinition(
                        "pipeline-grok",
                        List.of(
                                textSourceNode("INFO order-created"),
                                grokNode("_streamcraft_message", "", "(?<level>\\\\w+) (?<message>.*)"),
                                sinkNode("sink-grok")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "grok-1", "input-0"),
                                new PipelineEdge("edge-2", "grok-1", "output-0", "sink-grok", "input-0"))),
                "grok-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("INFO", sinkRecords.get(0).get("level"));
        assertEquals("order-created", sinkRecords.get(0).get("message"));
        assertEquals("INFO order-created", sinkRecords.get(0).get("_streamcraft_message"));
    }

    @Test
    void grokTransformSupportsStandardGrokPatternsAndNestedFieldPaths() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-grok-nested",
                new PipelineDefinition(
                        "pipeline-grok-nested",
                        List.of(
                                textSourceNode("INFO order-created"),
                                grokNode("_streamcraft_message", "", "%{LOGLEVEL:test.level} %{GREEDYDATA:test.message}"),
                                sinkNode("sink-grok-nested")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "grok-1", "input-0"),
                                new PipelineEdge("edge-2", "grok-1", "output-0", "sink-grok-nested", "input-0"))),
                "grok-transform-nested-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("level", "INFO", "message", "order-created"), sinkRecords.get(0).get("test"));
    }

    @Test
    void castTransformConvertsNestedFieldType() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-cast",
                new PipelineDefinition(
                        "pipeline-cast",
                        List.of(
                                jsonSourceNode("{\\\"test\\\":{\\\"age\\\":\\\"19\\\"}}"),
                                castNode("test.age", "test.age", "INT"),
                                sinkNode("sink-cast")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "cast-1", "input-0"),
                                new PipelineEdge("edge-2", "cast-1", "output-0", "sink-cast", "input-0"))),
                "cast-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("age", 19), sinkRecords.get(0).get("test"));
    }

    @Test
    void grokTransformWritesExtractedGroupsIntoOutputContainerField() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-grok-output",
                new PipelineDefinition(
                        "pipeline-grok-output",
                        List.of(
                                textSourceNode("INFO order-created"),
                                grokNode("_streamcraft_message", "parsed", "(?<level>\\\\w+) (?<message>.*)"),
                                sinkNode("sink-grok-output")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "grok-1", "input-0"),
                                new PipelineEdge("edge-2", "grok-1", "output-0", "sink-grok-output", "input-0"))),
                "grok-transform-output-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("level", "INFO", "message", "order-created"), sinkRecords.get(0).get("parsed"));
        assertEquals("INFO order-created", sinkRecords.get(0).get("_streamcraft_message"));
    }

    @Test
    void castTransformReadsInputFieldAndWritesConvertedValueToNestedOutputField() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-cast-output",
                new PipelineDefinition(
                        "pipeline-cast-output",
                        List.of(
                                jsonSourceNode("{\\\"test\\\":{\\\"age\\\":\\\"19\\\"}}"),
                                castNode("test.age", "profile.age", "INT"),
                                sinkNode("sink-cast-output")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "cast-1", "input-0"),
                                new PipelineEdge("edge-2", "cast-1", "output-0", "sink-cast-output", "input-0"))),
                "cast-transform-output-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("age", "19"), sinkRecords.get(0).get("test"));
        assertEquals(Map.of("age", 19), sinkRecords.get(0).get("profile"));
    }

    @Test
    void castTransformConvertsDecimalStringToInt() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-cast-int",
                new PipelineDefinition(
                        "pipeline-cast-int",
                        List.of(
                                jsonSourceNode("{\\\"parsed\\\":{\\\"duration\\\":\\\"0.043\\\"}}"),
                                castNode("parsed.duration", "test", "INT"),
                                sinkNode("sink-cast-int")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "cast-1", "input-0"),
                                new PipelineEdge("edge-2", "cast-1", "output-0", "sink-cast-int", "input-0"))),
                "cast-transform-decimal-int-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(0, sinkRecords.get(0).get("test"));
    }

    @Test
    void castTransformConvertsDecimalStringToLong() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-cast-long",
                new PipelineDefinition(
                        "pipeline-cast-long",
                        List.of(
                                jsonSourceNode("{\\\"parsed\\\":{\\\"duration\\\":\\\"0.043\\\"}}"),
                                castNode("parsed.duration", "test", "LONG"),
                                sinkNode("sink-cast-long")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "cast-1", "input-0"),
                                new PipelineEdge("edge-2", "cast-1", "output-0", "sink-cast-long", "input-0"))),
                "cast-transform-decimal-long-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(0L, sinkRecords.get(0).get("test"));
    }

    @Test
    void evalTransformWritesComputedResultToNestedField() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-eval",
                new PipelineDefinition(
                        "pipeline-eval",
                        List.of(
                                jsonSourceNode("{\\\"price\\\":12,\\\"quantity\\\":3}"),
                                evalNode("calc.total", "price * quantity"),
                                sinkNode("sink-eval")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "eval-1", "input-0"),
                                new PipelineEdge("edge-2", "eval-1", "output-0", "sink-eval", "input-0"))),
                "eval-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("total", 36), sinkRecords.get(0).get("calc"));
    }

    @Test
    void evalTransformWritesNullWhenExpressionFailsAndPutNullIsConfigured() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-eval-null",
                new PipelineDefinition(
                        "pipeline-eval-null",
                        List.of(
                                jsonSourceNode("{\\\"price\\\":12}"),
                                evalNode("calc.total", "price * missingQuantity", "OVERWRITE", "PUT_NULL"),
                                sinkNode("sink-eval-null")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "eval-1", "input-0"),
                                new PipelineEdge("edge-2", "eval-1", "output-0", "sink-eval-null", "input-0"))),
                "eval-null-error-strategy-test");

        assertEquals(1, sinkRecords.size());
        assertTrue(sinkRecords.get(0).get("calc") instanceof Map<?, ?>);
        Map<?, ?> calc = (Map<?, ?>) sinkRecords.get(0).get("calc");
        assertTrue(calc.containsKey("total"));
        assertEquals(null, calc.get("total"));
    }

    @Test
    void evalTransformDoesNotOverwriteExistingTargetWhenWriteIfAbsentIsConfigured() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-eval-write-if-absent",
                new PipelineDefinition(
                        "pipeline-eval-write-if-absent",
                        List.of(
                                jsonSourceNode("{\\\"price\\\":12,\\\"quantity\\\":3,\\\"calc\\\":{\\\"total\\\":99}}"),
                                evalNode("calc.total", "price * quantity", "WRITE_IF_ABSENT", "KEEP_ORIGINAL"),
                                sinkNode("sink-eval-write-if-absent")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "eval-1", "input-0"),
                                new PipelineEdge("edge-2", "eval-1", "output-0", "sink-eval-write-if-absent", "input-0"))),
                "eval-write-if-absent-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(99, ((Map<?, ?>) sinkRecords.get(0).get("calc")).get("total"));
    }

    @Test
    void deduplicateTransformKeepsFirstRecordForCompositeKeyWithinTtl() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-deduplicate",
                new PipelineDefinition(
                        "pipeline-deduplicate",
                        List.of(
                                jsonSourceNode(
                                        "{\\\"order\\\":{\\\"id\\\":\\\"A-1\\\"},\\\"region\\\":\\\"east\\\",\\\"amount\\\":10}",
                                        "{\\\"order\\\":{\\\"id\\\":\\\"A-1\\\"},\\\"region\\\":\\\"east\\\",\\\"amount\\\":99}",
                                        "{\\\"order\\\":{\\\"id\\\":\\\"B-2\\\"},\\\"region\\\":\\\"west\\\",\\\"amount\\\":20}"),
                                deduplicateNode(List.of("order.id", "region"), 3600, "FIRST"),
                                sinkNode("sink-deduplicate")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "deduplicate-1", "input-0"),
                                new PipelineEdge("edge-2", "deduplicate-1", "output-0", "sink-deduplicate", "input-0"))),
                "deduplicate-transform-test");

        assertEquals(2, sinkRecords.size());
        assertEquals(Map.of("id", "A-1"), sinkRecords.get(0).get("order"));
        assertEquals("east", sinkRecords.get(0).get("region"));
        assertEquals(10, sinkRecords.get(0).get("amount"));
        assertEquals(Map.of("id", "B-2"), sinkRecords.get(1).get("order"));
        assertEquals("west", sinkRecords.get(1).get("region"));
        assertEquals(20, sinkRecords.get(1).get("amount"));
    }

    @Test
    void deduplicateTransformKeepsLastRecordForCompositeKeyWithinTtl() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-deduplicate-last",
                new PipelineDefinition(
                        "pipeline-deduplicate-last",
                        List.of(
                                jsonSourceNode(
                                        "{\\\"order\\\":{\\\"id\\\":\\\"A-1\\\"},\\\"amount\\\":10}",
                                        "{\\\"order\\\":{\\\"id\\\":\\\"A-1\\\"},\\\"amount\\\":99}",
                                        "{\\\"order\\\":{\\\"id\\\":\\\"B-2\\\"},\\\"amount\\\":20}"),
                                deduplicateNode(List.of("order.id"), 1, "LAST"),
                                sinkNode("sink-deduplicate-last")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "deduplicate-1", "input-0"),
                                new PipelineEdge("edge-2", "deduplicate-1", "output-0", "sink-deduplicate-last", "input-0"))),
                "deduplicate-last-transform-test",
                new HoldingMockSourceFactory(1500));

        assertEquals(2, sinkRecords.size());
        Map<String, Map<String, Object>> byOrderId = new LinkedHashMap<>();
        for (Map<String, Object> record : sinkRecords) {
            Map<?, ?> order = (Map<?, ?>) record.get("order");
            byOrderId.put(String.valueOf(order.get("id")), record);
        }
        assertEquals(99, byOrderId.get("A-1").get("amount"));
        assertEquals(20, byOrderId.get("B-2").get("amount"));
    }

    @Test
    void deduplicateTransformKeepsLatestEventTimeRecordWithinWindow() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-deduplicate-event-time",
                new PipelineDefinition(
                        "pipeline-deduplicate-event-time",
                        List.of(
                                jsonSourceNode(
                                        "{\"order\":{\"id\":\"A-1\"},\"eventTime\":0,\"amount\":10}",
                                        "{\"order\":{\"id\":\"A-1\"},\"eventTime\":500,\"amount\":99}",
                                        "{\"order\":{\"id\":\"B-2\"},\"eventTime\":250,\"amount\":20}",
                                        "{\"order\":{\"id\":\"A-1\"},\"eventTime\":1000,\"amount\":7}"),
                                deduplicateEventTimeNode(List.of("order.id"), 1, 0, "EVENT_TIME_LATEST"),
                                sinkNode("sink-deduplicate-event-time")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "deduplicate-1", "input-0"),
                                new PipelineEdge("edge-2", "deduplicate-1", "output-0", "sink-deduplicate-event-time", "input-0"))),
                "deduplicate-event-time-transform-test");

        assertEquals(3, sinkRecords.size());
        Map<String, Map<String, Object>> byOrderAndTime = new LinkedHashMap<>();
        for (Map<String, Object> record : sinkRecords) {
            Map<?, ?> order = (Map<?, ?>) record.get("order");
            byOrderAndTime.put(order.get("id") + ":" + record.get("eventTime"), record);
        }
        assertEquals(99, byOrderAndTime.get("A-1:500").get("amount"));
        assertEquals(20, byOrderAndTime.get("B-2:250").get("amount"));
        assertEquals(7, byOrderAndTime.get("A-1:1000").get("amount"));
    }

    @Test
    void deduplicateTransformDiscardsLateEventTimeRecordAfterWindowCloses() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-deduplicate-late",
                new PipelineDefinition(
                        "pipeline-deduplicate-late",
                        List.of(
                                jsonSourceNode(
                                        "{\"order\":{\"id\":\"A-1\"},\"eventTime\":0,\"amount\":10}",
                                        "{\"order\":{\"id\":\"A-1\"},\"eventTime\":2000,\"amount\":99}",
                                        "{\"order\":{\"id\":\"A-1\"},\"eventTime\":500,\"amount\":7}"),
                                deduplicateEventTimeNode(List.of("order.id"), 1, 0, "EVENT_TIME_LATEST"),
                                sinkNode("sink-deduplicate-late")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "deduplicate-1", "input-0"),
                                new PipelineEdge("edge-2", "deduplicate-1", "output-0", "sink-deduplicate-late", "input-0"))),
                "deduplicate-late-event-time-transform-test",
                new DelayedMockSourceFactory(50));

        assertEquals(2, sinkRecords.size());
        Map<Long, Map<String, Object>> byEventTime = new LinkedHashMap<>();
        for (Map<String, Object> record : sinkRecords) {
            byEventTime.put(((Number) record.get("eventTime")).longValue(), record);
        }
        assertEquals(10, byEventTime.get(0L).get("amount"));
        assertEquals(99, byEventTime.get(2000L).get("amount"));
        assertTrue(byEventTime.get(500L) == null);
    }

    @Test
    void lookupEnrichTransformPopulatesMappedValuesFromStaticEntries() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-lookup-enrich",
                new PipelineDefinition(
                        "pipeline-lookup-enrich",
                        List.of(
                                jsonSourceNode("""
                                        {
                                          "countryCode": "CN",
                                          "status": "active"
                                        }
                                        """),
                                lookupEnrichNode("countryCode", "countryName", List.of(
                                        Map.of("key", "CN", "value", "China"),
                                        Map.of("key", "US", "value", "United States")), "KEEP_ORIGINAL", false),
                                sinkNode("sink-lookup-enrich")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "lookup-enrich-1", "input-0"),
                                new PipelineEdge("edge-2", "lookup-enrich-1", "output-0", "sink-lookup-enrich", "input-0"))),
                "lookup-enrich-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("CN", sinkRecords.get(0).get("countryCode"));
        assertEquals("China", sinkRecords.get(0).get("countryName"));
        assertEquals("active", sinkRecords.get(0).get("status"));
    }

    @Test
    void lookupEnrichTransformUsesConfiguredValueTypes() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-lookup-enrich-typed",
                new PipelineDefinition(
                        "pipeline-lookup-enrich-typed",
                        List.of(
                                jsonSourceNode(
                                        "{\"countryCode\":\"CN\",\"kind\":\"score\"}",
                                        "{\"countryCode\":\"US\",\"kind\":\"enabled\"}",
                                        "{\"countryCode\":\"JP\",\"kind\":\"profile\"}"),
                                lookupEnrichNode("countryCode", "countryValue", List.of(
                                        Map.of("key", "CN", "value", "86", "valueType", "NUMBER"),
                                        Map.of("key", "US", "value", "true", "valueType", "BOOLEAN"),
                                        Map.of("key", "JP", "value", "{\"name\":\"Japan\",\"region\":\"APAC\"}", "valueType", "JSON")), "KEEP_ORIGINAL", false),
                                sinkNode("sink-lookup-enrich-typed")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "lookup-enrich-1", "input-0"),
                                new PipelineEdge("edge-2", "lookup-enrich-1", "output-0", "sink-lookup-enrich-typed", "input-0"))),
                "lookup-enrich-typed-transform-test");

        assertEquals(3, sinkRecords.size());
        assertTrue(sinkRecords.get(0).get("countryValue") instanceof Number);
        assertEquals(86.0, ((Number) sinkRecords.get(0).get("countryValue")).doubleValue());
        assertEquals(true, sinkRecords.get(1).get("countryValue"));
        assertEquals(Map.of("name", "Japan", "region", "APAC"), sinkRecords.get(2).get("countryValue"));
    }

    @Test
    void lookupEnrichTransformDropsRecordsWhenConfiguredToDiscardMissingValues() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-lookup-enrich-discard",
                new PipelineDefinition(
                        "pipeline-lookup-enrich-discard",
                        List.of(
                                jsonSourceNode(
                                        "{\"countryCode\":\"CN\",\"status\":\"matched\"}",
                                        "{\"countryCode\":\"ZZ\",\"status\":\"unmatched\"}",
                                        "{\"status\":\"missing-key\"}"),
                                lookupEnrichNode("countryCode", "countryName", List.of(
                                        Map.of("key", "CN", "value", "China")), "DISCARD", false),
                                sinkNode("sink-lookup-enrich-discard")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "lookup-enrich-1", "input-0"),
                                new PipelineEdge("edge-2", "lookup-enrich-1", "output-0", "sink-lookup-enrich-discard", "input-0"))),
                "lookup-enrich-discard-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("matched", sinkRecords.get(0).get("status"));
        assertEquals("China", sinkRecords.get(0).get("countryName"));
    }

    @Test
    void lookupEnrichTransformFailsWhenConfiguredToFailOnMissingValues() {
        PipelineDefinition definition = new PipelineDefinition(
                "pipeline-lookup-enrich-fail",
                List.of(
                        jsonSourceNode(
                                "{\"countryCode\":\"ZZ\",\"status\":\"unmatched\"}"),
                        lookupEnrichNode("countryCode", "countryName", List.of(
                                Map.of("key", "CN", "value", "China")), "FAIL", false),
                        sinkNode("sink-lookup-enrich-fail")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "lookup-enrich-1", "input-0"),
                        new PipelineEdge("edge-2", "lookup-enrich-1", "output-0", "sink-lookup-enrich-fail", "input-0")));

        Exception exception = assertThrows(
                Exception.class,
                () -> execute("sink-lookup-enrich-fail", definition, "lookup-enrich-fail-transform-test"));

        assertTrue(exceptionMessageContains(exception, "LOOKUP_ENRICH"));
    }

    @Test
    void lookupJoinTransformAddsDimensionObjectForMatchedStaticEntry() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-lookup-join",
                new PipelineDefinition(
                        "pipeline-lookup-join",
                        List.of(
                                jsonSourceNode("""
                                        {
                                          "countryCode": "CN",
                                          "status": "active"
                                        }
                                        """),
                                lookupJoinNode("LEFT", "KEEP_ORIGINAL"),
                                sinkNode("sink-lookup-join")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "lookup-join-1", "input-0"),
                                new PipelineEdge("edge-2", "lookup-join-1", "output-0", "sink-lookup-join", "input-0"))),
                "lookup-join-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("CN", sinkRecords.get(0).get("countryCode"));
        assertEquals(Map.of("name", "China", "region", "APAC"), sinkRecords.get(0).get("country"));
        assertEquals("active", sinkRecords.get(0).get("status"));
    }

    @Test
    void lookupJoinTransformDropsUnmatchedRecordsForInnerJoin() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-lookup-join-inner",
                new PipelineDefinition(
                        "pipeline-lookup-join-inner",
                        List.of(
                                jsonSourceNode(
                                        "{\"countryCode\":\"CN\",\"status\":\"active\"}",
                                        "{\"countryCode\":\"ZZ\",\"status\":\"unknown\"}"),
                                lookupJoinNode("INNER", "KEEP_ORIGINAL"),
                                sinkNode("sink-lookup-join-inner")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "lookup-join-1", "input-0"),
                                new PipelineEdge("edge-2", "lookup-join-1", "output-0", "sink-lookup-join-inner", "input-0"))),
                "lookup-join-inner-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("CN", sinkRecords.get(0).get("countryCode"));
        assertEquals(Map.of("name", "China", "region", "APAC"), sinkRecords.get(0).get("country"));
    }

    @Test
    void streamJoinTransformConnectsLeftAndRightInputsByConfiguredPorts() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-stream-join",
                new PipelineDefinition(
                        "pipeline-stream-join",
                        List.of(
                                namedJsonSourceNode("left-source", "{\"orderId\":\"O-1\",\"amount\":99}"),
                                namedJsonSourceNode("right-source", "{\"orderId\":\"O-1\",\"sku\":\"SKU-1\"}"),
                                streamJoinNode(),
                                sinkNode("sink-stream-join")),
                        List.of(
                                new PipelineEdge("edge-1", "left-source", "output-0", "stream-join-1", "left"),
                                new PipelineEdge("edge-2", "right-source", "output-0", "stream-join-1", "right"),
                                new PipelineEdge("edge-3", "stream-join-1", "output-0", "sink-stream-join", "input-0"))),
                "stream-join-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("O-1", sinkRecords.get(0).get("orderId"));
        assertEquals(99, sinkRecords.get(0).get("amount"));
        assertEquals(Map.of("orderId", "O-1", "sku", "SKU-1"), sinkRecords.get(0).get("detail"));
    }

    @Test
    void flattenTransformFlattensNestedObjectFieldsIntoFlatKeys() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-flatten",
                new PipelineDefinition(
                        "pipeline-flatten",
                        List.of(
                                jsonSourceNode("""
                                        {
                                          "customer": {
                                            "id": "C-1",
                                            "name": "Ada",
                                            "address": {
                                              "city": "Shanghai",
                                              "zip": "200000"
                                            }
                                          },
                                          "order": {
                                            "id": "O-9"
                                          },
                                          "status": "new"
                                        }
                                        """),
                                flattenNode("customer", "customer_flat", "_", true),
                                sinkNode("sink-flatten")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "flatten-1", "input-0"),
                                new PipelineEdge("edge-2", "flatten-1", "output-0", "sink-flatten", "input-0"))),
                "flatten-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(null, sinkRecords.get(0).get("customer"));
        assertEquals(Map.of("id", "O-9"), sinkRecords.get(0).get("order"));
        assertEquals("new", sinkRecords.get(0).get("status"));
        assertEquals("C-1", sinkRecords.get(0).get("customer_flat_id"));
        assertEquals("Ada", sinkRecords.get(0).get("customer_flat_name"));
        assertEquals("Shanghai", sinkRecords.get(0).get("customer_flat_address_city"));
        assertEquals("200000", sinkRecords.get(0).get("customer_flat_address_zip"));
    }

    @Test
    void flattenTransformSupportsNestedSourceFieldAndDottedOutputPath() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-flatten-nested",
                new PipelineDefinition(
                        "pipeline-flatten-nested",
                        List.of(
                                jsonSourceNode("""
                                        {
                                          "payload": {
                                            "customer": {
                                              "id": "C-7",
                                              "address": {
                                                "city": "Hangzhou"
                                              }
                                            },
                                            "traceId": "trace-1"
                                          },
                                          "status": "new"
                                        }
                                        """),
                                flattenNode("payload.customer", "dim.customer", ".", true),
                                sinkNode("sink-flatten-nested")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "flatten-1", "input-0"),
                                new PipelineEdge("edge-2", "flatten-1", "output-0", "sink-flatten-nested", "input-0"))),
                "flatten-transform-nested-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("traceId", "trace-1"), sinkRecords.get(0).get("payload"));
        assertEquals(Map.of("customer", Map.of(
                "id", "C-7",
                "address", Map.of("city", "Hangzhou"))), sinkRecords.get(0).get("dim"));
        assertEquals("new", sinkRecords.get(0).get("status"));
    }

    @Test
    void flattenTransformPassesThroughMissingOrNonObjectSource() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-flatten-passthrough",
                new PipelineDefinition(
                        "pipeline-flatten-passthrough",
                        List.of(
                                jsonSourceNode(
                                        "{\"customer\":\"C-1\",\"status\":\"scalar\"}",
                                        "{\"status\":\"missing\"}"),
                                flattenNode("customer", "customer_flat", "_", true),
                                sinkNode("sink-flatten-passthrough")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "flatten-1", "input-0"),
                                new PipelineEdge("edge-2", "flatten-1", "output-0", "sink-flatten-passthrough", "input-0"))),
                "flatten-transform-passthrough-test");

        assertEquals(2, sinkRecords.size());
        assertEquals("C-1", sinkRecords.get(0).get("customer"));
        assertEquals("scalar", sinkRecords.get(0).get("status"));
        assertEquals("missing", sinkRecords.get(1).get("status"));
    }

    @Test
    void explodeTransformEmitsOneRecordPerArrayItem() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-explode",
                new PipelineDefinition(
                        "pipeline-explode",
                        List.of(
                                jsonSourceNode("""
                                        {
                                          "order": {
                                            "id": "O-1"
                                          },
                                          "items": [
                                            {"sku": "A", "qty": 1},
                                            {"sku": "B", "qty": 2}
                                          ],
                                          "status": "ready"
                                        }
                                        """),
                                explodeNode("items", "item", false),
                                sinkNode("sink-explode")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "explode-1", "input-0"),
                                new PipelineEdge("edge-2", "explode-1", "output-0", "sink-explode", "input-0"))),
                "explode-transform-test");

        assertEquals(2, sinkRecords.size());
        assertEquals(Map.of("id", "O-1"), sinkRecords.get(0).get("order"));
        assertEquals("ready", sinkRecords.get(0).get("status"));
        assertEquals(Map.of("sku", "A", "qty", 1), sinkRecords.get(0).get("item"));
        assertEquals(Map.of("sku", "B", "qty", 2), sinkRecords.get(1).get("item"));
    }

    @Test
    void explodeTransformKeepsOriginalRecordWhenArrayIsEmptyAndConfiguredToDoSo() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-explode-empty",
                new PipelineDefinition(
                        "pipeline-explode-empty",
                        List.of(
                                jsonSourceNode("""
                                        {
                                          "order": {
                                            "id": "O-2"
                                          },
                                          "items": [],
                                          "status": "empty"
                                        }
                                        """),
                                explodeNode("items", "item", true),
                                sinkNode("sink-explode-empty")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "explode-1", "input-0"),
                                new PipelineEdge("edge-2", "explode-1", "output-0", "sink-explode-empty", "input-0"))),
                "explode-transform-empty-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("id", "O-2"), sinkRecords.get(0).get("order"));
        assertEquals(List.of(), sinkRecords.get(0).get("items"));
        assertEquals("empty", sinkRecords.get(0).get("status"));
        assertEquals(null, sinkRecords.get(0).get("item"));
    }

    @Test
    void explodeTransformEmitsOneRecordForScalarSource() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-explode-scalar",
                new PipelineDefinition(
                        "pipeline-explode-scalar",
                        List.of(
                                jsonSourceNode("{\"item\":\"single\",\"status\":\"ready\"}"),
                                explodeNode("item", "explodedItem", false),
                                sinkNode("sink-explode-scalar")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "explode-1", "input-0"),
                                new PipelineEdge("edge-2", "explode-1", "output-0", "sink-explode-scalar", "input-0"))),
                "explode-transform-scalar-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("single", sinkRecords.get(0).get("item"));
        assertEquals("single", sinkRecords.get(0).get("explodedItem"));
        assertEquals("ready", sinkRecords.get(0).get("status"));
    }

    @Test
    void explodeTransformDropsMissingOrEmptySourceByDefault() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-explode-drop-empty",
                new PipelineDefinition(
                        "pipeline-explode-drop-empty",
                        List.of(
                                jsonSourceNode(
                                        "{\"items\":[],\"status\":\"empty\"}",
                                        "{\"status\":\"missing\"}"),
                                explodeNode("items", "item", false),
                                sinkNode("sink-explode-drop-empty")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "explode-1", "input-0"),
                                new PipelineEdge("edge-2", "explode-1", "output-0", "sink-explode-drop-empty", "input-0"))),
                "explode-transform-drop-empty-test");

        assertEquals(0, sinkRecords.size());
    }

    @Test
    void customCodeTransformCompilesJavaSourceAndUpdatesRecordFields() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-custom-code",
                new PipelineDefinition(
                        "pipeline-custom-code",
                        List.of(
                                jsonSourceNode("{\\\"test\\\":\\\"23\\\"}"),
                                customCodeNode("""
                                        import com.streamcraft.core.model.DataEntity;
                                        import com.streamcraft.core.runtime.transform.custom.CustomTransform;
                                        import com.streamcraft.core.runtime.transform.custom.CustomTransformContext;

                                        public class MyTransform implements CustomTransform {
                                            public DataEntity process(DataEntity input, CustomTransformContext context) {
                                                return input.withField("result.value", context.get(input, "test") + "-ok");
                                            }
                                        }
                                        """, "KEEP_ORIGINAL"),
                                sinkNode("sink-custom-code")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "custom-code-1", "input-0"),
                                new PipelineEdge("edge-2", "custom-code-1", "output-0", "sink-custom-code", "input-0"))),
                "custom-code-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("23", sinkRecords.get(0).get("test"));
        assertEquals(Map.of("value", "23-ok"), sinkRecords.get(0).get("result"));
    }

    @Test
    void customCodeTransformSkipsRecordWhenUserCodeReturnsNull() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-custom-code-null",
                new PipelineDefinition(
                        "pipeline-custom-code-null",
                        List.of(
                                jsonSourceNode("{\\\"test\\\":\\\"23\\\"}"),
                                customCodeNode("""
                                        import com.streamcraft.core.model.DataEntity;
                                        import com.streamcraft.core.runtime.transform.custom.CustomTransform;
                                        import com.streamcraft.core.runtime.transform.custom.CustomTransformContext;

                                        public class MyTransform implements CustomTransform {
                                            public DataEntity process(DataEntity input, CustomTransformContext context) {
                                                return null;
                                            }
                                        }
                                        """, "KEEP_ORIGINAL"),
                                sinkNode("sink-custom-code-null")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "custom-code-1", "input-0"),
                                new PipelineEdge("edge-2", "custom-code-1", "output-0", "sink-custom-code-null", "input-0"))),
                "custom-code-transform-null-test");

        assertEquals(0, sinkRecords.size());
    }

    @Test
    void customCodeTransformKeepsOriginalRecordWhenUserCodeThrowsByDefault() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-custom-code-error",
                new PipelineDefinition(
                        "pipeline-custom-code-error",
                        List.of(
                                jsonSourceNode("{\\\"test\\\":\\\"23\\\"}"),
                                customCodeNode("""
                                        import com.streamcraft.core.model.DataEntity;
                                        import com.streamcraft.core.runtime.transform.custom.CustomTransform;
                                        import com.streamcraft.core.runtime.transform.custom.CustomTransformContext;

                                        public class MyTransform implements CustomTransform {
                                            public DataEntity process(DataEntity input, CustomTransformContext context) {
                                                throw new IllegalStateException("boom");
                                            }
                                        }
                                        """, "KEEP_ORIGINAL"),
                                sinkNode("sink-custom-code-error")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "custom-code-1", "input-0"),
                                new PipelineEdge("edge-2", "custom-code-1", "output-0", "sink-custom-code-error", "input-0"))),
                "custom-code-transform-error-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("23", sinkRecords.get(0).get("test"));
        assertEquals(null, sinkRecords.get(0).get("result"));
    }

    @Test
    void timeDeriveTransformWritesPartitionFieldsFromPatternTimestamp() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-time-derive",
                new PipelineDefinition(
                        "pipeline-time-derive",
                        List.of(
                                jsonSourceNode("{\"eventTime\":\"2026-05-16 13:24:55\",\"amount\":7}"),
                                timeDeriveNode(),
                                sinkNode("sink-time-derive")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "time-derive-1", "input-0"),
                                new PipelineEdge("edge-2", "time-derive-1", "output-0", "sink-time-derive", "input-0"))),
                "time-derive-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("2026-05-16", sinkRecords.get(0).get("dt"));
        assertEquals(2026, sinkRecords.get(0).get("year"));
        assertEquals(5, sinkRecords.get(0).get("hour"));
        assertEquals(20, sinkRecords.get(0).get("week"));
        assertEquals(2, sinkRecords.get(0).get("quarter"));
        assertEquals("2026051605", sinkRecords.get(0).get("hourKey"));
    }

    @Test
    void timeDeriveTransformSetsDerivedFieldsToNullWhenConfigured() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-time-derive-null",
                new PipelineDefinition(
                        "pipeline-time-derive-null",
                        List.of(
                                jsonSourceNode("{\"eventTime\":\"bad-time\",\"amount\":7}"),
                                timeDeriveSetNullNode(),
                                sinkNode("sink-time-derive-null")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "time-derive-1", "input-0"),
                                new PipelineEdge("edge-2", "time-derive-1", "output-0", "sink-time-derive-null", "input-0"))),
                "time-derive-transform-null-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(null, sinkRecords.get(0).get("dt"));
        assertEquals(null, sinkRecords.get(0).get("hour"));
    }

    @Test
    void maskHashTransformMasksAndHashesConfiguredFields() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-mask-hash",
                new PipelineDefinition(
                        "pipeline-mask-hash",
                        List.of(
                                jsonSourceNode("{\"phone\":\"13812345678\",\"userId\":\"u-1\"}"),
                                maskHashNode(),
                                sinkNode("sink-mask-hash")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "mask-hash-1", "input-0"),
                                new PipelineEdge("edge-2", "mask-hash-1", "output-0", "sink-mask-hash", "input-0"))),
                "mask-hash-transform-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("138****5678", sinkRecords.get(0).get("phoneMasked"));
        assertEquals(sha256("salt-u-1"), sinkRecords.get(0).get("userHash"));
    }

    @Test
    void caseWhenTransformWritesFirstMatchingValue() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                "sink-case-when",
                new PipelineDefinition(
                        "pipeline-case-when",
                        List.of(
                                jsonSourceNode(
                                        "{\"amount\":120,\"status\":\"paid\"}",
                                        "{\"amount\":30,\"status\":\"paid\"}",
                                        "{\"amount\":5,\"status\":\"cancelled\"}"),
                                caseWhenNode(),
                                sinkNode("sink-case-when")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "case-when-1", "input-0"),
                                new PipelineEdge("edge-2", "case-when-1", "output-0", "sink-case-when", "input-0"))),
                "case-when-transform-test");

        assertEquals(3, sinkRecords.size());
        assertEquals("large_paid", sinkRecords.get(0).get("orderClass"));
        assertEquals("paid", sinkRecords.get(1).get("orderClass"));
        assertEquals("other", sinkRecords.get(2).get("orderClass"));
    }

    @Test
    void routeTransformEmitsRecordsToConfiguredOutputPorts() throws Exception {
        PipelineDefinition definition = new PipelineDefinition(
                "pipeline-route",
                List.of(
                        jsonSourceNode(
                                "{\"amount\":120,\"status\":\"paid\"}",
                                "{\"amount\":10,\"status\":\"cancelled\"}",
                                "{\"amount\":3,\"status\":\"pending\"}"),
                        routeNode(),
                        sinkNode("sink-large"),
                        sinkNode("sink-cancelled"),
                        sinkNode("sink-unmatched")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "route-1", "input-0"),
                        new PipelineEdge("edge-2", "route-1", "large", "sink-large", "input-0"),
                        new PipelineEdge("edge-3", "route-1", "cancelled", "sink-cancelled", "input-0"),
                        new PipelineEdge("edge-4", "route-1", "unmatched", "sink-unmatched", "input-0")));

        execute("sink-large", definition, "route-transform-test");

        assertEquals(1, CAPTURED_RECORDS.get("sink-large").size());
        assertEquals(120, CAPTURED_RECORDS.get("sink-large").get(0).get("amount"));
        assertEquals(1, CAPTURED_RECORDS.get("sink-cancelled").size());
        assertEquals("cancelled", CAPTURED_RECORDS.get("sink-cancelled").get(0).get("status"));
        assertEquals(1, CAPTURED_RECORDS.get("sink-unmatched").size());
        assertEquals("pending", CAPTURED_RECORDS.get("sink-unmatched").get(0).get("status"));
    }

    private List<Map<String, Object>> execute(String sinkId, PipelineDefinition definition, String jobName) throws Exception {
        return execute(sinkId, definition, jobName, new MockSourceFactory());
    }

    private List<Map<String, Object>> execute(
            String sinkId,
            PipelineDefinition definition,
            String jobName,
            MockSourceFactory mockSourceFactory) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1L);
        CAPTURED_RECORDS.clear();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                mockSourceFactory,
                new CapturingKafkaSinkFactory(),
                new TransformOperatorFactory(),
                true);

        runtime.run(definition);
        env.execute(jobName);
        return CAPTURED_RECORDS.getOrDefault(sinkId, List.of());
    }

    private static final class HoldingMockSourceFactory extends MockSourceFactory {

        private final long holdMillis;

        private HoldingMockSourceFactory(long holdMillis) {
            this.holdMillis = holdMillis;
        }

        @Override
        public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
            return super.create(env, sourceNode)
                    .union(env.addSource(new HoldingSourceFunction(holdMillis)).name(sourceNode.name() + "-timer-hold"));
        }
    }

    private static final class HoldingSourceFunction implements SourceFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private final long holdMillis;
        private volatile boolean running = true;

        private HoldingSourceFunction(long holdMillis) {
            this.holdMillis = holdMillis;
        }

        @Override
        public void run(SourceContext<DataEntity> ctx) throws Exception {
            long deadline = System.currentTimeMillis() + holdMillis;
            while (running && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class DelayedMockSourceFactory extends MockSourceFactory {

        private static final ObjectMapper SOURCE_OBJECT_MAPPER = new ObjectMapper();
        private final long delayAfterEachRecordMillis;

        private DelayedMockSourceFactory(long delayAfterEachRecordMillis) {
            this.delayAfterEachRecordMillis = delayAfterEachRecordMillis;
        }

        @Override
        public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
            JsonNode config = sourceNode.config();
            String format = config.path("format").asText("JSON");
            List<Map<String, Object>> sampleData = new ArrayList<>();
            for (JsonNode item : config.path("sampleData")) {
                try {
                    sampleData.add(KafkaSourceFactory.decodeFields(
                            format,
                            item.isTextual() ? item.asText() : "",
                            SOURCE_OBJECT_MAPPER));
                } catch (Exception exception) {
                    throw new IllegalArgumentException("Failed to parse delayed mock source sample.", exception);
                }
            }
            return env.addSource(new DelayedMockSourceFunction(sampleData, delayAfterEachRecordMillis))
                    .name(sourceNode.name());
        }
    }

    private static final class DelayedMockSourceFunction implements SourceFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private final List<Map<String, Object>> sampleData;
        private final long delayAfterEachRecordMillis;
        private volatile boolean running = true;

        private DelayedMockSourceFunction(List<Map<String, Object>> sampleData, long delayAfterEachRecordMillis) {
            this.sampleData = List.copyOf(sampleData);
            this.delayAfterEachRecordMillis = delayAfterEachRecordMillis;
        }

        @Override
        public void run(SourceContext<DataEntity> ctx) throws Exception {
            for (Map<String, Object> record : sampleData) {
                if (!running) {
                    break;
                }
                ctx.collect(new DataEntity(
                        UUID.randomUUID().toString(),
                        System.currentTimeMillis(),
                        new LinkedHashMap<>(record),
                        Map.of("source", "delayed-mock")));
                if (delayAfterEachRecordMillis > 0) {
                    Thread.sleep(delayAfterEachRecordMillis);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private PipelineNode textSourceNode(String sampleData) {
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
                          "format": "TEXT",
                          "sampleData": ["%s"]
                        }
                        """.formatted(sampleData)));
    }

    private PipelineNode jsonSourceNode(String... sampleData) {
        return namedJsonSourceNode("source-1", sampleData);
    }

    private PipelineNode namedJsonSourceNode(String nodeId, String... sampleData) {
        ObjectNode config = objectMapper.createObjectNode()
                .put("bootstrapServers", "127.0.0.1:9092")
                .put("groupId", "group-1")
                .put("consumeMode", "earliest")
                .put("authType", "NONE")
                .put("format", "JSON");
        config.putArray("topics").add("input-topic");
        ArrayNode samples = config.putArray("sampleData");
        for (String sample : sampleData) {
            samples.add(sample.replace("\\\"", "\""));
        }
        return new PipelineNode(
                nodeId,
                "Source",
                PipelineNodeType.SOURCE,
                PipelineOperator.KAFKA_SOURCE,
                config);
    }

    private PipelineNode grokNode(String inputField, String outputField, String pattern) {
        return new PipelineNode(
                "grok-1",
                "Grok",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.GROK,
                jsonNode("""
                        {
                          "inputField": "%s",
                          "outputField": "%s",
                          "pattern": "%s"
                        }
                        """.formatted(inputField, outputField, pattern)));
    }

    private PipelineNode castNode(String inputField, String outputField, String targetType) {
        return new PipelineNode(
                "cast-1",
                "Cast",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.CAST,
                jsonNode("""
                        {
                          "inputField": "%s",
                          "outputField": "%s",
                          "targetType": "%s"
                        }
                        """.formatted(inputField, outputField, targetType)));
    }

    private PipelineNode evalNode(String targetField, String expression) {
        return evalNode(targetField, expression, "OVERWRITE", "KEEP_ORIGINAL");
    }

    private PipelineNode evalNode(String targetField, String expression, String outputMode, String errorStrategy) {
        return new PipelineNode(
                "eval-1",
                "Eval",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.EVAL,
                jsonNode("""
                        {
                          "targetField": "%s",
                          "expression": "%s",
                          "outputMode": "%s",
                          "errorStrategy": "%s"
                        }
                        """.formatted(targetField, expression, outputMode, errorStrategy)));
    }

    private PipelineNode deduplicateNode(List<String> keyFields, long ttlSeconds, String keepStrategy) {
        ObjectNode config = objectMapper.createObjectNode()
                .put("ttlSeconds", ttlSeconds)
                .put("keepStrategy", keepStrategy);
        ArrayNode fields = config.putArray("keyFields");
        keyFields.forEach(fields::add);
        return new PipelineNode(
                "deduplicate-1",
                "Deduplicate",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.DEDUPLICATE,
                config);
    }

    private PipelineNode deduplicateEventTimeNode(
            List<String> keyFields,
            long windowSeconds,
            long watermarkDelaySeconds,
            String keepStrategy) {
        ObjectNode config = objectMapper.createObjectNode()
                .put("timeMode", "EVENT_TIME")
                .put("eventTimeField", "eventTime")
                .put("windowSeconds", windowSeconds)
                .put("watermarkDelaySeconds", watermarkDelaySeconds)
                .put("keepStrategy", keepStrategy)
                .put("lateDataStrategy", "DISCARD")
                .put("duplicateStrategy", "DISCARD");
        ArrayNode fields = config.putArray("keyFields");
        keyFields.forEach(fields::add);
        return new PipelineNode(
                "deduplicate-1",
                "Deduplicate",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.DEDUPLICATE,
                config);
    }

    private PipelineNode lookupEnrichNode(
            String sourceField,
            String targetField,
            List<Map<String, Object>> entries,
            String missingStrategy,
            boolean overwriteTargetField) {
        ObjectNode config = objectMapper.createObjectNode()
                .put("sourceField", sourceField)
                .put("targetField", targetField)
                .put("missingStrategy", missingStrategy)
                .put("overwriteTargetField", overwriteTargetField);
        ArrayNode entryArray = config.putArray("entries");
        for (Map<String, Object> entry : entries) {
            ObjectNode item = entryArray.addObject();
            item.put("key", String.valueOf(entry.get("key")));
            item.put("value", String.valueOf(entry.get("value")));
            if (entry.containsKey("valueType")) {
                item.put("valueType", String.valueOf(entry.get("valueType")));
            }
        }
        return new PipelineNode(
                "lookup-enrich-1",
                "Lookup enrich",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("LOOKUP_ENRICH"),
                config);
    }

    private PipelineNode lookupJoinNode(String joinType, String missingStrategy) {
        return new PipelineNode(
                "lookup-join-1",
                "Lookup join",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("LOOKUP_JOIN"),
                jsonNode("""
                        {
                          "sourceField": "countryCode",
                          "targetField": "country",
                          "joinType": "%s",
                          "missingStrategy": "%s",
                          "overwriteTargetField": false,
                          "entries": [
                            {"key": "CN", "fields": {"name": "China", "region": "APAC"}},
                            {"key": "US", "fields": {"name": "United States", "region": "NA"}}
                          ]
                        }
                        """.formatted(joinType, missingStrategy)));
    }

    private PipelineNode flattenNode(String sourceField, String targetPrefix, String delimiter, boolean removeSourceField) {
        return new PipelineNode(
                "flatten-1",
                "Flatten",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("FLATTEN"),
                objectMapper.createObjectNode()
                        .put("sourceField", sourceField)
                        .put("targetPrefix", targetPrefix)
                        .put("delimiter", delimiter)
                        .put("removeSourceField", removeSourceField));
    }

    private PipelineNode explodeNode(String sourceField, String targetField, boolean keepEmpty) {
        return new PipelineNode(
                "explode-1",
                "Explode",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("EXPLODE"),
                objectMapper.createObjectNode()
                        .put("sourceField", sourceField)
                        .put("targetField", targetField)
                        .put("keepEmpty", keepEmpty));
    }

    private PipelineNode customCodeNode(String sourceCode, String errorStrategy) {
        return new PipelineNode(
                "custom-code-1",
                "Custom Code",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.CUSTOM_CODE,
                objectMapper.createObjectNode()
                        .put("language", "JAVA")
                        .put("compilePattern", "SOURCE_CODE")
                        .put("className", "MyTransform")
                        .put("sourceCode", sourceCode)
                        .put("errorStrategy", errorStrategy));
    }

    private PipelineNode timeDeriveNode() {
        return new PipelineNode(
                "time-derive-1",
                "Time derive",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("TIME_DERIVE"),
                jsonNode("""
                        {
                          "sourceField": "eventTime",
                          "sourceFormat": "PATTERN",
                          "sourcePattern": "yyyy-MM-dd HH:mm:ss",
                          "sourceTimeZone": "Asia/Shanghai",
                          "outputTimeZone": "UTC",
                          "parseErrorStrategy": "KEEP_ORIGINAL",
                          "derivations": [
                            {"outputField": "dt", "type": "DATE"},
                            {"outputField": "year", "type": "YEAR"},
                            {"outputField": "hour", "type": "HOUR"},
                            {"outputField": "week", "type": "WEEK"},
                            {"outputField": "quarter", "type": "QUARTER"},
                            {"outputField": "hourKey", "type": "FORMAT", "pattern": "yyyyMMddHH"}
                          ]
                        }
                        """));
    }

    private PipelineNode timeDeriveSetNullNode() {
        return new PipelineNode(
                "time-derive-1",
                "Time derive",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("TIME_DERIVE"),
                jsonNode("""
                        {
                          "sourceField": "eventTime",
                          "sourceFormat": "PATTERN",
                          "sourcePattern": "yyyy-MM-dd HH:mm:ss",
                          "sourceTimeZone": "Asia/Shanghai",
                          "outputTimeZone": "UTC",
                          "parseErrorStrategy": "SET_NULL",
                          "derivations": [
                            {"outputField": "dt", "type": "DATE"},
                            {"outputField": "hour", "type": "HOUR"}
                          ]
                        }
                        """));
    }

    private PipelineNode streamJoinNode() {
        return new PipelineNode(
                "stream-join-1",
                "Stream join",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("STREAM_JOIN"),
                jsonNode("""
                        {
                          "leftKeyField": "orderId",
                          "rightKeyField": "orderId",
                          "targetField": "detail",
                          "joinType": "LEFT",
                          "missingStrategy": "KEEP_ORIGINAL",
                          "overwriteTargetField": false,
                          "timeMode": "PROCESSING_TIME",
                          "timeUnit": "SECONDS",
                          "windowBefore": 60,
                          "windowAfter": 60,
                          "watermarkDelay": 30
                        }
                        """));
    }

    private PipelineNode maskHashNode() {
        return new PipelineNode(
                "mask-hash-1",
                "Mask hash",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("MASK_HASH"),
                jsonNode("""
                        {
                          "rules": [
                            {"sourceField": "phone", "targetField": "phoneMasked", "action": "MASK", "keepFirst": 3, "keepLast": 4},
                            {"sourceField": "userId", "targetField": "userHash", "action": "HASH", "algorithm": "SHA256", "salt": "salt-"}
                          ]
                        }
                        """));
    }

    private PipelineNode caseWhenNode() {
        return new PipelineNode(
                "case-when-1",
                "Case when",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("CASE_WHEN"),
                jsonNode("""
                        {
                          "targetField": "orderClass",
                          "cases": [
                            {"condition": "amount >= 100 and status == 'paid'", "value": "large_paid"},
                            {"condition": "status == 'paid'", "value": "paid"}
                          ],
                          "defaultValue": "other"
                        }
                        """));
    }

    private PipelineNode routeNode() {
        return new PipelineNode(
                "route-1",
                "Route",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("ROUTE"),
                jsonNode("""
                        {
                          "matchMode": "FIRST_MATCH",
                          "includeUnmatched": true,
                          "unmatchedPort": "unmatched",
                          "routes": [
                            {"portId": "large", "condition": "amount >= 100"},
                            {"portId": "cancelled", "condition": "status == 'cancelled'"}
                          ]
                        }
                        """));
    }

    private String sha256(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
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

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static boolean exceptionMessageContains(Throwable exception, String expected) {
        Throwable current = exception;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
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
            CAPTURED_RECORDS.get(sinkId).add(new LinkedHashMap<>(value.fields()));
        }
    }
}
