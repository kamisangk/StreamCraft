package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig;
import com.sun.net.httpserver.HttpServer;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class SourceReaderReliabilityRegressionTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void fullCheckpointRetainsReplayableSplitUntilBufferedRowsAreEmitted() throws Exception {
        try (Stub stub = new Stub(request -> hits("", "a", "b", "c"))) {
            var config = es(stub.url(), false, 1, 0);
            List<DataEntity> values = new ArrayList<>();
            List<ElasticsearchSourceSplit> saved;
            try (var reader = new ElasticsearchSourceReader(context(), config)) {
                reader.start(); reader.addSplits(ElasticsearchSource.splitPlan(config));
                reader.notifyNoMoreSplits(); reader.pollNext(output(values));
                saved = reader.snapshotState(1);
                assertEquals(1, saved.size(), "Buffered rows require a replayable split");
            }
            try (var reader = new ElasticsearchSourceReader(context(), config)) {
                reader.start(); reader.addSplits(saved); reader.notifyNoMoreSplits();
                drain(reader, values);
            }
            assertEquals(List.of("a", "b", "c"), values.stream().map(DataEntity::id).distinct().sorted().toList());
        }
    }

    @Test
    void unrestrictedProjectionDoesNotSendSourceFilter() throws Exception {
        List<JsonNode> requests = new ArrayList<>();
        try (Stub stub = new Stub(request -> { requests.add(request); return hits("", "a"); })) {
            var config = es(stub.url(), true, 1, 1);
            try (var reader = new ElasticsearchSourceReader(context(), config)) {
                reader.start(); reader.addSplits(ElasticsearchSource.splitPlan(config));
                reader.pollNext(output(new ArrayList<>()));
                assertFalse(requests.get(0).has("_source"));
            }
        }
    }

    @Test
    void incrementalPollLimitWaitsForLateSplitUntilNoMoreSplits() throws Exception {
        try (Stub stub = new Stub(request -> hits("", "a"))) {
            var config = es(stub.url(), true, 1, 1);
            try (var reader = new ElasticsearchSourceReader(context(), config)) {
                reader.start(); reader.addSplits(ElasticsearchSource.splitPlan(config));
                List<DataEntity> values = new ArrayList<>(); reader.pollNext(output(values));
                assertNotEquals(InputStatus.END_OF_INPUT, reader.pollNext(output(values)));
                reader.addSplits(ElasticsearchSource.splitPlan(config)); reader.notifyNoMoreSplits();
                drain(reader, values);
                assertEquals(2, values.size());
            }
        }
    }

    @Test
    void incrementalQueriesKeepElasticsearchRangeAndCompositeWrappers() throws Exception {
        List<JsonNode> requests = new ArrayList<>();
        try (Stub stub = new Stub(request -> { requests.add(request); return hits(""); })) {
            var config = es(stub.url(), true, 1, 1);
            for (String uniqueId : new String[] {null, "a"}) {
                try (var reader = new ElasticsearchSourceReader(context(), config)) {
                    reader.start(); reader.addSplits(List.of(new ElasticsearchSourceSplit("query",
                            new IncrementalSourceState("LONG", "updated_at", "100", "id", uniqueId), "", 0)));
                    reader.pollNext(output(new ArrayList<>()));
                }
            }
            JsonNode simple = requests.get(0).path("query").path("bool").path("must").get(1);
            assertEquals(100, simple.path("range").path("updated_at").path("gt").asInt());
            JsonNode should = requests.get(1).path("query").path("bool").path("should");
            assertEquals(100, should.get(0).path("range").path("updated_at").path("gt").asInt());
            JsonNode ties = should.get(1).path("bool").path("must");
            assertEquals(100, ties.get(0).path("term").path("updated_at").asInt());
            assertEquals("a", ties.get(1).path("range").path("id").path("gt").asText());
        }
    }

    @Test
    void incrementalEmissionPreservesSliceMetadata() throws Exception {
        try (Stub stub = new Stub(request -> hits("", "a"))) {
            var config = es(stub.url(), true, 2, 1);
            try (var reader = new ElasticsearchSourceReader(context(), config)) {
                reader.start(); reader.addSplits(List.of(ElasticsearchSource.splitPlan(config).get(1)));
                reader.pollNext(output(new ArrayList<>()));
                var saved = reader.snapshotState(1).get(0);
                assertEquals(1, saved.sliceId()); assertEquals(2, saved.sliceMax());
            }
        }
    }

    @Test
    void fullReaderContinuesAfterEmptySlice() throws Exception {
        try (Stub stub = new Stub(request -> request.path("slice").path("id").asInt() == 0
                ? hits("") : hits("", "b"))) {
            var config = es(stub.url(), false, 2, 0);
            try (var reader = new ElasticsearchSourceReader(context(), config)) {
                reader.start(); reader.addSplits(ElasticsearchSource.splitPlan(config)); reader.notifyNoMoreSplits();
                List<DataEntity> values = new ArrayList<>(); drain(reader, values);
                assertEquals(List.of("b"), values.stream().map(DataEntity::id).toList());
            }
        }
    }

    @Test
    void fullScrollPagesAreNotLimitedByIncrementalMaxPollsAndYieldBetweenPages() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Stub stub = new Stub(request -> switch (calls.incrementAndGet()) {
            case 1 -> hits("scroll-1", "a");
            case 2 -> hits("scroll-2", "b");
            default -> hits("");
        })) {
            var config = es(stub.url(), false, 1, 1);
            try (var reader = new ElasticsearchSourceReader(context(), config)) {
                reader.start(); reader.addSplits(ElasticsearchSource.splitPlan(config)); reader.notifyNoMoreSplits();
                List<DataEntity> values = new ArrayList<>(); reader.pollNext(output(values));
                assertEquals(1, calls.get(), "A mailbox poll fetches at most one scroll page");
                drain(reader, values);
                assertEquals(List.of("a", "b"), values.stream().map(DataEntity::id).toList());
            }
        }
    }

    @Test
    void scrollReadYieldsAfterOnePageWithoutAPollLimit() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Stub stub = new Stub(request -> calls.incrementAndGet() == 1 ? hits("scroll-1", "a") : hits(""))) {
            var config = es(stub.url(), false, 1, 0);
            try (var reader = new ElasticsearchSourceReader(context(), config)) {
                reader.start(); reader.addSplits(ElasticsearchSource.splitPlan(config));
                reader.pollNext(output(new ArrayList<>()));
                assertEquals(1, calls.get());
                assertEquals(1, reader.snapshotState(1).size(), "An unread next page requires replayable state");
            }
        }
    }

    @Test
    void influxFullReaderEmitsAllMeasurementAndRangeSplits() throws Exception {
        AtomicInteger queries = new AtomicInteger();
        try (Stub stub = new Stub(request -> {
            int n = queries.incrementAndGet();
            return "{\"results\":[{\"series\":[{\"name\":\"m" + n
                    + "\",\"columns\":[\"time\",\"value\"],\"values\":[[1000,1]]}]}]}";
        })) {
            var config = new InfluxDbSourceConfig(stub.url(), "metrics", "select * from cpu", "{}", "ms", 10, 1000,
                    InfluxDbSourceConfig.ReadMode.FULL, "", InfluxDbSourceConfig.CursorType.STRING,
                    "", 1, 0, "", "time", "", "", List.of("cpu", "mem"), 1, "", "", "", 0);
            var state = new IncrementalSourceState("STRING", "", null, null, null);
            var splits = new ArrayList<>(InfluxDbSource.splitPlan(config));
            splits.add(new InfluxDbSourceSplit("range", state, 0, "", "0", "10", true));
            try (var reader = new InfluxDbSourceReader(context(), config)) {
                reader.start(); reader.addSplits(splits); reader.notifyNoMoreSplits();
                List<DataEntity> values = new ArrayList<>();
                for (int i = 0; i < 20; i++) if (reader.pollNext(output(values)) == InputStatus.END_OF_INPUT) break;
                assertEquals(3, values.size()); assertEquals(3, queries.get());
            }
        }
    }

    private static ElasticsearchSourceConfig es(String host, boolean incremental, int slices, int maxPolls) {
        return new ElasticsearchSourceConfig(List.of(host), "orders", List.of(), "{}",
                incremental ? ElasticsearchSourceConfig.ReadMode.INCREMENTAL : ElasticsearchSourceConfig.ReadMode.FULL,
                incremental ? "updated_at" : "", ElasticsearchSourceConfig.CursorType.LONG, "", 1, 2, "1m",
                maxPolls, "id", "", ElasticsearchSourceConfig.AuthType.NONE, "", "", "", slices);
    }

    private static String hits(String scroll, String... ids) {
        List<String> rows = new ArrayList<>();
        for (String id : ids) rows.add("{\"_id\":\"" + id + "\",\"_source\":{\"id\":\"" + id + "\",\"updated_at\":1}}");
        return "{\"_scroll_id\":\"" + scroll + "\",\"hits\":{\"hits\":[" + String.join(",", rows) + "]}}";
    }

    private static void drain(ElasticsearchSourceReader reader, List<DataEntity> values) throws Exception {
        for (int i = 0; i < 30; i++) if (reader.pollNext(output(values)) == InputStatus.END_OF_INPUT) return;
        fail("Bounded reader did not finish");
    }

    private static SourceReaderContext context() {
        return (SourceReaderContext) Proxy.newProxyInstance(SourceReaderContext.class.getClassLoader(),
                new Class<?>[] {SourceReaderContext.class}, (proxy, method, args) -> null);
    }

    @SuppressWarnings("unchecked")
    private static ReaderOutput<DataEntity> output(List<DataEntity> values) {
        return (ReaderOutput<DataEntity>) Proxy.newProxyInstance(ReaderOutput.class.getClassLoader(),
                new Class<?>[] {ReaderOutput.class}, (proxy, method, args) -> {
                    if (method.getName().equals("collect")) values.add((DataEntity) args[0]); return null;
                });
    }

    private static final class Stub implements AutoCloseable {
        private final HttpServer server;
        Stub(Function<JsonNode, String> response) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                byte[] request = exchange.getRequestBody().readAllBytes();
                JsonNode body = request.length == 0 ? MAPPER.createObjectNode() : MAPPER.readTree(request);
                String value = exchange.getRequestMethod().equals("DELETE") ? "{}" : response.apply(body);
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length); exchange.getResponseBody().write(bytes); exchange.close();
            }); server.start();
        }
        String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        public void close() { server.stop(0); }
    }
}
