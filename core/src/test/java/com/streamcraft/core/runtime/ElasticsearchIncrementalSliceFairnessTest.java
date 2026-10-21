package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import com.sun.net.httpserver.HttpServer;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class ElasticsearchIncrementalSliceFairnessTest {
    @Test
    void rotatesSlicesAndRestoresEachCompositeCursor() throws Exception {
        List<JsonNode> requests = new ArrayList<>();
        try (Stub stub = new Stub(request -> {
            requests.add(request);
            int slice = request.path("slice").path("id").asInt();
            return hits("", "id-" + slice, slice == 0 ? 10 : 20);
        })) {
            var config = config(stub.url(), 1, 0);
            List<ElasticsearchSourceSplit> saved;
            try (var reader = reader(config, ElasticsearchSource.splitPlan(config))) {
                List<DataEntity> rows = new ArrayList<>();
                for (int i = 0; i < 8 && requests.size() < 2; i++) reader.pollNext(output(rows));
                assertEquals(List.of(0, 1), requests.stream().map(r -> r.path("slice").path("id").asInt()).toList());
                saved = reader.snapshotState(1);
                assertEquals(Map.of(0, "10", 1, "20"), saved.stream().collect(java.util.stream.Collectors.toMap(
                        ElasticsearchSourceSplit::sliceId, s -> s.state().cursorValue())));
                assertTrue(saved.stream().allMatch(s -> s.sliceMax() == 2));
            }
            var serializer = new ElasticsearchSourceSplitSerializer();
            List<ElasticsearchSourceSplit> restored = new ArrayList<>();
            for (var split : saved) restored.add(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)));
            requests.clear();
            try (var reader = reader(config, restored)) {
                for (int i = 0; i < 8 && requests.size() < 2; i++) reader.pollNext(output(new ArrayList<>()));
                assertEquals(2, requests.size());
                for (JsonNode request : requests) {
                    int slice = request.path("slice").path("id").asInt();
                    JsonNode should = request.path("query").path("bool").path("should");
                    assertEquals(slice == 0 ? 10 : 20, should.get(0).path("range").path("updated_at").path("gt").asInt());
                    assertEquals("id-" + slice, should.get(1).path("bool").path("must").get(1)
                            .path("range").path("id").path("gt").asText());
                }
            }
        }
    }

    @Test
    void emptySliceDoesNotDelayReadySliceAndAllCoolingSlicesWaitWithoutBusyLoop() throws Exception {
        List<Integer> queried = new ArrayList<>();
        try (Stub stub = new Stub(request -> {
            int slice = request.path("slice").path("id").asInt(); queried.add(slice);
            return slice == 0 ? hits("") : hits("", "one", 1);
        })) {
            try (var reader = reader(config(stub.url(), 60_000, 0), null)) {
                List<DataEntity> rows = new ArrayList<>();
                reader.pollNext(output(rows)); reader.pollNext(output(rows));
                assertEquals(List.of(0, 1), queried, "An empty slice must yield to another ready slice");
                assertEquals(1, rows.size());
                assertFalse(reader.isAvailable().isDone(), "All completed slices must honor the poll interval");
                for (int i = 0; i < 8; i++) assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(output(rows)));
                assertEquals(2, queried.size(), "Polling before availability must not issue more HTTP queries");
            }
        }
    }

    @Test
    void drainsEveryScrollPageAndBufferedRowBeforeRotating() throws Exception {
        List<String> events = new ArrayList<>();
        try (Stub stub = new Stub(request -> {
            if (request.has("scroll_id")) {
                String scroll = request.path("scroll_id").asText(); events.add(scroll);
                return scroll.equals("page-1") ? hits("page-2", "b", 2) : hits("");
            }
            int slice = request.path("slice").path("id").asInt(); events.add("slice-" + slice);
            return slice == 0 ? hits("page-1", "a", 1, "a2", 1) : hits("", "c", 3);
        })) {
            try (var reader = reader(config(stub.url(), 60_000, 0), null)) {
                List<DataEntity> rows = new ArrayList<>();
                for (int i = 0; i < 8 && rows.size() < 4; i++) reader.pollNext(output(rows));
                assertEquals(List.of("slice-0", "page-1", "page-2", "slice-1"), events);
                assertEquals(List.of("a", "a2", "b", "c"), rows.stream().map(DataEntity::id).toList());
                assertFalse(reader.isAvailable().isDone());
            }
        }
    }

    @Test
    void lateReadySliceWakesAvailabilityAndBypassesCoolingActiveSlice() throws Exception {
        List<Integer> queried = new ArrayList<>();
        try (Stub stub = new Stub(request -> {
            queried.add(request.path("slice").path("id").asInt()); return hits("");
        })) {
            var config = config(stub.url(), 60_000, 0);
            try (var reader = reader(config, List.of(ElasticsearchSource.splitPlan(config).get(0)))) {
                reader.pollNext(output(new ArrayList<>()));
                var available = reader.isAvailable();
                assertFalse(available.isDone());
                reader.addSplits(List.of(ElasticsearchSource.splitPlan(config).get(1)));
                assertTrue(available.isDone(), "A new ready assignment must wake the existing mailbox wait");
                reader.pollNext(output(new ArrayList<>()));
                assertEquals(List.of(0, 1), queried);
                assertFalse(reader.isAvailable().isDone(), "Both completed splits retain their cooldown");
            }
        }
    }

    @Test
    void finitePollLimitStillRunsEveryConfiguredCyclePerSlice() throws Exception {
        List<Integer> queried = new ArrayList<>();
        try (Stub stub = new Stub(request -> {
            int slice = request.path("slice").path("id").asInt(); queried.add(slice); return hits("", "row", 1);
        })) {
            try (var reader = reader(config(stub.url(), 1, 2), null)) {
                reader.notifyNoMoreSplits();
                for (int i = 0; i < 20; i++) if (reader.pollNext(output(new ArrayList<>())) == InputStatus.END_OF_INPUT) break;
                assertEquals(List.of(0, 0, 1, 1), queried);
            }
        }
    }

    private static ElasticsearchSourceConfig config(String host, long interval, int maxPolls) {
        return new ElasticsearchSourceConfig(List.of(host), "orders", List.of(), "{}",
                ElasticsearchSourceConfig.ReadMode.INCREMENTAL, "updated_at", ElasticsearchSourceConfig.CursorType.LONG,
                "", interval, 2, "1m", maxPolls, "id", "", ElasticsearchSourceConfig.AuthType.NONE, "", "", "", 2);
    }

    private static ElasticsearchSourceReader reader(ElasticsearchSourceConfig config, List<ElasticsearchSourceSplit> splits) {
        var context = (SourceReaderContext) Proxy.newProxyInstance(SourceReaderContext.class.getClassLoader(),
                new Class<?>[] {SourceReaderContext.class}, (proxy, method, args) -> null);
        var reader = new ElasticsearchSourceReader(context, config); reader.start();
        reader.addSplits(splits == null ? ElasticsearchSource.splitPlan(config) : splits); return reader;
    }

    @SuppressWarnings("unchecked")
    private static ReaderOutput<DataEntity> output(List<DataEntity> rows) {
        return (ReaderOutput<DataEntity>) Proxy.newProxyInstance(ReaderOutput.class.getClassLoader(),
                new Class<?>[] {ReaderOutput.class}, (proxy, method, args) -> {
                    if (method.getName().equals("collect")) rows.add((DataEntity) args[0]); return null;
                });
    }

    private static String hits(String scroll, Object... pairs) {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < pairs.length; i += 2) rows.add("{\"_id\":\"" + pairs[i]
                + "\",\"_source\":{\"id\":\"" + pairs[i] + "\",\"updated_at\":" + pairs[i + 1] + "}}");
        return "{\"_scroll_id\":\"" + scroll + "\",\"hits\":{\"hits\":[" + String.join(",", rows) + "]}}";
    }

    private static final class Stub implements AutoCloseable {
        private final HttpServer server;
        Stub(Function<JsonNode, String> response) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                JsonNode request = mapper.readTree(exchange.getRequestBody());
                String value = exchange.getRequestMethod().equals("DELETE") ? "{}" : response.apply(request);
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8); exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes); exchange.close();
            }); server.start();
        }
        String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        public void close() { server.stop(0); }
    }
}
