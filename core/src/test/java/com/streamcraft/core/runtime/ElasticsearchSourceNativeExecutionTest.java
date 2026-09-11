package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import com.sun.net.httpserver.HttpServer;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.junit.jupiter.api.Test;

class ElasticsearchSourceNativeExecutionTest {

    @Test
    void emitsIncrementalHitsAndAdvancesCompositeCursorAfterEmission() throws Exception {
        List<String> bodies = new ArrayList<>();
        int[] requestCount = {0};
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            String response = requestCount[0]++ == 0
                    ? "{\"_scroll_id\":\"s1\",\"hits\":{\"hits\":[{\"_id\":\"2\",\"_source\":{\"id\":\"2\",\"updated_at\":200,\"value\":\"b\"}}]}}"
                    : "{\"_scroll_id\":\"s1\",\"hits\":{\"hits\":[]}}";
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        try {
            ElasticsearchSourceConfig config = new ElasticsearchSourceConfig(
                    List.of("http://127.0.0.1:" + server.getAddress().getPort()), "orders", List.of(), "{}",
                    ElasticsearchSourceConfig.ReadMode.INCREMENTAL, "updated_at",
                    ElasticsearchSourceConfig.CursorType.LONG, "100", 1, 10, "1m", 1,
                    "id", "", ElasticsearchSourceConfig.AuthType.NONE, "", "", "");
            ElasticsearchSourceReader reader = new ElasticsearchSourceReader(context(), config);
            reader.start();
            reader.addSplits(List.of(new ElasticsearchSourceSplit(
                    "es", new IncrementalSourceState("LONG", "100", "id", "1"), "", 0)));
            List<DataEntity> output = new ArrayList<>();
            ReaderOutput<DataEntity> sink = output(output);
            reader.pollNext(sink);
            assertEquals("2", output.get(0).id());
            assertEquals("200", reader.snapshotState(1).get(0).state().cursorValue());
            assertEquals("2", reader.snapshotState(1).get(0).state().uniqueIdValue());
            assertEquals(true, bodies.get(0).contains("\"sort\":[{\"updated_at\":\"asc\"},{\"id\":\"asc\"}]"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsCheckpointWithDifferentIdField() {
        ElasticsearchSourceConfig config = new ElasticsearchSourceConfig(
                List.of("http://127.0.0.1:9200"), "orders", List.of(), "{}",
                ElasticsearchSourceConfig.ReadMode.INCREMENTAL, "updated_at",
                ElasticsearchSourceConfig.CursorType.LONG, "", 1, 10, "1m", 1,
                "id", "", ElasticsearchSourceConfig.AuthType.NONE, "", "", "");
        ElasticsearchSourceReader reader = new ElasticsearchSourceReader(context(), config);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> reader.addSplits(List.of(new ElasticsearchSourceSplit(
                        "es", new IncrementalSourceState("LONG", "100", "legacy_id", "1"), "", 0))));

        assertEquals(
                "Elasticsearch Source checkpoint state version 1 idField changed from legacy_id to id.",
                exception.getMessage());
    }

    private SourceReaderContext context() {
        return (SourceReaderContext) Proxy.newProxyInstance(
                SourceReaderContext.class.getClassLoader(), new Class<?>[] {SourceReaderContext.class},
                (ignored, method, args) -> null);
    }

    @SuppressWarnings("unchecked")
    private ReaderOutput<DataEntity> output(List<DataEntity> values) {
        return (ReaderOutput<DataEntity>) Proxy.newProxyInstance(
                ReaderOutput.class.getClassLoader(), new Class<?>[] {ReaderOutput.class},
                (ignored, method, args) -> {
                    if (method.getName().equals("collect")) {
                        values.add((DataEntity) args[0]);
                    }
                    return null;
                });
    }
}
