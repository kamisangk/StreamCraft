package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.elasticsearch.ElasticsearchSinkConfig;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.junit.jupiter.api.Test;

class ElasticsearchSinkCheckpointTest {

    @Test
    void lowVolumeRecordsFlushWhenCheckpointCompletes() throws Exception {
        try (TestServer server = new TestServer(items(201))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 0);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(1L));

            assertEquals(0, server.requestCount());
            sink.notifyCheckpointComplete(1L);

            assertEquals(1, server.requestCount());
            assertEquals(0, sink.bufferedRecordCount());
            assertTrue(server.requestBodies().get(0).contains("\"_id\":\"1\""));
        }
    }

    @Test
    void recordsArrivingAfterSnapshotWaitForALaterCheckpoint() throws Exception {
        try (TestServer server = new TestServer(items(201), items(201))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 0);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(2L));
            sink.invoke(entity(2L, "Grace"), null);

            sink.notifyCheckpointComplete(2L);

            assertEquals(1, server.requestCount());
            assertTrue(server.requestBodies().get(0).contains("\"_id\":\"1\""));
            assertFalse(server.requestBodies().get(0).contains("\"_id\":\"2\""));
            assertEquals(1, sink.bufferedRecordCount());

            sink.snapshotState(context(3L));
            sink.notifyCheckpointComplete(3L);

            assertEquals(2, server.requestCount());
            assertTrue(server.requestBodies().get(1).contains("\"_id\":\"2\""));
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void abortedCheckpointDoesNotFlushItsSnapshot() throws Exception {
        try (TestServer server = new TestServer(items(201))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 0);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(4L));
            sink.notifyCheckpointAborted(4L);
            sink.notifyCheckpointComplete(4L);

            assertEquals(0, server.requestCount());
            assertEquals(1, sink.bufferedRecordCount());
        }
    }

    @Test
    void retriesOnlyTransientBulkItems() throws Exception {
        try (TestServer server = new TestServer(items(201, 429), items(201))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 1);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.invoke(entity(2L, "Grace"), null);
            sink.snapshotState(context(5L));

            sink.notifyCheckpointComplete(5L);

            assertEquals(2, server.requestCount());
            assertTrue(server.requestBodies().get(0).contains("\"_id\":\"1\""));
            assertTrue(server.requestBodies().get(0).contains("\"_id\":\"2\""));
            assertFalse(server.requestBodies().get(1).contains("\"_id\":\"1\""));
            assertTrue(server.requestBodies().get(1).contains("\"_id\":\"2\""));
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void retriesItemServerErrorWithBoundedAttempts() throws Exception {
        try (TestServer server = new TestServer(items(500), items(201))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 1);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(6L));

            sink.notifyCheckpointComplete(6L);

            assertEquals(2, server.requestCount());
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void retriesTransientBulkRequest() throws Exception {
        try (TestServer server = new TestServer(
                new ResponsePlan(503, null, "{\"error\":\"busy\"}"),
                items(201))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 1);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(10L));

            sink.notifyCheckpointComplete(10L);

            assertEquals(2, server.requestCount());
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void permanentItemFailureDoesNotRetryAndRetainsBuffer() throws Exception {
        try (TestServer server = new TestServer(items(400))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 3);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(7L));

            assertThrows(IllegalStateException.class, () -> sink.notifyCheckpointComplete(7L));

            assertEquals(1, server.requestCount());
            assertEquals(1, sink.bufferedRecordCount());
        }
    }

    @Test
    void malformedItemStatusFailsWithoutRetryAndRetainsBuffer() throws Exception {
        try (TestServer server = new TestServer(raw("{\"errors\":true,\"items\":[{\"index\":{}}]}"))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 3);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(8L));

            assertThrows(IllegalStateException.class, () -> sink.notifyCheckpointComplete(8L));

            assertEquals(1, server.requestCount());
            assertEquals(1, sink.bufferedRecordCount());
        }
    }

    @Test
    void itemCountMismatchFailsWithoutRetryAndRetainsBuffer() throws Exception {
        try (TestServer server = new TestServer(raw("{\"errors\":false,\"items\":[]}"))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 3);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(9L));

            assertThrows(IllegalStateException.class, () -> sink.notifyCheckpointComplete(9L));

            assertEquals(1, server.requestCount());
            assertEquals(1, sink.bufferedRecordCount());
        }
    }

    @Test
    void malformedJsonFailsWithoutRetryAndRetainsBuffer() throws Exception {
        try (TestServer server = new TestServer(raw("not-json"))) {
            ElasticsearchSinkFactory.ElasticsearchSinkFunction sink = sink(server, 3);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(11L));

            assertThrows(IllegalStateException.class, () -> sink.notifyCheckpointComplete(11L));

            assertEquals(1, server.requestCount());
            assertEquals(1, sink.bufferedRecordCount());
        }
    }

    private static ElasticsearchSinkFactory.ElasticsearchSinkFunction sink(TestServer server, int maxRetryCount) {
        return new ElasticsearchSinkFactory.ElasticsearchSinkFunction(new ElasticsearchSinkConfig(
                List.of(server.url()),
                "orders",
                "",
                List.of("id"),
                "_",
                List.of("id", "name"),
                100,
                60_000L,
                maxRetryCount,
                ElasticsearchSinkConfig.AuthType.NONE,
                "",
                "",
                "",
                "",
                ""));
    }

    private static DataEntity entity(long id, String name) {
        return new DataEntity(String.valueOf(id), id, Map.of("id", id, "name", name), Map.of());
    }

    private static FunctionSnapshotContext context(long checkpointId) {
        return new FunctionSnapshotContext() {
            @Override
            public long getCheckpointId() {
                return checkpointId;
            }

            @Override
            public long getCheckpointTimestamp() {
                return checkpointId;
            }
        };
    }

    private static ResponsePlan items(int... statuses) {
        return new ResponsePlan(200, statuses, null);
    }

    private static ResponsePlan raw(String body) {
        return new ResponsePlan(200, null, body);
    }

    private record ResponsePlan(int httpStatus, int[] itemStatuses, String body) {
    }

    private static final class TestServer implements AutoCloseable {

        private final HttpServer server;
        private final Deque<ResponsePlan> responses = new ArrayDeque<>();
        private final List<String> requestBodies = new CopyOnWriteArrayList<>();
        private final AtomicInteger requestCount = new AtomicInteger();

        private TestServer(ResponsePlan... responses) throws Exception {
            for (ResponsePlan response : responses) {
                this.responses.add(response);
            }
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/_bulk", exchange -> {
                String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                requestBodies.add(requestBody);
                requestCount.incrementAndGet();
                ResponsePlan response = this.responses.isEmpty() ? items(recordCount(requestBody)) : this.responses.removeFirst();
                String responseBody = response.body() == null ? responseBody(response.itemStatuses()) : response.body();
                byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(response.httpStatus(), bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
        }

        private static int recordCount(String body) {
            return (int) body.lines().filter(line -> !line.isBlank()).count() / 2;
        }

        private static String responseBody(int[] statuses) {
            StringBuilder items = new StringBuilder();
            for (int index = 0; index < statuses.length; index++) {
                if (index > 0) {
                    items.append(',');
                }
                items.append("{\"index\":{\"status\":").append(statuses[index]).append("}}");
            }
            boolean hasErrors = false;
            for (int status : statuses) {
                hasErrors |= status < 200 || status >= 300;
            }
            return "{\"errors\":" + hasErrors + ",\"items\":[" + items + "]}";
        }

        private String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private int requestCount() {
            return requestCount.get();
        }

        private List<String> requestBodies() {
            return requestBodies;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
