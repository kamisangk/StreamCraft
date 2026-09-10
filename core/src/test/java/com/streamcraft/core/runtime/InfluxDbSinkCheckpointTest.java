package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.influxdb.InfluxDbSinkConfig;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.junit.jupiter.api.Test;

class InfluxDbSinkCheckpointTest {

    @Test
    void lowVolumeRecordsFlushWhenCheckpointCompletes() throws Exception {
        try (TestServer server = new TestServer(204)) {
            InfluxDbSinkFactory.InfluxDbSinkFunction sink = sink(server, 0);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(1L));

            assertEquals(0, server.requestCount());
            sink.notifyCheckpointComplete(1L);

            assertEquals(1, server.requestCount());
            assertEquals(0, sink.bufferedRecordCount());
            assertTrue(server.bodies().get(0).contains("Ada"));
        }
    }

    @Test
    void recordsArrivingAfterSnapshotWaitForALaterCheckpoint() throws Exception {
        try (TestServer server = new TestServer(204)) {
            InfluxDbSinkFactory.InfluxDbSinkFunction sink = sink(server, 0);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(2L));
            sink.invoke(entity(2L, "Grace"), null);

            sink.notifyCheckpointComplete(2L);

            assertEquals(1, server.requestCount());
            assertTrue(server.bodies().get(0).contains("Ada"));
            assertTrue(!server.bodies().get(0).contains("Grace"));
            assertEquals(1, sink.bufferedRecordCount());

            sink.snapshotState(context(3L));
            sink.notifyCheckpointComplete(3L);
            assertEquals(2, server.requestCount());
            assertTrue(server.bodies().get(1).contains("Grace"));
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void abortedCheckpointDoesNotFlushItsSnapshot() throws Exception {
        try (TestServer server = new TestServer(204)) {
            InfluxDbSinkFactory.InfluxDbSinkFunction sink = sink(server, 0);
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
    void retriesRateLimitedWriteAndThenFlushesTheCheckpoint() throws Exception {
        try (TestServer server = new TestServer(429, 204)) {
            InfluxDbSinkFactory.InfluxDbSinkFunction sink = sink(server, 1);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(5L));

            sink.notifyCheckpointComplete(5L);

            assertEquals(2, server.requestCount());
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void retriesServerErrorWriteAndThenFlushesTheCheckpoint() throws Exception {
        try (TestServer server = new TestServer(500, 204)) {
            InfluxDbSinkFactory.InfluxDbSinkFunction sink = sink(server, 1);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(7L));

            sink.notifyCheckpointComplete(7L);

            assertEquals(2, server.requestCount());
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void doesNotRetryPermanentHttpFailureAndRetainsBuffer() throws Exception {
        try (TestServer server = new TestServer(400)) {
            InfluxDbSinkFactory.InfluxDbSinkFunction sink = sink(server, 3);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(6L));

            assertThrows(IllegalStateException.class, () -> sink.notifyCheckpointComplete(6L));

            assertEquals(1, server.requestCount());
            assertEquals(1, sink.bufferedRecordCount());
        }
    }

    private static InfluxDbSinkFactory.InfluxDbSinkFunction sink(TestServer server, int maxRetries) {
        return new InfluxDbSinkFactory.InfluxDbSinkFunction(new InfluxDbSinkConfig(
                server.url(),
                "db",
                "orders",
                "time",
                List.of(),
                List.of("name"),
                100,
                maxRetries,
                1L,
                1L,
                1_000,
                60_000L,
                "ms",
                "",
                ""));
    }

    private static DataEntity entity(long id, String name) {
        return new DataEntity(String.valueOf(id), id, Map.of("name", name), Map.of());
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

    private static final class TestServer implements AutoCloseable {
        private final HttpServer server;
        private final Deque<Integer> statuses = new ArrayDeque<>();
        private final List<String> bodies = new ArrayList<>();
        private final AtomicInteger requestCount = new AtomicInteger();

        private TestServer(int... statuses) throws Exception {
            for (int status : statuses) {
                this.statuses.add(status);
            }
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/write", exchange -> {
                bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                requestCount.incrementAndGet();
                int status = this.statuses.isEmpty() ? 204 : this.statuses.removeFirst();
                exchange.sendResponseHeaders(status, -1);
                exchange.close();
            });
            server.start();
        }

        private String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private int requestCount() {
            return requestCount.get();
        }

        private List<String> bodies() {
            return bodies;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
