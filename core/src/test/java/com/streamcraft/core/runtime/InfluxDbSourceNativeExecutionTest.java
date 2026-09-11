package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig;
import com.sun.net.httpserver.HttpServer;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.junit.jupiter.api.Test;

class InfluxDbSourceNativeExecutionTest {

    @Test
    void emitsRowsAndPersistsInfluxTimestampCursor() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/query", exchange -> {
            byte[] response = "{\"results\":[{\"series\":[{\"name\":\"cpu\",\"tags\":{\"host\":\"a\"},\"columns\":[\"time\",\"value\"],\"values\":[[1000,1.0]]}]}]}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            InfluxDbSourceConfig config = new InfluxDbSourceConfig(
                    "http://127.0.0.1:" + server.getAddress().getPort(), "metrics", "select * from cpu", "{}", "ms",
                    30, 1000, InfluxDbSourceConfig.ReadMode.INCREMENTAL, "time",
                    InfluxDbSourceConfig.CursorType.TIMESTAMP, "1970-01-01T00:00:00Z", 1, 1,
                    "host", "time", "", "");
            InfluxDbSourceReader reader = new InfluxDbSourceReader(context(), config);
            reader.start();
            reader.addSplits(List.of(new InfluxDbSourceSplit("influx", new IncrementalSourceState(
                    "TIMESTAMP", "1970-01-01T00:00:00Z", "host", null), 0)));
            List<DataEntity> output = new ArrayList<>();
            reader.pollNext(output(output));
            assertEquals(1, output.size());
            assertEquals("1970-01-01T00:00:01Z", reader.snapshotState(1).get(0).state().cursorValue());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void ordersEqualCursorsByConfiguredIdFieldAcrossSeries() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/query", exchange -> {
            byte[] response = ("{\"results\":[{\"series\":["
                    + "{\"name\":\"cpu\",\"tags\":{\"host\":\"b\"},"
                    + "\"columns\":[\"time\",\"value\"],\"values\":[[1000,2.0]]},"
                    + "{\"name\":\"cpu\",\"tags\":{\"host\":\"a\"},"
                    + "\"columns\":[\"time\",\"value\"],\"values\":[[1000,1.0]]}]}]}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            InfluxDbSourceConfig config = new InfluxDbSourceConfig(
                    "http://127.0.0.1:" + server.getAddress().getPort(), "metrics", "select * from cpu", "{}", "ms",
                    30, 1000, InfluxDbSourceConfig.ReadMode.INCREMENTAL, "time",
                    InfluxDbSourceConfig.CursorType.TIMESTAMP, "1970-01-01T00:00:00Z", 1, 1,
                    "host", "time", "", "");
            InfluxDbSourceReader reader = new InfluxDbSourceReader(context(), config);
            reader.start();
            reader.addSplits(List.of(new InfluxDbSourceSplit("influx", new IncrementalSourceState(
                    "TIMESTAMP", "1970-01-01T00:00:00Z", "host", null), 0)));
            List<DataEntity> output = new ArrayList<>();
            ReaderOutput<DataEntity> sink = output(output);
            reader.pollNext(sink);
            reader.pollNext(sink);
            assertEquals(List.of("a", "b"), output.stream().map(DataEntity::id).toList());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsCheckpointWithDifferentCursorType() {
        InfluxDbSourceConfig config = new InfluxDbSourceConfig(
                "http://127.0.0.1:8086", "metrics", "select * from cpu", "{}", "ms",
                30, 1000, InfluxDbSourceConfig.ReadMode.INCREMENTAL, "time",
                InfluxDbSourceConfig.CursorType.TIMESTAMP, "", 1, 1,
                "host", "time", "", "");
        InfluxDbSourceReader reader = new InfluxDbSourceReader(context(), config);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> reader.addSplits(List.of(new InfluxDbSourceSplit(
                        "influx", new IncrementalSourceState("LONG", "1000", "host", "a"), 0))));

        assertEquals(
                "InfluxDB Source checkpoint state version 1 cursorType changed from LONG to TIMESTAMP.",
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
                    if (method.getName().equals("collect")) values.add((DataEntity) args[0]);
                    return null;
                });
    }
}
