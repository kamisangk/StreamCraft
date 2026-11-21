package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig;
import com.sun.net.httpserver.HttpServer;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class InfluxDbSourceCursorPrecisionTest {

    @ParameterizedTest
    @CsvSource({
        "n, 1000000001, 1000000101, 1000000199, 1970-01-01T00:00:01.000000001Z, 1970-01-01T00:00:01.000000101Z",
        "u, 1000000, 1000001, 1000009, 1970-01-01T00:00:01Z, 1970-01-01T00:00:01.000001Z",
        "n, -1001, -1000, -1, 1969-12-31T23:59:59.999998999Z, 1969-12-31T23:59:59.999999Z"
    })
    void preservesTimestampOrderAndExactCursorAcrossCheckpointRestore(
            String epoch, String initialCursor, long earlier, long later, String initialInstant, String earlierInstant)
            throws Exception {
        AtomicReference<String> query = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/query", exchange -> {
            try (exchange) {
                query.set(URLDecoder.decode(exchange.getRequestURI().getRawQuery(), StandardCharsets.UTF_8));
                byte[] response = """
                        {"results":[{"series":[{"name":"cpu","columns":["time","host"],
                        "values":[[%d,"a"],[%d,"z"]]}]}]}
                        """.formatted(later, earlier).getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
        });
        server.start();
        try {
            InfluxDbSourceConfig config = new InfluxDbSourceConfig(
                    "http://127.0.0.1:" + server.getAddress().getPort(), "metrics", "select * from cpu", "{}", epoch,
                    5, 1000, InfluxDbSourceConfig.ReadMode.INCREMENTAL, "time",
                    InfluxDbSourceConfig.CursorType.TIMESTAMP, initialCursor, 1, 1, "host", "time", "", "");
            InfluxDbSourceSplit initialSplit = new InfluxDbSourceSplit(
                    "influx", new IncrementalSourceState("TIMESTAMP", "time", initialCursor, "host", null), 0);
            InfluxDbSourceReader reader = reader(config, initialSplit);
            List<DataEntity> emitted = new ArrayList<>();
            ReaderOutput<DataEntity> output = output(emitted);

            reader.pollNext(output);
            assertEquals("z", emitted.get(0).id());
            assertTrue(query.get().contains("time > '" + initialInstant + "'"), query::get);
            InfluxDbSourceSplit checkpoint = reader.snapshotState(1).get(0);
            assertEquals(earlierInstant, checkpoint.state().cursorValue());
            assertEquals("z", checkpoint.state().uniqueIdValue());
            reader.pollNext(output);
            assertEquals(List.of("z", "a"), emitted.stream().map(DataEntity::id).toList());

            InfluxDbSourceSplitSerializer serializer = new InfluxDbSourceSplitSerializer();
            InfluxDbSourceSplit restored = serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));
            reader(config, restored).pollNext(output(new ArrayList<>()));
            assertTrue(query.get().contains("time > '" + earlierInstant + "' or (time = '"
                    + earlierInstant + "' and host > 'z')"), query::get);
        } finally {
            server.stop(0);
        }
    }

    private static InfluxDbSourceReader reader(InfluxDbSourceConfig config, InfluxDbSourceSplit split) {
        SourceReaderContext context = (SourceReaderContext) Proxy.newProxyInstance(
                SourceReaderContext.class.getClassLoader(), new Class<?>[] {SourceReaderContext.class},
                (proxy, method, args) -> null);
        InfluxDbSourceReader reader = new InfluxDbSourceReader(context, config);
        reader.addSplits(List.of(split));
        reader.start();
        return reader;
    }

    @SuppressWarnings("unchecked")
    private static ReaderOutput<DataEntity> output(List<DataEntity> records) {
        return (ReaderOutput<DataEntity>) Proxy.newProxyInstance(
                ReaderOutput.class.getClassLoader(), new Class<?>[] {ReaderOutput.class}, (proxy, method, args) -> {
                    if ("collect".equals(method.getName())) {
                        records.add((DataEntity) args[0]);
                    }
                    return null;
                });
    }
}
