package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.jdbc.JdbcSourceConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.lang.reflect.Proxy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.junit.jupiter.api.Test;

class JdbcSourceNativeExecutionTest {

    @Test
    void directReaderEmitsRows() throws Exception {
        String url = "jdbc:h2:mem:reader_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        try (Connection connection = DriverManager.getConnection(url); Statement statement = connection.createStatement()) {
            statement.execute("create table events (id bigint primary key, payload varchar(32))");
            statement.execute("insert into events values (1, 'a'), (2, 'b')");
        }
        JdbcSourceConfig config = new JdbcSourceConfig(
                url, "org.h2.Driver", "", "", "select id, payload from events", "",
                JdbcSourceConfig.ReadMode.INCREMENTAL, "ID", JdbcSourceConfig.CursorType.LONG, "1", 1, 100, 1,
                "ID", "");
        JdbcSourceReader reader = new JdbcSourceReader((SourceReaderContext) Proxy.newProxyInstance(
                SourceReaderContext.class.getClassLoader(), new Class<?>[] {SourceReaderContext.class},
                (ignored, method, args) -> null), config);
        reader.start();
        reader.addSplits(List.of(new JdbcSourceSplit("jdbc", new IncrementalSourceState("LONG", "1", "ID", null))));
        List<DataEntity> output = new ArrayList<>();
        ReaderOutput<DataEntity> sink = (ReaderOutput<DataEntity>) Proxy.newProxyInstance(
                ReaderOutput.class.getClassLoader(), new Class<?>[] {ReaderOutput.class},
                (ignored, method, args) -> { if (method.getName().equals("collect")) output.add((DataEntity) args[0]); return null; });
        reader.pollNext(sink);
        reader.pollNext(sink);
        assertEquals(List.of(2L), output.stream().map(value -> value.fields().get("ID")).toList());
    }

    @Test
    void readsFullAndCompositeIncrementalSource() throws Exception {
        String url = "jdbc:h2:mem:source_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        try (Connection connection = DriverManager.getConnection(url); Statement statement = connection.createStatement()) {
            statement.execute("create table events (id bigint primary key, payload varchar(32))");
            statement.execute("insert into events values (1, 'a'), (2, 'b'), (3, 'c')");
        }
        JdbcSourceConfig config = new JdbcSourceConfig(
                url, "org.h2.Driver", "", "", "select id, payload from events", "",
                JdbcSourceConfig.ReadMode.INCREMENTAL, "ID", JdbcSourceConfig.CursorType.LONG, "1", 1, 100, 1,
                "ID", "");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<DataEntity> values = Collections.synchronizedList(new ArrayList<>());
        env.fromSource(new JdbcSource(config), WatermarkStrategy.noWatermarks(), "jdbc")
                .addSink(new RichSinkFunction<>() {
                    @Override
                    public void invoke(DataEntity value, Context context) {
                        values.add(value);
                    }
                });
        env.execute("jdbc-source");
        assertEquals(List.of(2L, 3L), values.stream().map(value -> value.fields().get("ID")).toList());
    }
}
