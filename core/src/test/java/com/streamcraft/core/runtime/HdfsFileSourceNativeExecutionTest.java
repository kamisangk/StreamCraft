package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.file.HdfsFileFormatType;
import com.streamcraft.shared.file.HdfsFileSourceConfig;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HdfsFileSourceNativeExecutionTest {

    @TempDir Path temp;

    @Test
    void persistsStableFileProgressAndCompletedFlag() throws Exception {
        Path file = temp.resolve("orders.json");
        Files.writeString(file, "{\"id\":\"o-1\"}\n{\"id\":\"o-2\"}\n");
        HdfsFileSourceConfig config = new HdfsFileSourceConfig(
                "file:///", file.toUri().toString(), HdfsFileFormatType.JSON, "{}", List.of(), "\001", "\n", 0,
                false, "UTF-8", "none", false, "", HdfsFileSourceConfig.ReadMode.FULL, 1, 1, "id", "", "", "", "");
        org.apache.flink.core.fs.Path flinkPath = new org.apache.flink.core.fs.Path(file.toUri().toString());
        org.apache.flink.core.fs.FileStatus status = flinkPath.getFileSystem().getFileStatus(flinkPath);
        String fingerprint = flinkPath + ":" + status.getLen() + ":" + status.getModificationTime();
        HdfsFileSourceReader reader = new HdfsFileSourceReader(context(), config);
        reader.start();
        reader.addSplits(List.of(new HdfsFileSourceSplit("file", new HdfsFileSourceState(
                flinkPath.toString(), fingerprint, 0, 0, "JSON", false, false))));
        List<DataEntity> output = new ArrayList<>();
        ReaderOutput<DataEntity> sink = output(output);
        reader.pollNext(sink); reader.pollNext(sink);
        assertEquals(List.of("o-1", "o-2"), output.stream().map(DataEntity::id).toList());
        assertTrue(reader.snapshotState(1).get(0).state().completed());
        assertEquals(2, reader.snapshotState(1).get(0).state().lineNumber());
    }

    @Test
    void consumesAllSplitsAssignedToOneReader() throws Exception {
        Path first = temp.resolve("first.json");
        Path second = temp.resolve("second.json");
        Files.writeString(first, "{\"id\":\"o-1\"}\n");
        Files.writeString(second, "{\"id\":\"o-2\"}\n");
        HdfsFileSourceConfig config = new HdfsFileSourceConfig(
                "file:///", first.getParent().toUri().toString(), HdfsFileFormatType.JSON, "{}", List.of(), "\001", "\n", 0,
                false, "UTF-8", "none", false, "", HdfsFileSourceConfig.ReadMode.FULL, 1, 1, "id", "", "", "", "");
        org.apache.flink.core.fs.Path firstPath = new org.apache.flink.core.fs.Path(first.toUri().toString());
        org.apache.flink.core.fs.Path secondPath = new org.apache.flink.core.fs.Path(second.toUri().toString());
        HdfsFileSourceReader reader = new HdfsFileSourceReader(context(), config);
        reader.start();
        reader.addSplits(List.of(
                new HdfsFileSourceSplit("first", new HdfsFileSourceState(firstPath.toString(), fingerprint(firstPath), 0, 0, "JSON", false, false)),
                new HdfsFileSourceSplit("second", new HdfsFileSourceState(secondPath.toString(), fingerprint(secondPath), 0, 0, "JSON", false, false))));
        List<DataEntity> output = new ArrayList<>();
        ReaderOutput<DataEntity> sink = output(output);
        for (int i = 0; i < 8 && output.size() < 2; i++) {
            reader.pollNext(sink);
        }
        assertEquals(List.of("o-1", "o-2"), output.stream().map(DataEntity::id).toList());
    }

    @Test
    void retiresChangedFingerprintSoANewStableVersionCanBeDiscovered() throws Exception {
        Path file = temp.resolve("changing.json");
        Files.writeString(file, "{\"id\":\"old\"}\n");
        org.apache.flink.core.fs.Path flinkPath = new org.apache.flink.core.fs.Path(file.toUri().toString());
        String originalFingerprint = fingerprint(flinkPath);
        Files.writeString(file, "{\"id\":\"new-longer-value\"}\n");
        HdfsFileSourceConfig config = new HdfsFileSourceConfig(
                "file:///", file.toUri().toString(), HdfsFileFormatType.JSON, "{}", List.of(), "\001", "\n", 0,
                false, "UTF-8", "none", false, "", HdfsFileSourceConfig.ReadMode.INCREMENTAL, 1, 0,
                "id", "", "", "", "");
        HdfsFileSourceReader reader = new HdfsFileSourceReader(context(), config);
        reader.start();
        reader.addSplits(List.of(new HdfsFileSourceSplit("old", new HdfsFileSourceState(
                flinkPath.toString(), originalFingerprint, 0, 0, "JSON", false, false))));

        reader.pollNext(output(new ArrayList<>()));

        assertTrue(reader.snapshotState(1).get(0).state().completed());
    }

    @Test
    void restoresCsvHeaderFieldsFromANonZeroOffset() throws Exception {
        Path file = temp.resolve("orders.csv");
        Files.writeString(file, "id,value\n1,a\n2,b\n");
        org.apache.flink.core.fs.Path flinkPath = new org.apache.flink.core.fs.Path(file.toUri().toString());
        long offset = "id,value\n1,a\n".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        HdfsFileSourceConfig config = new HdfsFileSourceConfig(
                "file:///", file.toUri().toString(), HdfsFileFormatType.CSV, "{}", List.of(), ",", "\n", 0,
                true, "UTF-8", "none", false, "", HdfsFileSourceConfig.ReadMode.FULL, 1, 1,
                "id", "", "", "", "");
        HdfsFileSourceReader reader = new HdfsFileSourceReader(context(), config);
        reader.start();
        reader.addSplits(List.of(new HdfsFileSourceSplit("csv", new HdfsFileSourceState(
                flinkPath.toString(), fingerprint(flinkPath), offset, 2, "CSV", true, false))));
        List<DataEntity> output = new ArrayList<>();

        reader.pollNext(output(output));

        assertEquals(List.of("2"), output.stream().map(DataEntity::id).toList());
        assertEquals("b", output.get(0).fields().get("value"));
    }

    @Test
    void boundedEnumeratorSignalsReadersWhenDiscoveryIsComplete() throws Exception {
        Path file = temp.resolve("only.json");
        Files.writeString(file, "{\"id\":\"o-1\"}\n");
        HdfsFileSourceConfig config = new HdfsFileSourceConfig(
                "file:///", file.toUri().toString(), HdfsFileFormatType.JSON, "{}", List.of(), "\001", "\n", 0,
                false, "UTF-8", "none", false, "", HdfsFileSourceConfig.ReadMode.FULL, 1, 1,
                "id", "", "", "", "");
        List<SplitsAssignment<HdfsFileSourceSplit>> assignments = new ArrayList<>();
        List<Integer> noMoreSplits = new ArrayList<>();
        SplitEnumeratorContext<HdfsFileSourceSplit> context = enumeratorContext(assignments, noMoreSplits);
        HdfsFileEnumerator enumerator = new HdfsFileEnumerator(
                context, config, new HdfsFileEnumeratorState(List.of(), java.util.Set.of()));
        enumerator.addReader(0);
        enumerator.addReader(1);

        enumerator.start();

        assertEquals(1, assignments.stream()
                .flatMap(assignment -> assignment.assignment().values().stream())
                .mapToInt(List::size)
                .sum());
        assertEquals(List.of(0, 1), noMoreSplits.stream().sorted().toList());
    }

    private String fingerprint(org.apache.flink.core.fs.Path path) throws Exception {
        org.apache.flink.core.fs.FileStatus status = path.getFileSystem().getFileStatus(path);
        return path + ":" + status.getLen() + ":" + status.getModificationTime();
    }

    @SuppressWarnings("unchecked")
    private SplitEnumeratorContext<HdfsFileSourceSplit> enumeratorContext(
            List<SplitsAssignment<HdfsFileSourceSplit>> assignments,
            List<Integer> noMoreSplits) {
        Map<Integer, ReaderInfo> readers = Map.of(
                0, new ReaderInfo(0, "reader-0"),
                1, new ReaderInfo(1, "reader-1"));
        return (SplitEnumeratorContext<HdfsFileSourceSplit>) Proxy.newProxyInstance(
                SplitEnumeratorContext.class.getClassLoader(),
                new Class<?>[] {SplitEnumeratorContext.class},
                (ignored, method, args) -> switch (method.getName()) {
                    case "registeredReaders" -> readers;
                    case "currentParallelism" -> 2;
                    case "assignSplits" -> {
                        assignments.add((SplitsAssignment<HdfsFileSourceSplit>) args[0]);
                        yield null;
                    }
                    case "signalNoMoreSplits" -> {
                        noMoreSplits.add((Integer) args[0]);
                        yield null;
                    }
                    default -> null;
                });
    }

    private SourceReaderContext context() { return (SourceReaderContext) Proxy.newProxyInstance(
            SourceReaderContext.class.getClassLoader(), new Class<?>[] {SourceReaderContext.class}, (o,m,a)->null); }
    @SuppressWarnings("unchecked") private ReaderOutput<DataEntity> output(List<DataEntity> values) { return (ReaderOutput<DataEntity>) Proxy.newProxyInstance(
            ReaderOutput.class.getClassLoader(), new Class<?>[] {ReaderOutput.class}, (o,m,a)->{if(m.getName().equals("collect"))values.add((DataEntity)a[0]);return null;}); }
}
