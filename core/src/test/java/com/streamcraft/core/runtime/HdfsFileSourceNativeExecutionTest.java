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
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
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

    private SourceReaderContext context() { return (SourceReaderContext) Proxy.newProxyInstance(
            SourceReaderContext.class.getClassLoader(), new Class<?>[] {SourceReaderContext.class}, (o,m,a)->null); }
    @SuppressWarnings("unchecked") private ReaderOutput<DataEntity> output(List<DataEntity> values) { return (ReaderOutput<DataEntity>) Proxy.newProxyInstance(
            ReaderOutput.class.getClassLoader(), new Class<?>[] {ReaderOutput.class}, (o,m,a)->{if(m.getName().equals("collect"))values.add((DataEntity)a[0]);return null;}); }
}
