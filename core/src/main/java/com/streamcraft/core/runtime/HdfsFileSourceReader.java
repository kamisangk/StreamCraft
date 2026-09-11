package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.file.HdfsFileFormatType;
import com.streamcraft.shared.file.HdfsFileSourceConfig;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HdfsFileSourceReader implements SourceReader<DataEntity, HdfsFileSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsFileSourceReader.class);
    private final SourceReaderContext context;
    private final HdfsFileSourceConfig config;
    private final Object lock = new Object();
    private final ArrayDeque<HdfsRecord> records = new ArrayDeque<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private HdfsFileSourceSplit split;
    private boolean started;
    private boolean exhausted;
    private boolean batchFullyRead;
    private long nextPoll;

    HdfsFileSourceReader(SourceReaderContext context, HdfsFileSourceConfig config) {
        this.context = context;
        this.config = config;
    }

    @Override
    public void start() {
        started = true;
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<DataEntity> output) throws Exception {
        synchronized (lock) {
            if (!started || split == null) return InputStatus.NOTHING_AVAILABLE;
            if (!records.isEmpty()) {
                emit(output);
                return records.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
            }
            if (exhausted) return InputStatus.END_OF_INPUT;
            if (System.currentTimeMillis() < nextPoll) return InputStatus.NOTHING_AVAILABLE;
            read();
            if (!records.isEmpty()) {
                emit(output);
                return records.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
            }
            if (config.readMode() == HdfsFileSourceConfig.ReadMode.FULL) {
                exhausted = true;
                return InputStatus.END_OF_INPUT;
            }
            nextPoll = System.currentTimeMillis() + config.pollIntervalMillis();
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    @Override
    public List<HdfsFileSourceSplit> snapshotState(long checkpointId) {
        synchronized (lock) {
            return split == null ? List.of() : List.of(split);
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<HdfsFileSourceSplit> splits) {
        synchronized (lock) {
            if (!splits.isEmpty()) split = splits.get(0);
            exhausted = false;
        }
    }

    @Override public void notifyNoMoreSplits() { }
    @Override public void close() { exhausted = true; }

    private void read() throws Exception {
        batchFullyRead = false;
        HdfsFileSourceSplit current = split;
        Path path = new Path(current.state().path());
        FileStatus status = path.getFileSystem().getFileStatus(path);
        String fingerprint = path + ":" + status.getLen() + ":" + status.getModificationTime();
        if (!fingerprint.equals(current.state().fingerprint())) {
            LOG.warn("Skipping changed HDFS file {}", path);
            return;
        }
        long lineNumber = current.state().lineNumber();
        boolean headerSeen = current.state().headerSeen();
        List<String> fieldNames = new ArrayList<>(config.fieldNames());
        try (FSDataInputStream input = path.getFileSystem().open(path)) {
            if (current.state().byteOffset() > 0) input.seek(current.state().byteOffset());
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(input, Charset.forName(config.encoding())))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    if (line.isBlank() || lineNumber <= config.skipHeaderRowNumber()) continue;
                    if (config.fileFormatType() == HdfsFileFormatType.CSV
                            && config.csvUseHeaderLine() && fieldNames.isEmpty()) {
                        fieldNames = HdfsFileRuntimeSupport.parseCsvLine(line, config.fieldDelimiter());
                        headerSeen = true;
                        continue;
                    }
                    try {
                        Map<String, Object> fields = HdfsFileRuntimeSupport.parseRecord(
                                config.fileFormatType(), line, fieldNames, config.fieldDelimiter(), objectMapper);
                        if (config.parsePartitionFromPath()) {
                            Map<String, Object> merged = new LinkedHashMap<>(partitionFields(path));
                            merged.putAll(fields);
                            fields = merged;
                        }
                        records.add(new HdfsRecord(toEntity(path, lineNumber, fields), lineNumber));
                    } catch (Exception exception) {
                        LOG.warn("Skipping HDFS line {}: {}", lineNumber, exception.getMessage());
                    }
                }
            }
        }
        // The file is marked complete only after every record has been emitted.
        // Until then snapshots retain the previous offset and Flink can replay.
        if (records.isEmpty()) {
            split = new HdfsFileSourceSplit(current.splitId(), new HdfsFileSourceState(
                    path.toString(), fingerprint, status.getLen(), lineNumber,
                    config.fileFormatType().name(), headerSeen, true));
        }
        batchFullyRead = true;
    }

    private void emit(ReaderOutput<DataEntity> output) {
        HdfsRecord record = records.removeFirst();
        output.collect(record.entity());
        HdfsFileSourceState state = split.state();
        split = new HdfsFileSourceSplit(split.splitId(), new HdfsFileSourceState(
                state.path(), state.fingerprint(), state.byteOffset(), record.lineNumber(),
                state.format(), state.headerSeen(), records.isEmpty() && batchFullyRead));
    }

    private DataEntity toEntity(Path path, long line, Map<String, Object> fields) {
        Object configuredId = config.idField().isBlank() ? null : fields.get(config.idField());
        String id = configuredId == null ? path + ":" + line : String.valueOf(configuredId);
        Object timestamp = config.timestampField().isBlank() ? null : fields.get(config.timestampField());
        long time = timestamp instanceof Number number ? number.longValue() : System.currentTimeMillis();
        Map<String, String> headers = new HashMap<>();
        headers.put("source", "hdfs-file"); headers.put("path", path.toString());
        headers.put("line", String.valueOf(line)); headers.put("format", config.fileFormatType().name());
        return new DataEntity(id, time, fields, headers);
    }

    private Map<String, Object> partitionFields(Path path) {
        Map<String, Object> partitions = new LinkedHashMap<>();
        String value = path.toString().replace('\\', '/');
        int index = value.lastIndexOf('/');
        if (index > 0) for (String segment : value.substring(0, index).split("/")) {
            int separator = segment.indexOf('=');
            if (separator > 0 && separator < segment.length() - 1) {
                partitions.put(segment.substring(0, separator), segment.substring(separator + 1));
            }
        }
        return partitions;
    }

    private record HdfsRecord(DataEntity entity, long lineNumber) { }
}
