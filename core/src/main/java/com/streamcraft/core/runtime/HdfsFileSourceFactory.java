package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.file.HdfsFileSourceConfig;
import com.streamcraft.shared.file.HdfsFileSourceConfig.ReadMode;
import com.streamcraft.shared.file.HdfsFileSourceConfigParser;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileSourceFactory {

    public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
        HdfsFileSourceConfig config = HdfsFileSourceConfigParser.parse(
                sourceNode.config(),
                IllegalArgumentException::new);
        return env.addSource(new HdfsFileSourceFunction(config))
                .name(sourceNode.name());
    }

    private static final class HdfsFileSourceFunction extends RichParallelSourceFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(HdfsFileSourceFunction.class);

        private final HdfsFileSourceConfig config;
        private volatile boolean running = true;
        private transient ObjectMapper objectMapper;
        private transient Charset charset;
        private transient Pattern fileFilterPattern;

        private HdfsFileSourceFunction(HdfsFileSourceConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            objectMapper = new ObjectMapper();
            charset = Charset.forName(config.encoding());
            fileFilterPattern = config.fileFilterPattern().isBlank()
                    ? null
                    : Pattern.compile(config.fileFilterPattern());
        }

        @Override
        public void run(SourceContext<DataEntity> sourceContext) throws Exception {
            Set<String> consumedFiles = new HashSet<>();
            int polls = 0;
            while (running) {
                List<Path> files = discoverAssignedFiles();
                for (Path file : files) {
                    String fileKey = file.toString();
                    if (config.readMode() == ReadMode.INCREMENTAL && consumedFiles.contains(fileKey)) {
                        continue;
                    }
                    readFile(file, sourceContext);
                    consumedFiles.add(fileKey);
                }

                polls++;
                if (config.readMode() == ReadMode.FULL || (config.maxPolls() > 0 && polls >= config.maxPolls())) {
                    return;
                }
                Thread.sleep(config.pollIntervalMillis());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        private List<Path> discoverAssignedFiles() throws Exception {
            Path root = HdfsFileRuntimeSupport.resolvePath(config.defaultFs(), config.path());
            List<Path> files = new ArrayList<>();
            collectFiles(root, files);
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            return files.stream()
                    .sorted((left, right) -> left.toString().compareTo(right.toString()))
                    .filter(path -> Math.floorMod(path.toString().hashCode(), parallelism) == subtaskIndex)
                    .toList();
        }

        private void collectFiles(Path path, List<Path> files) throws Exception {
            FileSystem fileSystem = path.getFileSystem();
            FileStatus status = fileSystem.getFileStatus(path);
            if (!status.isDir()) {
                if (matchesFilter(path)) {
                    files.add(path);
                }
                return;
            }
            FileStatus[] statuses = fileSystem.listStatus(path);
            if (statuses == null) {
                return;
            }
            for (FileStatus child : statuses) {
                collectFiles(child.getPath(), files);
            }
        }

        private boolean matchesFilter(Path path) {
            if (fileFilterPattern == null) {
                return true;
            }
            String value = path.toString();
            return fileFilterPattern.matcher(value).matches()
                    || fileFilterPattern.matcher(path.getName()).matches();
        }

        private void readFile(Path path, SourceContext<DataEntity> sourceContext) throws Exception {
            FileSystem fileSystem = path.getFileSystem();
            try (FSDataInputStream input = fileSystem.open(path);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(input, charset))) {
                List<String> csvFieldNames = new ArrayList<>(config.fieldNames());
                long lineNumber = 0;
                int skipped = 0;
                String line;
                while (running && (line = reader.readLine()) != null) {
                    lineNumber++;
                    if (line.isBlank()) {
                        continue;
                    }
                    if (skipped < config.skipHeaderRowNumber()) {
                        skipped++;
                        continue;
                    }
                    if (config.fileFormatType() == com.streamcraft.shared.file.HdfsFileFormatType.CSV
                            && config.csvUseHeaderLine()
                            && csvFieldNames.isEmpty()) {
                        csvFieldNames = HdfsFileRuntimeSupport.parseCsvLine(line, config.fieldDelimiter());
                        continue;
                    }
                    try {
                        Map<String, Object> fields = HdfsFileRuntimeSupport.parseRecord(
                                config.fileFormatType(),
                                line,
                                csvFieldNames,
                                config.fieldDelimiter(),
                                objectMapper);
                        if (config.parsePartitionFromPath()) {
                            fields = mergePartitionFields(path, fields);
                        }
                        synchronized (sourceContext.getCheckpointLock()) {
                            sourceContext.collect(toEntity(path, lineNumber, fields));
                        }
                    } catch (Exception exception) {
                        LOG.warn("Skipping HDFS File Source record from '{}' line {} because {}",
                                path, lineNumber, exception.getMessage());
                    }
                }
            }
        }

        private Map<String, Object> mergePartitionFields(Path path, Map<String, Object> fields) {
            Map<String, Object> merged = new LinkedHashMap<>(parsePartitionFields(path));
            merged.putAll(fields);
            return merged;
        }

        private Map<String, Object> parsePartitionFields(Path path) {
            Map<String, Object> partitions = new LinkedHashMap<>();
            String value = path.toString().replace('\\', '/');
            int fileNameIndex = value.lastIndexOf('/');
            String parentPath = fileNameIndex < 0 ? "" : value.substring(0, fileNameIndex);
            for (String segment : parentPath.split("/")) {
                int separator = segment.indexOf('=');
                if (separator <= 0 || separator == segment.length() - 1) {
                    continue;
                }
                partitions.put(segment.substring(0, separator), segment.substring(separator + 1));
            }
            return partitions;
        }

        private DataEntity toEntity(Path path, long lineNumber, Map<String, Object> fields) {
            String id = resolveId(path, lineNumber, fields);
            long timestamp = resolveTimestamp(fields);
            Map<String, String> headers = new HashMap<>();
            headers.put("source", "hdfs-file");
            headers.put("path", path.toString());
            headers.put("line", String.valueOf(lineNumber));
            headers.put("format", config.fileFormatType().name());
            return new DataEntity(id, timestamp, fields, headers);
        }

        private String resolveId(Path path, long lineNumber, Map<String, Object> fields) {
            if (!config.idField().isBlank()) {
                Object value = fields.get(config.idField());
                if (value != null) {
                    return String.valueOf(value);
                }
            }
            return path + ":" + lineNumber + ":" + UUID.randomUUID();
        }

        private long resolveTimestamp(Map<String, Object> fields) {
            if (!config.timestampField().isBlank()) {
                Object value = fields.get(config.timestampField());
                if (value instanceof Number number) {
                    return number.longValue();
                }
                if (value != null) {
                    try {
                        return Long.parseLong(String.valueOf(value));
                    } catch (NumberFormatException ignored) {
                        return System.currentTimeMillis();
                    }
                }
            }
            return System.currentTimeMillis();
        }
    }
}
