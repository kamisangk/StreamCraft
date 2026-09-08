package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

/** Offline collecting sink used by Preview; it never constructs a Kafka producer. */
public class PreviewCollectingSinkFactory implements KafkaSinkAttacher {

    private static final Map<String, Map<String, List<String>>> RECORDS_BY_RUN_ID = new ConcurrentHashMap<>();

    private final String runId = UUID.randomUUID().toString();

    /** Compatibility overload for preview callers that do not need runtime identity. */
    public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
        attach(stream, sinkNode, null, null, false);
    }

    @Override
    public void attach(
            DataStream<DataEntity> stream,
            PipelineNode sinkNode,
            String pipelineId,
            String deploymentNamespace,
            boolean checkpointingEnabled) {
        if (stream == null) {
            throw new IllegalArgumentException("Preview sink input stream is required.");
        }
        if (sinkNode == null) {
            throw new IllegalArgumentException("Preview sink node is required.");
        }

        KafkaSinkConfig config = previewConfig(sinkNode);
        recordsBySinkId().computeIfAbsent(sinkNode.id(), ignored -> Collections.synchronizedList(new ArrayList<>()));
        stream.addSink(new PreviewCapturingSinkFunction(runId, sinkNode.id(), config))
                .name("preview-" + sinkNode.id());
    }

    public PreviewRunResult snapshot() {
        return new PreviewRunResult(recordsBySinkId().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new PreviewRunResult.PreviewOutput(entry.getKey(), List.copyOf(entry.getValue())))
                .toList());
    }

    private KafkaSinkConfig previewConfig(PipelineNode sinkNode) {
        return KafkaSinkConfig.fromPreview(sinkNode.config());
    }

    private Map<String, List<String>> recordsBySinkId() {
        return RECORDS_BY_RUN_ID.computeIfAbsent(runId, ignored -> new ConcurrentHashMap<>());
    }

    private static final class PreviewCapturingSinkFunction extends RichSinkFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String runId;
        private final String sinkId;
        private final KafkaSinkConfig config;
        private transient ObjectMapper objectMapper;

        private PreviewCapturingSinkFunction(String runId, String sinkId, KafkaSinkConfig config) {
            this.runId = runId;
            this.sinkId = sinkId;
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            objectMapper = new ObjectMapper();
        }

        @Override
        public void invoke(DataEntity value, Context context) throws Exception {
            try {
                String payload = KafkaPayloadCodec.encode(config, value, objectMapper);
                RECORDS_BY_RUN_ID.computeIfAbsent(runId, ignored -> new ConcurrentHashMap<>())
                        .computeIfAbsent(sinkId, ignored -> Collections.synchronizedList(new ArrayList<>()))
                        .add(payload);
            } catch (Exception exception) {
                if ("TEXT".equals(config.format())) {
                    return;
                }
                throw exception;
            }
        }
    }
}
