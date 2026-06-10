package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
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

public class PreviewCollectingSinkFactory extends KafkaSinkFactory {

    private static final Map<String, Map<String, List<String>>> RECORDS_BY_RUN_ID = new ConcurrentHashMap<>();

    private final String runId = UUID.randomUUID().toString();

    @Override
    public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
        JsonNode config = sinkNode.config();
        recordsBySinkId().computeIfAbsent(sinkNode.id(), ignored -> Collections.synchronizedList(new ArrayList<>()));
        stream.addSink(new PreviewCapturingSinkFunction(
                        runId,
                        sinkNode.id(),
                        config.path("topic").asText("preview"),
                        config.path("format").asText("JSON"),
                        config.path("messageField").asText("_streamcraft_message")))
                .name("preview-" + sinkNode.id());
    }

    public PreviewRunResult snapshot() {
        return new PreviewRunResult(recordsBySinkId().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new PreviewRunResult.PreviewOutput(entry.getKey(), List.copyOf(entry.getValue())))
                .toList());
    }

    private Map<String, List<String>> recordsBySinkId() {
        return RECORDS_BY_RUN_ID.computeIfAbsent(runId, ignored -> new ConcurrentHashMap<>());
    }

    private static final class PreviewCapturingSinkFunction extends RichSinkFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String runId;
        private final String sinkId;
        private final String topic;
        private final String format;
        private final String messageField;
        private transient ObjectMapper objectMapper;

        private PreviewCapturingSinkFunction(
                String runId,
                String sinkId,
                String topic,
                String format,
                String messageField) {
            this.runId = runId;
            this.sinkId = sinkId;
            this.topic = topic;
            this.format = format;
            this.messageField = messageField;
        }

        @Override
        public void open(OpenContext openContext) {
            objectMapper = new ObjectMapper();
        }

        @Override
        public void invoke(DataEntity value, Context context) throws Exception {
            String payload = buildPayloadOrSkip(format, messageField, topic, value, objectMapper, message -> {
            });
            if (payload == null) {
                return;
            }

            RECORDS_BY_RUN_ID.computeIfAbsent(runId, ignored -> new ConcurrentHashMap<>())
                    .computeIfAbsent(sinkId, ignored -> Collections.synchronizedList(new ArrayList<>()))
                    .add(payload);
        }
    }
}
