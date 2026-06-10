package com.streamcraft.core.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceFactory.class);

    public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
        JsonNode config = sourceNode.config();
        List<String> topics = new ArrayList<>();
        for (JsonNode topic : config.path("topics")) {
            topics.add(topic.asText());
        }

        return env.addSource(new LegacyKafkaSourceFunction(
                        config.path("bootstrapServers").asText(),
                        topics,
                        config.path("groupId").asText(),
                        config.path("consumeMode").asText(),
                        config.path("format").asText("JSON"),
                        config))
                .name(sourceNode.name());
    }

    static Map<String, Object> decodeFields(String format, String payload, ObjectMapper objectMapper) throws IOException {
        if ("TEXT".equals(format)) {
            return new HashMap<>(Map.of("_streamcraft_message", payload == null ? "" : payload));
        }

        JsonNode json = objectMapper.readTree(payload == null ? "null" : payload);
        if (json == null || !json.isObject()) {
            throw new IllegalArgumentException("Kafka Source JSON payload must be a JSON object.");
        }
        return objectMapper.convertValue(json, new TypeReference<Map<String, Object>>() {});
    }

    private static final class LegacyKafkaSourceFunction extends RichParallelSourceFunction<DataEntity> {

        private static final long serialVersionUID = 1L;

        private final String bootstrapServers;
        private final List<String> topics;
        private final String groupId;
        private final String consumeMode;
        private final String format;
        private final JsonNode config;
        private volatile boolean running = true;
        private transient KafkaConsumer<String, String> consumer;
        private transient ObjectMapper objectMapper;

        private LegacyKafkaSourceFunction(
                String bootstrapServers,
                List<String> topics,
                String groupId,
                String consumeMode,
                String format,
                JsonNode config) {
            this.bootstrapServers = bootstrapServers;
            this.topics = List.copyOf(topics);
            this.groupId = groupId;
            this.consumeMode = consumeMode;
            this.format = format;
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resolveAutoOffsetReset(consumeMode));
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            KafkaAuthProperties.apply(config, properties);
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(topics);
            objectMapper = new ObjectMapper();
        }

        @Override
        public void run(SourceContext<DataEntity> sourceContext) throws Exception {
            try {
                while (running) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        synchronized (sourceContext.getCheckpointLock()) {
                            String id = record.key() != null ? record.key() : UUID.randomUUID().toString();
                            long timestamp = record.timestamp();
                            Map<String, Object> fields;
                            try {
                                fields = decodeFields(format, record.value(), objectMapper);
                            } catch (Exception exception) {
                                LOG.warn(
                                        "Skipping Kafka Source record from topic '{}' partition {} offset {} because {}",
                                        record.topic(),
                                        record.partition(),
                                        record.offset(),
                                        exception.getMessage());
                                continue;
                            }

                            Map<String, String> headers = new HashMap<>();
                            headers.put("topic", record.topic());
                            headers.put("partition", String.valueOf(record.partition()));
                            headers.put("offset", String.valueOf(record.offset()));

                            sourceContext.collect(new DataEntity(id, timestamp, fields, headers));
                        }
                    }
                    if (!records.isEmpty()) {
                        consumer.commitAsync();
                    }
                }
            } catch (WakeupException wakeupException) {
                if (running) {
                    throw wakeupException;
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
            if (consumer != null) {
                consumer.wakeup();
            }
        }

        @Override
        public void close() {
            if (consumer != null) {
                consumer.close(Duration.ofSeconds(5));
            }
        }
    }

    private static String resolveAutoOffsetReset(String consumeMode) {
        String normalizedMode = consumeMode == null ? "" : consumeMode.trim().toLowerCase(Locale.ROOT);
        return switch (normalizedMode) {
            case "earliest" -> "earliest";
            case "latest" -> "latest";
            case "committed" -> "none";
            default -> throw new IllegalArgumentException("Unsupported consumeMode: " + consumeMode);
        };
    }
}
