package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSinkFactory {

    public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
        JsonNode config = sinkNode.config();
        String topic = config.path("topic").asText();
        SinkDeliveryGuarantee guarantee = resolveGuarantee(config.path("deliveryGuarantee").asText());
        stream.addSink(new LegacyKafkaSinkFunction(
                        config.path("bootstrapServers").asText(),
                        topic,
                        guarantee,
                        config.path("format").asText("JSON"),
                        config.path("messageField").asText("_streamcraft_message"),
                        config))
                .name(sinkNode.name());
    }

    static String buildPayload(String format, String messageField, DataEntity value, ObjectMapper objectMapper)
            throws IOException {
        if ("JSON".equals(format)) {
            return objectMapper.writeValueAsString(value.fields());
        }

        Object fieldValue = resolveMessageFieldValue(value.fields(), messageField);
        if (fieldValue == null) {
            throw new IllegalArgumentException("Kafka Sink TEXT format requires a non-null messageField value.");
        }
        if (fieldValue instanceof String stringValue) {
            return stringValue;
        }
        if (fieldValue instanceof Number || fieldValue instanceof Boolean) {
            return String.valueOf(fieldValue);
        }
        return objectMapper.writeValueAsString(fieldValue);
    }

    static String buildPayloadOrSkip(
            String format,
            String messageField,
            String topic,
            DataEntity value,
            ObjectMapper objectMapper,
            Consumer<String> skipLogger) {
        try {
            return buildPayload(format, messageField, value, objectMapper);
        } catch (Exception exception) {
            skipLogger.accept(String.format(
                    "Skipping Kafka Sink %s record for topic '%s' with messageField '%s' and record id '%s' because %s",
                    format,
                    topic,
                    messageField,
                    value.id(),
                    exception.getMessage()));
            return null;
        }
    }

    private static Object resolveMessageFieldValue(Map<String, Object> fields, String messageField) {
        if (messageField == null || messageField.isBlank()) {
            return null;
        }
        if (fields.containsKey(messageField)) {
            return fields.get(messageField);
        }

        Object current = fields;
        for (String segment : messageField.split("\\.", -1)) {
            if (segment.isEmpty()) {
                return null;
            }
            if (!(current instanceof Map<?, ?> currentMap)) {
                return null;
            }
            current = currentMap.get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private SinkDeliveryGuarantee resolveGuarantee(String value) {
        return switch (value) {
            case "NONE" -> SinkDeliveryGuarantee.NONE;
            case "EXACTLY_ONCE" -> SinkDeliveryGuarantee.EXACTLY_ONCE;
            default -> SinkDeliveryGuarantee.AT_LEAST_ONCE;
        };
    }

    private static final class LegacyKafkaSinkFunction extends RichSinkFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(LegacyKafkaSinkFunction.class);

        private final String bootstrapServers;
        private final String topic;
        private final SinkDeliveryGuarantee guarantee;
        private final String format;
        private final String messageField;
        private final JsonNode config;
        private transient KafkaProducer<String, String> producer;
        private transient ObjectMapper objectMapper;
        private transient AtomicReference<RuntimeException> sendFailure;

        private LegacyKafkaSinkFunction(
                String bootstrapServers,
                String topic,
                SinkDeliveryGuarantee guarantee,
                String format,
                String messageField,
                JsonNode config) {
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.guarantee = guarantee;
            this.format = format;
            this.messageField = messageField;
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.ACKS_CONFIG, guarantee == SinkDeliveryGuarantee.NONE ? "0" : "all");
            if (guarantee == SinkDeliveryGuarantee.EXACTLY_ONCE) {
                properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
                properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
            }
            KafkaAuthProperties.apply(config, properties);
            producer = new KafkaProducer<>(properties);
            objectMapper = new ObjectMapper();
            sendFailure = new AtomicReference<>();
        }

        @Override
        public void invoke(DataEntity value, Context context) throws Exception {
            failIfSendFailed();
            String payload = buildPayloadOrSkip(
                    format,
                    messageField,
                    topic,
                    value,
                    objectMapper,
                    message -> LOG.warn(message));
            if (payload == null) {
                return;
            }

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value.id(), payload);
            producer.send(record, (metadata, exception) -> {
                if (exception != null && guarantee != SinkDeliveryGuarantee.NONE) {
                    sendFailure.compareAndSet(
                            null, new IllegalStateException("Failed to publish record to Kafka.", exception));
                }
            });
        }

        @Override
        public void close() {
            if (producer != null) {
                producer.flush();
                failIfSendFailed();
                producer.close(Duration.ofSeconds(5));
            }
        }

        private void failIfSendFailed() {
            RuntimeException failure = sendFailure == null ? null : sendFailure.get();
            if (failure != null) {
                throw failure;
            }
        }
    }

    private enum SinkDeliveryGuarantee {
        NONE,
        AT_LEAST_ONCE,
        EXACTLY_ONCE
    }
}
