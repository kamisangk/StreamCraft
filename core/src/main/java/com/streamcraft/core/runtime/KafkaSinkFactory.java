package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Attaches the official Flink Kafka Sink and its checkpoint-aware delivery guarantee. */
public class KafkaSinkFactory implements KafkaSinkAttacher {

    private static final int MAX_TRANSACTIONAL_ID_PREFIX_LENGTH = 180;
    private static final int TRANSACTIONAL_ID_HASH_LENGTH = 16;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkFactory.class);

    /**
     * Compatibility overload for callers that do not provide an execution identity.
     * Exactly-once sinks intentionally fail because they cannot be made recoverable
     * without a stable identity and checkpointing capability.
     */
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
            throw new IllegalArgumentException("Kafka Sink input stream is required.");
        }
        if (sinkNode == null) {
            throw new IllegalArgumentException("Kafka Sink node is required.");
        }

        KafkaSinkConfig config = KafkaSinkConfig.from(sinkNode.config());
        requireCheckpointingForExactlyOnce(config, checkpointingEnabled);
        if (config.deliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE
                && (pipelineId == null || pipelineId.isBlank())) {
            throw new IllegalArgumentException("Kafka Sink EXACTLY_ONCE requires a stable pipelineId.");
        }

        DataStream<KafkaPayloadRecord> records = stream
                .flatMap(new KafkaPayloadFlatMapFunction(config))
                .name("kafka-payload-" + sinkNode.id())
                ;

        Properties producerProperties = buildProducerProperties(config);

        KafkaSinkBuilder<KafkaPayloadRecord> builder = KafkaSink.<KafkaPayloadRecord>builder()
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(new KafkaPayloadSerializationSchema(config.topic()))
                .setDeliveryGuarantee(config.deliveryGuarantee());
        if (config.deliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            builder.setTransactionalIdPrefix(transactionalIdPrefix(deploymentNamespace, pipelineId, sinkNode.id()));
        }
        records.sinkTo(builder.build()).name(sinkNode.name());
    }

    static Properties buildProducerProperties(KafkaSinkConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Kafka Sink config is required.");
        }

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(
                ProducerConfig.ACKS_CONFIG,
                config.deliveryGuarantee() == DeliveryGuarantee.NONE ? "0" : "all");
        if (config.deliveryGuarantee() == DeliveryGuarantee.NONE) {
            // Kafka 3.x enables idempotence by default when no conflicting setting exists.
            properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        }
        KafkaAuthProperties.apply(config, properties);
        return properties;
    }

    static void requireCheckpointingForExactlyOnce(KafkaSinkConfig config, boolean checkpointingEnabled) {
        if (config.deliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE && !checkpointingEnabled) {
            throw new IllegalArgumentException(
                    "Kafka Sink EXACTLY_ONCE requires Flink checkpointing to be enabled.");
        }
    }

    static String transactionalIdPrefix(String deploymentNamespace, String pipelineId, String sinkNodeId) {
        requireText(deploymentNamespace, "deployment namespace");
        requireText(pipelineId, "pipelineId");
        requireText(sinkNodeId, "sink node id");

        String hash = shortHash(deploymentNamespace + "\u0000" + pipelineId + "\u0000" + sinkNodeId);
        String base = "streamcraft-"
                + sanitize(deploymentNamespace)
                + "-"
                + sanitize(pipelineId)
                + "-"
                + sanitize(sinkNodeId);
        int maximumBaseLength = MAX_TRANSACTIONAL_ID_PREFIX_LENGTH - TRANSACTIONAL_ID_HASH_LENGTH - 1;
        if (base.length() > maximumBaseLength) {
            base = base.substring(0, maximumBaseLength);
        }
        return base + "-" + hash;
    }

    /** Retained as a package-level compatibility helper for existing codec tests. */
    static String buildPayload(String format, String messageField, DataEntity value, ObjectMapper objectMapper)
            throws IOException {
        return KafkaPayloadCodec.encode(format, messageField, value, objectMapper);
    }

    /** Retained as a package-level compatibility helper for existing skip-behavior tests. */
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
                    value == null ? null : value.id(),
                    exception.getMessage()));
            return null;
        }
    }

    private static String sanitize(String value) {
        String normalized = value.trim().replaceAll("[^A-Za-z0-9._-]+", "-");
        normalized = normalized.replaceAll("-+", "-");
        if (normalized.isBlank()) {
            return "default";
        }
        return normalized;
    }

    private static String shortHash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, TRANSACTIONAL_ID_HASH_LENGTH);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required for Kafka transactional identity.", exception);
        }
    }

    private static void requireText(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Kafka Sink " + label + " must not be blank.");
        }
    }

    private static final class KafkaPayloadFlatMapFunction extends RichFlatMapFunction<DataEntity, KafkaPayloadRecord> {

        private final KafkaSinkConfig config;
        private transient ObjectMapper objectMapper;

        private KafkaPayloadFlatMapFunction(KafkaSinkConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            objectMapper = new ObjectMapper();
        }

        @Override
        public void flatMap(DataEntity value, Collector<KafkaPayloadRecord> out) throws Exception {
            try {
                out.collect(new KafkaPayloadRecord(value.id(), KafkaPayloadCodec.encode(config, value, objectMapper)));
            } catch (Exception exception) {
                if (config.deliveryGuarantee() != DeliveryGuarantee.EXACTLY_ONCE
                        && "TEXT".equals(config.format())) {
                    LOG.warn(
                            "Skipping Kafka Sink TEXT record for topic '{}' with messageField '{}' and record id '{}' because {}",
                            config.topic(),
                            config.messageField(),
                            value == null ? null : value.id(),
                            exception.getMessage());
                    return;
                }
                throw exception;
            }
        }
    }

    private static final class KafkaPayloadSerializationSchema
            implements KafkaRecordSerializationSchema<KafkaPayloadRecord> {

        private final String topic;

        private KafkaPayloadSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                KafkaPayloadRecord element,
                KafkaSinkContext context,
                Long timestamp) {
            byte[] key = element.key() == null ? null : element.key().getBytes(StandardCharsets.UTF_8);
            byte[] value = element.value().getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topic, key, value);
        }
    }
}
