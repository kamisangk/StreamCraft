package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.junit.jupiter.api.Test;

class KafkaSinkFactoryContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void mapsExactlyOnceToCheckpointRequirement() throws Exception {
        KafkaSinkConfig config = KafkaSinkConfig.from(objectMapper.readTree("""
                {"bootstrapServers":"broker:9092","topic":"out","deliveryGuarantee":"EXACTLY_ONCE","format":"JSON"}
                """));

        assertThrows(IllegalArgumentException.class,
                () -> KafkaSinkFactory.requireCheckpointingForExactlyOnce(config, false));
    }

    @Test
    void derivesStableBoundedTransactionalPrefix() {
        String prefix = KafkaSinkFactory.transactionalIdPrefix(
                "prod/east", "pipeline-123", "sink.kafka.1");

        assertEquals(prefix, KafkaSinkFactory.transactionalIdPrefix(
                "prod/east", "pipeline-123", "sink.kafka.1"));
        org.junit.jupiter.api.Assertions.assertTrue(prefix.length() <= 180);
        org.junit.jupiter.api.Assertions.assertTrue(prefix.matches("[A-Za-z0-9._-]+"));
    }

    @Test
    void disablesKafkaIdempotenceForNoneDelivery() throws Exception {
        KafkaSinkConfig config = KafkaSinkConfig.from(objectMapper.readTree("""
                {"bootstrapServers":"broker:9092","topic":"out","deliveryGuarantee":"NONE","format":"JSON"}
                """));

        java.util.Properties properties = KafkaSinkFactory.buildProducerProperties(config);

        assertEquals("0", properties.getProperty("acks"));
        assertEquals("false", properties.getProperty("enable.idempotence"));
        assertEquals(DeliveryGuarantee.NONE, config.deliveryGuarantee());
    }

    @Test
    void normalizesSinkAuthenticationTypeBeforeApplyingProperties() throws Exception {
        KafkaSinkConfig config = KafkaSinkConfig.from(objectMapper.readTree("""
                {
                  "bootstrapServers":"broker:9092",
                  "topic":"out",
                  "deliveryGuarantee":"AT_LEAST_ONCE",
                  "format":"JSON",
                  "authType":" sasl_plain ",
                  "username":"alice",
                  "password":"secret"
                }
                """));

        java.util.Properties properties = KafkaSinkFactory.buildProducerProperties(config);

        assertEquals("SASL_PLAINTEXT", properties.getProperty("security.protocol"));
        assertEquals("PLAIN", properties.getProperty("sasl.mechanism"));
        org.junit.jupiter.api.Assertions.assertTrue(
                properties.getProperty("sasl.jaas.config").contains("username=\"alice\""));
    }
}
