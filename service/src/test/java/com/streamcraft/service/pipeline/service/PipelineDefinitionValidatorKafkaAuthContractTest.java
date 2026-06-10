package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorKafkaAuthContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runAllowsNoneAuthWithoutCredentials() {
        assertDoesNotThrow(() -> validator.validateForRun("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders"],
                        "groupId": "group-a",
                        "consumeMode": "earliest",
                        "authType": "NONE",
                        "username": "",
                        "password": "",
                        "scramMechanism": "",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "orders-out",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "NONE",
                        "username": "",
                        "password": "",
                        "scramMechanism": "",
                        "format": "JSON"
                      }
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """));
    }

    @Test
    void runRequiresUsernameWhenKafkaAuthTypeIsSaslPlain() {
        assertThatThrownBy(() -> validator.validateForRun("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders"],
                        "groupId": "group-a",
                        "consumeMode": "earliest",
                        "authType": "SASL_PLAIN",
                        "username": "",
                        "password": "secret-value",
                        "scramMechanism": "",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "orders-out",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "SASL_PLAIN",
                        "username": "",
                        "password": "secret-value",
                        "scramMechanism": "",
                        "format": "JSON"
                      }
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("username");
    }

    @Test
    void runRequiresPasswordWhenKafkaAuthTypeIsSaslPlain() {
        assertThatThrownBy(() -> validator.validateForRun("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders"],
                        "groupId": "group-a",
                        "consumeMode": "earliest",
                        "authType": "SASL_PLAIN",
                        "username": "streamcraft-user",
                        "password": "",
                        "scramMechanism": "",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "orders-out",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "SASL_PLAIN",
                        "username": "streamcraft-user",
                        "password": "",
                        "scramMechanism": "",
                        "format": "JSON"
                      }
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    void runRequiresScramMechanismWhenKafkaAuthTypeIsSaslScram() {
        assertThatThrownBy(() -> validator.validateForRun("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders"],
                        "groupId": "group-a",
                        "consumeMode": "earliest",
                        "authType": "SASL_SCRAM",
                        "username": "streamcraft-user",
                        "password": "secret-value",
                        "scramMechanism": "",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "orders-out",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "SASL_SCRAM",
                        "username": "streamcraft-user",
                        "password": "secret-value",
                        "scramMechanism": "",
                        "format": "JSON"
                      }
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scramMechanism");
    }

    @Test
    void runRejectsUnsupportedScramMechanismWhenKafkaAuthTypeIsSaslScram() {
        assertThatThrownBy(() -> validator.validateForRun("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders"],
                        "groupId": "group-a",
                        "consumeMode": "earliest",
                        "authType": "SASL_SCRAM",
                        "username": "streamcraft-user",
                        "password": "secret-value",
                        "scramMechanism": "SCRAM-SHA-999",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "orders-out",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "SASL_SCRAM",
                        "username": "streamcraft-user",
                        "password": "secret-value",
                        "scramMechanism": "SCRAM-SHA-999",
                        "format": "JSON"
                      }
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SCRAM-SHA-256")
                .hasMessageContaining("SCRAM-SHA-512");
    }
}
