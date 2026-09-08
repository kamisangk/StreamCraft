package com.streamcraft.core.runtime;

import java.io.Serializable;

/** Materialized Kafka key/value pair produced before the connector owns delivery. */
record KafkaPayloadRecord(String key, String value) implements Serializable {
}
