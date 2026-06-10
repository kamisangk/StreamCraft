package com.streamcraft.core.model;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;

public enum PipelineOperator {
    KAFKA_SOURCE,
    JDBC_SOURCE,
    ELASTICSEARCH_SOURCE,
    INFLUXDB_SOURCE,
    HDFS_FILE_SOURCE,
    KAFKA_SINK,
    JDBC_SINK,
    ELASTICSEARCH_SINK,
    INFLUXDB_SINK,
    HDFS_FILE_SINK,

    // Transform operators
    PUT,
    PRUNE,
    RENAME,
    DESERIALIZE,
    SERIALIZE,
    FILTER,
    GROK,
    CAST,
    EVAL,
    CUSTOM_CODE,
    AGGREGATE,
    DEDUPLICATE,
    LOOKUP_ENRICH,
    LOOKUP_JOIN,
    STREAM_JOIN,
    FLATTEN,
    EXPLODE,
    DATA_QUALITY,
    TIME_DERIVE,
    MASK_HASH,
    CASE_WHEN,
    ROUTE,
    JSON_PARSER,
    JOIN,

    @JsonEnumDefaultValue
    UNKNOWN
}
