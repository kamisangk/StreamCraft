package com.streamcraft.service.flink.client;

import com.fasterxml.jackson.annotation.JsonProperty;

public record FlinkOverviewResponse(
        @JsonProperty("taskmanagers") Integer taskmanagers,
        @JsonProperty("slots-total") Integer slotsTotal,
        @JsonProperty("slots-available") Integer slotsAvailable,
        @JsonProperty("flink-version") String flinkVersion) {
}
