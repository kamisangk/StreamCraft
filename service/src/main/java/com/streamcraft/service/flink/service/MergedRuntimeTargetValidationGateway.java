package com.streamcraft.service.flink.service;

import com.streamcraft.service.runtime.client.RuntimeTargetValidationGateway;
import com.streamcraft.service.runtime.client.StandaloneValidationResponse;
import com.streamcraft.service.flink.client.FlinkOverviewClient;
import com.streamcraft.service.flink.client.FlinkOverviewResponse;
import java.net.URI;
import java.net.URISyntaxException;

public class MergedRuntimeTargetValidationGateway implements RuntimeTargetValidationGateway {

    private final FlinkOverviewClient flinkOverviewClient;

    public MergedRuntimeTargetValidationGateway(FlinkOverviewClient flinkOverviewClient) {
        this.flinkOverviewClient = flinkOverviewClient;
    }

    @Override
    public StandaloneValidationResponse validateStandalone(String jobManagerUrl) {
        if (!isHttpUrl(jobManagerUrl)) {
            return new StandaloneValidationResponse(
                    false,
                    "UNREACHABLE",
                    "JobManager URL must start with http:// or https://.",
                    null,
                    null,
                    null,
                    null);
        }

        try {
            FlinkOverviewResponse overview = flinkOverviewClient.fetchOverview(jobManagerUrl);
            if (overview == null || overview.flinkVersion() == null) {
                return new StandaloneValidationResponse(
                        false,
                        "UNREACHABLE",
                        "The target endpoint is reachable but does not look like a Flink cluster.",
                        null,
                        null,
                        null,
                        null);
            }

            return new StandaloneValidationResponse(
                    true,
                    "CONNECTED",
                    "Flink Standalone cluster is reachable.",
                    overview.flinkVersion(),
                    overview.taskmanagers(),
                    overview.slotsTotal(),
                    overview.slotsAvailable());
        } catch (Exception exception) {
            return new StandaloneValidationResponse(
                    false,
                    "UNREACHABLE",
                    "Failed to connect to Flink JobManager endpoint.",
                    null,
                    null,
                    null,
                    null);
        }
    }

    private boolean isHttpUrl(String value) {
        try {
            URI uri = new URI(value);
            return "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
        } catch (URISyntaxException exception) {
            return false;
        }
    }
}
