package com.streamcraft.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.cli.CoreCommandLineOptions;
import com.streamcraft.core.config.PipelineDefinitionLoader;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.runtime.ExecutionMode;
import com.streamcraft.core.runtime.ElasticsearchSinkFactory;
import com.streamcraft.core.runtime.ElasticsearchSourceFactory;
import com.streamcraft.core.runtime.InfluxDbSinkFactory;
import com.streamcraft.core.runtime.InfluxDbSourceFactory;
import com.streamcraft.core.runtime.JdbcSinkFactory;
import com.streamcraft.core.runtime.JdbcSourceFactory;
import com.streamcraft.core.runtime.KafkaSinkFactory;
import com.streamcraft.core.runtime.KafkaSourceFactory;
import com.streamcraft.core.runtime.MockSourceFactory;
import com.streamcraft.core.runtime.PipelineRuntime;
import com.streamcraft.core.runtime.PreviewCollectingSinkFactory;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import com.streamcraft.core.submission.PackagedProgramJobGraphFactory;
import com.streamcraft.core.submission.RemoteSubmissionArgumentsBuilder;
import com.streamcraft.core.validation.PipelineRuntimeValidator;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

public class StreamCraftCoreApplication {

    private static final PackagedProgramJobGraphFactory JOB_GRAPH_FACTORY = new PackagedProgramJobGraphFactory();
    private static final RemoteSubmissionArgumentsBuilder REMOTE_SUBMISSION_ARGUMENTS_BUILDER =
            new RemoteSubmissionArgumentsBuilder();

    public static void main(String[] args) throws Exception {
        CoreCommandLineOptions options = CoreCommandLineOptions.parse(args);
        if (hasText(options.clusterBaseUrl())) {
            submitToCluster(options);
            return;
        }

        PipelineDefinition definition = new PipelineDefinitionLoader().load(options);
        new PipelineRuntimeValidator().validate(definition, options.executionMode());

        StreamExecutionEnvironment env = createExecutionEnvironment(options);
        if (options.parallelism() != null) {
            env.setParallelism(options.parallelism());
        }
        env.enableCheckpointing(10_000L);

        ExecutionMode executionMode = options.executionMode();
        PreviewCollectingSinkFactory previewSinkFactory = executionMode.interceptSinks()
                ? new PreviewCollectingSinkFactory()
                : null;
        KafkaSinkFactory sinkFactory = previewSinkFactory != null ? previewSinkFactory : new KafkaSinkFactory();

        new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new MockSourceFactory(),
                new ElasticsearchSourceFactory(),
                new InfluxDbSourceFactory(),
                new JdbcSourceFactory(),
                sinkFactory,
                new JdbcSinkFactory(),
                new ElasticsearchSinkFactory(),
                new InfluxDbSinkFactory(),
                new TransformOperatorFactory(),
                options.testMode(),
                executionMode).run(definition);
        String jobName = "StreamCraft_Job_" + definition.pipelineId();
        env.execute(jobName);

        if (executionMode == ExecutionMode.PREVIEW
                && options.previewOutputFile() != null
                && previewSinkFactory != null) {
            new ObjectMapper().writeValue(Path.of(options.previewOutputFile()).toFile(), previewSinkFactory.snapshot());
        }
    }

    private static StreamExecutionEnvironment createExecutionEnvironment(CoreCommandLineOptions options) {
        return options.testMode() || options.executionMode() == ExecutionMode.PREVIEW
                ? StreamExecutionEnvironment.createLocalEnvironment()
                : StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static void submitToCluster(CoreCommandLineOptions options) throws Exception {
        URI clusterUri = URI.create(options.clusterBaseUrl());
        if (!hasText(clusterUri.getHost())) {
            throw new IllegalArgumentException("Invalid --cluster-base-url host.");
        }

        Configuration configuration = new Configuration();
        configuration.set(RestOptions.ADDRESS, clusterUri.getHost());
        configuration.set(RestOptions.PORT, clusterUri.getPort() > 0 ? clusterUri.getPort() : 8081);
        int parallelism = options.parallelism() == null || options.parallelism() < 1 ? 1 : options.parallelism();

        JobGraph jobGraph = JOB_GRAPH_FACTORY.create(
                resolveProgramJar(options),
                StreamCraftCoreApplication.class.getName(),
                REMOTE_SUBMISSION_ARGUMENTS_BUILDER.build(options),
                configuration,
                parallelism);

        try (RestClusterClient<StandaloneClusterId> clusterClient =
                     new RestClusterClient<>(configuration, StandaloneClusterId.getInstance())) {
            org.apache.flink.api.common.JobID jobId = clusterClient.submitJob(jobGraph).get();
            System.out.println("STREAMCRAFT_JOB_ID=" + jobId);
            System.out.flush();
        }
    }

    private static List<String> resolveShipJars(CoreCommandLineOptions options) {
        List<String> shipJars = new ArrayList<>();
        if (hasText(options.shipJar())) {
            shipJars.add(options.shipJar());
            return shipJars;
        }

        try {
            URI codeSource = StreamCraftCoreApplication.class.getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI();
            Path path = Path.of(codeSource);
            if (Files.isRegularFile(path) && path.toString().endsWith(".jar")) {
                shipJars.add(path.toString());
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to resolve the core jar for remote submission.", exception);
        }
        return shipJars;
    }

    private static String resolveProgramJar(CoreCommandLineOptions options) {
        List<String> shipJars = resolveShipJars(options);
        if (shipJars.isEmpty()) {
            throw new IllegalStateException("Unable to resolve a jar for remote submission.");
        }
        return shipJars.get(0);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
