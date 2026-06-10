package com.streamcraft.service.pipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.config.PipelineRuntimeProperties;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.pipeline.client.FlinkMetricsClient;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobRequest;
import com.streamcraft.service.pipeline.client.StopFlinkJobRequest;
import com.streamcraft.service.pipeline.client.StopFlinkJobResponse;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobRequest;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobResponse;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.PipelinePreviewOutputResponse;
import com.streamcraft.service.pipeline.web.PipelinePreviewRequest;
import com.streamcraft.service.pipeline.web.PipelinePreviewResponse;
import com.streamcraft.service.pipeline.web.RunPipelineRequest;
import com.streamcraft.service.pipeline.web.SavePipelineRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpClientErrorException;

@Service
public class PipelineService {

    private final PipelineRepository repository;
    private final ObjectMapper objectMapper;
    private final PipelineDefinitionValidator validator;
    private final PipelineDefinitionNormalizer definitionNormalizer;
    private final FlinkRuntimeTargetService runtimeTargetService;
    private final FlinkJobGateway flinkJobGateway;
    private final FlinkMetricsClient flinkMetricsClient;
    private final PipelineRuntimeProperties runtimeProperties;
    private final UiMessageService messages;

    @Autowired
    public PipelineService(PipelineRepository repository,
                           ObjectMapper objectMapper,
                           PipelineDefinitionValidator validator,
                           PipelineDefinitionNormalizer definitionNormalizer,
                           FlinkRuntimeTargetService runtimeTargetService,
                           FlinkJobGateway flinkJobGateway,
                           FlinkMetricsClient flinkMetricsClient,
                           PipelineRuntimeProperties runtimeProperties,
                           UiMessageService messages) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.definitionNormalizer = definitionNormalizer;
        this.runtimeTargetService = runtimeTargetService;
        this.flinkJobGateway = flinkJobGateway;
        this.flinkMetricsClient = flinkMetricsClient;
        this.runtimeProperties = runtimeProperties;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public PipelineService(PipelineRepository repository,
                           ObjectMapper objectMapper,
                           PipelineDefinitionValidator validator,
                           PipelineDefinitionNormalizer definitionNormalizer,
                           FlinkRuntimeTargetService runtimeTargetService,
                           FlinkJobGateway flinkJobGateway,
                           FlinkMetricsClient flinkMetricsClient,
                           PipelineRuntimeProperties runtimeProperties) {
        this(
                repository,
                objectMapper,
                validator,
                definitionNormalizer,
                runtimeTargetService,
                flinkJobGateway,
                flinkMetricsClient,
                runtimeProperties,
                UiMessageService.englishFallback());
    }

    @Transactional
    public Pipeline save(SavePipelineRequest request) {
        Pipeline pipeline = request.id() == null
                ? new Pipeline()
                : getStored(request.id());

        String normalizedDefinition = definitionNormalizer.normalize(request.definitionJson());
        validator.validateForSave(normalizedDefinition);

        pipeline.setName(request.name());
        pipeline.setDescription(request.description());
        pipeline.setDefinitionJson(normalizedDefinition);

        return repository.save(pipeline);
    }

    public List<Pipeline> list() {
        FlinkRuntimeTarget runtimeTarget = loadRuntimeTarget();
        return resolveRuntimeStatus(repository.findAllByOrderByUpdatedAtDesc(), runtimeTarget);
    }

    public List<Pipeline> listRunningPipelines() {
        FlinkRuntimeTarget runtimeTarget = loadRuntimeTarget();
        return resolveRuntimeStatus(repository.findByLastRunStatus(PipelineRunStatus.RUNNING), runtimeTarget).stream()
                .filter(this::isRunning)
                .toList();
    }

    public List<PipelineRuntimeSnapshot> listRuntimeSnapshots() {
        List<Pipeline> storedPipelines = repository.findAllByOrderByUpdatedAtDesc();
        FlinkRuntimeTarget runtimeTarget = loadRuntimeTarget();

        List<PipelineRuntimeSnapshot> snapshots = new ArrayList<>();
        for (Pipeline pipeline : storedPipelines) {
            snapshots.add(buildRuntimeSnapshot(pipeline, runtimeTarget));
        }
        return snapshots;
    }

    public Pipeline get(Long id) {
        return withResolvedRuntimeStatus(getStored(id));
    }

    @Transactional(readOnly = true)
    public JsonNode getDefinition(Long id) {
        Pipeline pipeline = getStored(id);
        try {
            JsonNode definition = objectMapper.readTree(pipeline.getDefinitionJson());
            return definitionNormalizer.normalizeTree(definition);
        } catch (IOException ex) {
            throw new IllegalStateException(messages.get("pipeline.error.storedDefinitionInvalidJson"), ex);
        }
    }

    public PipelinePreviewResponse preview(PipelinePreviewRequest request) {
        String normalizedDefinition = definitionNormalizer.normalize(request.definitionJson());
        validator.validateForPreview(normalizedDefinition);
        try {
            PipelinePreviewExecutionResult result = toPreviewExecutionResult(
                    flinkJobGateway.preview(new PreviewFlinkJobRequest(
                            normalizedDefinition,
                            1)));
            return toPreviewResponse(normalizedDefinition, result);
        } catch (Exception exception) {
            throw new IllegalStateException(messages.get("pipeline.error.previewExecutionFailed"), exception);
        }
    }

    public Pipeline run(Long id, RunPipelineRequest request) {
        Pipeline pipeline = getStored(id);
        validator.validateForRun(pipeline.getDefinitionJson());
        FlinkRuntimeTarget runtimeTarget = runtimeTargetService.requireTarget();

        SubmitFlinkJobResponse response = flinkJobGateway.submit(new SubmitFlinkJobRequest(
                runtimeTarget.getJobManagerUrl(),
                buildDefinitionUrl(id),
                request != null && request.testMode() != null ? request.testMode() : runtimeProperties.testMode(),
                request != null && request.parallelism() != null ? request.parallelism() : runtimeProperties.parallelism()));

        pipeline.setLastRunStatus(PipelineRunStatus.RUNNING);
        pipeline.setLastRunMessage(response.message());
        pipeline.setLastJobId(response.jobId());
        pipeline.setLastSubmittedAt(Instant.now());
        return repository.save(pipeline);
    }

    public Pipeline stop(Long id) {
        Pipeline pipeline = getStored(id);
        if (!hasText(pipeline.getLastJobId())) {
            throw new IllegalArgumentException(messages.get("pipeline.error.runningJobIdMissing"));
        }
        FlinkRuntimeTarget runtimeTarget = runtimeTargetService.requireTarget();

        PipelineRunStatus runtimeStatus = resolveRuntimeStatus(pipeline, runtimeTarget);
        if (runtimeStatus != PipelineRunStatus.RUNNING) {
            return copyWithRunStatus(
                    pipeline,
                    runtimeStatus,
                    runtimeStatus == PipelineRunStatus.FAILED
                            ? messages.get("pipeline.error.flinkJobNotRunning")
                            : pipeline.getLastRunMessage());
        }

        StopFlinkJobResponse response;
        try {
            response = flinkJobGateway.stop(new StopFlinkJobRequest(
                    runtimeTarget.getJobManagerUrl(),
                    pipeline.getLastJobId()));
        } catch (RuntimeException exception) {
            PipelineRunStatus refreshedStatus = resolveRuntimeStatus(pipeline, runtimeTarget);
            if (refreshedStatus != PipelineRunStatus.RUNNING) {
                return copyWithRunStatus(
                        pipeline,
                        refreshedStatus,
                        refreshedStatus == PipelineRunStatus.FAILED
                                ? messages.get("pipeline.error.flinkJobNotRunning")
                                : pipeline.getLastRunMessage());
            }
            throw exception;
        }

        pipeline.setLastRunStatus(PipelineRunStatus.STOPPED);
        pipeline.setLastRunMessage(response.message());
        return repository.save(pipeline);
    }

    public void delete(Long id) {
        Pipeline pipeline = getStored(id);
        if (resolveRuntimeStatus(pipeline) == PipelineRunStatus.RUNNING) {
            throw new IllegalArgumentException(messages.get("pipeline.error.stopBeforeDeletion"));
        }
        repository.delete(pipeline);
    }

    public PipelineMetrics getMetrics(Long id) {
        Pipeline pipeline = getStored(id);
        if (!hasText(pipeline.getLastJobId())) {
            throw new IllegalArgumentException(messages.get("pipeline.error.runningJobMissing"));
        }
        return resolveMetrics(pipeline, runtimeTargetService.requireTarget());
    }

    private Pipeline getStored(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException(messages.get("pipeline.error.notFound")));
    }

    PipelinePreviewResponse toPreviewResponse(String normalizedDefinition, PipelinePreviewExecutionResult result) {
        Map<String, String> titleByNodeId = indexPreviewTitles(normalizedDefinition);
        return new PipelinePreviewResponse(result.outputs().stream()
                .map(output -> new PipelinePreviewOutputResponse(
                        output.nodeId(),
                        titleByNodeId.getOrDefault(output.nodeId(), output.nodeId()),
                        output.records()))
                .toList());
    }

    private Pipeline withResolvedRuntimeStatus(Pipeline pipeline) {
        return withResolvedRuntimeStatus(pipeline, loadRuntimeTarget());
    }

    private Pipeline withResolvedRuntimeStatus(Pipeline pipeline, FlinkRuntimeTarget runtimeTarget) {
        Pipeline snapshot = copyPipeline(pipeline);
        snapshot.setLastRunStatus(resolveRuntimeStatus(pipeline, runtimeTarget));
        return snapshot;
    }

    private List<Pipeline> resolveRuntimeStatus(
            List<Pipeline> storedPipelines,
            FlinkRuntimeTarget runtimeTarget) {
        List<Pipeline> pipelines = new ArrayList<>();
        for (Pipeline pipeline : storedPipelines) {
            pipelines.add(withResolvedRuntimeStatus(pipeline, runtimeTarget));
        }
        return pipelines;
    }

    private PipelineRuntimeSnapshot buildRuntimeSnapshot(
            Pipeline pipeline,
            FlinkRuntimeTarget runtimeTarget) {
        Pipeline snapshot = copyPipeline(pipeline);
        snapshot.setLastRunStatus(resolveRuntimeStatus(pipeline, runtimeTarget));
        PipelineMetrics metrics = resolveMetricsIfEligible(snapshot, runtimeTarget);
        return new PipelineRuntimeSnapshot(snapshot, metrics);
    }

    private PipelineRunStatus resolveRuntimeStatus(Pipeline pipeline) {
        return resolveRuntimeStatus(pipeline, loadRuntimeTarget());
    }

    private PipelineRunStatus resolveRuntimeStatus(
            Pipeline pipeline,
            FlinkRuntimeTarget runtimeTarget) {
        if (!hasResolvableRunningJob(pipeline)
                || runtimeTarget == null
                || !hasText(runtimeTarget.getJobManagerUrl())) {
            return pipeline.getLastRunStatus();
        }

        try {
            PipelineRunStatus liveStatus = flinkMetricsClient.getJobStatus(
                    runtimeTarget.getJobManagerUrl(),
                    pipeline.getLastJobId());
            if (liveStatus == null) {
                return pipeline.getLastRunStatus();
            }
            return liveStatus == PipelineRunStatus.RUNNING ? PipelineRunStatus.RUNNING : PipelineRunStatus.FAILED;
        } catch (HttpClientErrorException exception) {
            return exception.getStatusCode() == HttpStatus.NOT_FOUND
                    ? PipelineRunStatus.FAILED
                    : pipeline.getLastRunStatus();
        } catch (RuntimeException exception) {
            return pipeline.getLastRunStatus();
        }
    }

    private FlinkRuntimeTarget loadRuntimeTarget() {
        return runtimeTargetService.findTarget().orElse(null);
    }

    private PipelineMetrics resolveMetricsIfEligible(
            Pipeline pipeline,
            FlinkRuntimeTarget runtimeTarget) {
        if (!hasResolvableRunningJob(pipeline)) {
            return null;
        }

        if (runtimeTarget == null || !hasText(runtimeTarget.getJobManagerUrl())) {
            return null;
        }

        try {
            return resolveMetrics(pipeline, runtimeTarget);
        } catch (RuntimeException exception) {
            return null;
        }
    }

    private PipelineMetrics resolveMetrics(Pipeline pipeline, FlinkRuntimeTarget runtimeTarget) {
        try {
            DefinitionNodes definitionNodes = parseDefinitionNodes(pipeline.getDefinitionJson());
            return flinkMetricsClient.getJobMetrics(
                    runtimeTarget.getJobManagerUrl(),
                    pipeline.getLastJobId(),
                    definitionNodes.nodeIds(),
                    definitionNodes.nodeNames());
        } catch (IOException ex) {
            throw new IllegalStateException(messages.get("pipeline.error.parseDefinitionFailed"), ex);
        }
    }

    private DefinitionNodes parseDefinitionNodes(String definitionJson) throws IOException {
        JsonNode definition = objectMapper.readTree(definitionJson);
        JsonNode nodes = definition.path("nodes");

        List<String> nodeIds = new ArrayList<>();
        Map<String, String> nodeNames = new HashMap<>();

        for (JsonNode node : nodes) {
            String nodeId = node.path("id").asText();
            String nodeName = node.path("name").asText();
            nodeIds.add(nodeId);
            nodeNames.put(nodeId, nodeName);
        }

        return new DefinitionNodes(nodeIds, nodeNames);
    }

    private Map<String, String> indexPreviewTitles(String definitionJson) {
        try {
            JsonNode root = objectMapper.readTree(definitionJson);
            Map<String, String> titles = new HashMap<>();
            for (JsonNode node : root.path("nodes")) {
                String nodeId = node.path("id").asText();
                String displayName = node.path("displayName").asText("");
                String fallbackName = node.path("name").asText(nodeId);
                titles.put(nodeId, displayName == null || displayName.isBlank() ? fallbackName : displayName);
            }
            return titles;
        } catch (IOException exception) {
            throw new IllegalStateException(messages.get("pipeline.error.mapPreviewTitlesFailed"), exception);
        }
    }

    private boolean hasResolvableRunningJob(Pipeline pipeline) {
        return pipeline.getLastRunStatus() == PipelineRunStatus.RUNNING
                && hasText(pipeline.getLastJobId());
    }

    private boolean isRunning(Pipeline pipeline) {
        return pipeline.getLastRunStatus() == PipelineRunStatus.RUNNING;
    }

    private Pipeline copyPipeline(Pipeline source) {
        Pipeline copy = new Pipeline();
        copy.setId(source.getId());
        copy.setName(source.getName());
        copy.setDescription(source.getDescription());
        copy.setDefinitionJson(source.getDefinitionJson());
        copy.setLastJobId(source.getLastJobId());
        copy.setLastRunStatus(source.getLastRunStatus());
        copy.setLastRunMessage(source.getLastRunMessage());
        copy.setLastSubmittedAt(source.getLastSubmittedAt());
        copy.setCreatedAt(source.getCreatedAt());
        copy.setUpdatedAt(source.getUpdatedAt());
        return copy;
    }

    private Pipeline copyWithRunStatus(Pipeline source, PipelineRunStatus runStatus, String runMessage) {
        Pipeline copy = copyPipeline(source);
        copy.setLastRunStatus(runStatus);
        copy.setLastRunMessage(runMessage);
        return copy;
    }

    private String buildDefinitionUrl(Long id) {
        String baseUrl = runtimeProperties.serviceBaseUrl();
        String normalizedBaseUrl = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        return normalizedBaseUrl + "/api/pipelines/" + id + "/definition";
    }

    private PipelinePreviewExecutionResult toPreviewExecutionResult(
            com.streamcraft.service.pipeline.client.PreviewFlinkJobResponse response) {
        return new PipelinePreviewExecutionResult(response.outputs().stream()
                .map(output -> new PipelinePreviewExecutionResult.Output(output.nodeId(), output.records()))
                .toList());
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private record DefinitionNodes(List<String> nodeIds, Map<String, String> nodeNames) {
    }
}
