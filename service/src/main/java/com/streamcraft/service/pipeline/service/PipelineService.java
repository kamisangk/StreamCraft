package com.streamcraft.service.pipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.SavePipelineRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PipelineService {

    private final PipelineRepository repository;
    private final PipelineDefinitionService definitionService;
    private final PipelineRuntimeStateSupport runtimeStateSupport;
    private final UiMessageService messages;

    @Autowired
    public PipelineService(PipelineRepository repository,
                           PipelineDefinitionService definitionService,
                           PipelineRuntimeStateSupport runtimeStateSupport,
                           UiMessageService messages) {
        this.repository = repository;
        this.definitionService = definitionService;
        this.runtimeStateSupport = runtimeStateSupport;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    @Transactional
    public Pipeline save(SavePipelineRequest request) {
        Pipeline pipeline = request.id() == null
                ? new Pipeline()
                : getStored(request.id());

        String normalizedDefinition = definitionService.normalizeAndValidateForSave(request.definitionJson());

        pipeline.setName(request.name());
        pipeline.setDescription(request.description());
        pipeline.setDefinitionJson(normalizedDefinition);

        return repository.save(pipeline);
    }

    @Transactional(readOnly = true)
    public JsonNode getDefinition(Long id) {
        return definitionService.getDefinition(id);
    }

    public void delete(Long id) {
        Pipeline pipeline = getStored(id);
        if (runtimeStateSupport.resolveRuntimeStatus(pipeline) == PipelineRunStatus.RUNNING) {
            throw new IllegalArgumentException(messages.get("pipeline.error.stopBeforeDeletion"));
        }
        repository.delete(pipeline);
    }

    private Pipeline getStored(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException(messages.get("pipeline.error.notFound")));
    }

}
