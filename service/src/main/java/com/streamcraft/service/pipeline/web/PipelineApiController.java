package com.streamcraft.service.pipeline.web;

import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.service.PipelineService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineApiController {

    private final PipelineService pipelineService;

    public PipelineApiController(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @PostMapping
    public ResponseEntity<PipelineSummaryResponse> save(@Valid @RequestBody SavePipelineRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(PipelineSummaryResponse.from(pipelineService.save(request)));
    }

    @GetMapping
    public List<PipelineSummaryResponse> list() {
        return pipelineService.list().stream()
                .map(PipelineSummaryResponse::from)
                .toList();
    }

    @GetMapping("/{id}")
    public PipelineDetailResponse get(@PathVariable Long id) {
        return PipelineDetailResponse.from(pipelineService.get(id));
    }

    @GetMapping("/{id}/definition")
    public PipelineDefinitionResponse definition(@PathVariable Long id) {
        return PipelineDefinitionResponse.from(pipelineService.getDefinition(id));
    }

    @PostMapping("/{id}/run")
    public PipelineSummaryResponse run(@PathVariable Long id, @RequestBody(required = false) RunPipelineRequest request) {
        return PipelineSummaryResponse.from(pipelineService.run(id, request));
    }

    @PostMapping("/preview")
    public PipelinePreviewResponse preview(@RequestBody PipelinePreviewRequest request) {
        return pipelineService.preview(request);
    }

    @PostMapping("/{id}/stop")
    public PipelineSummaryResponse stop(@PathVariable Long id) {
        return PipelineSummaryResponse.from(pipelineService.stop(id));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        pipelineService.delete(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{id}/metrics")
    public PipelineMetrics metrics(@PathVariable Long id) {
        return pipelineService.getMetrics(id);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException exception) {
        return ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
    }
}
