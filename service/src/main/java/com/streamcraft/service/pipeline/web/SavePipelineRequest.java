package com.streamcraft.service.pipeline.web;

import jakarta.validation.constraints.NotBlank;

public record SavePipelineRequest(
        Long id,
        @NotBlank(message = "Pipeline name is required.") String name,
        String description,
        @NotBlank(message = "Pipeline definition is required.") String definitionJson) {
}
