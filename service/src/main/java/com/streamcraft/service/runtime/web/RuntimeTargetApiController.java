package com.streamcraft.service.runtime.web;

import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import jakarta.validation.Valid;
import java.util.Map;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/runtime-target")
public class RuntimeTargetApiController {

    private final FlinkRuntimeTargetService runtimeTargetService;

    public RuntimeTargetApiController(FlinkRuntimeTargetService runtimeTargetService) {
        this.runtimeTargetService = runtimeTargetService;
    }

    @GetMapping
    public RuntimeTargetResponse getTarget() {
        return runtimeTargetService.findTarget()
                .map(RuntimeTargetResponse::from)
                .orElseGet(RuntimeTargetResponse::unconfigured);
    }

    @PutMapping("/standalone")
    public RuntimeTargetResponse saveStandalone(@Valid @RequestBody SaveStandaloneRuntimeTargetRequest request) {
        return RuntimeTargetResponse.from(runtimeTargetService.saveStandalone(request));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public org.springframework.http.ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException exception) {
        return org.springframework.http.ResponseEntity.badRequest().body(Map.of("message", exception.getMessage()));
    }
}
