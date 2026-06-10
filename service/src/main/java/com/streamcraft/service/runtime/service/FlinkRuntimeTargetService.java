package com.streamcraft.service.runtime.service;

import com.streamcraft.service.runtime.client.RuntimeTargetValidationGateway;
import com.streamcraft.service.runtime.client.StandaloneValidationResponse;
import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.RuntimeTargetType;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.persistence.FlinkRuntimeTargetRepository;
import com.streamcraft.service.runtime.web.SaveStandaloneRuntimeTargetRequest;
import com.streamcraft.service.config.UiMessageService;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FlinkRuntimeTargetService {

    public static final long SINGLETON_ID = 1L;

    private final FlinkRuntimeTargetRepository repository;
    private final RuntimeTargetValidationGateway validationGateway;
    private final UiMessageService messages;

    @Autowired
    public FlinkRuntimeTargetService(
            FlinkRuntimeTargetRepository repository,
            RuntimeTargetValidationGateway validationGateway,
            UiMessageService messages) {
        this.repository = repository;
        this.validationGateway = validationGateway;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public FlinkRuntimeTargetService(
            FlinkRuntimeTargetRepository repository,
            RuntimeTargetValidationGateway validationGateway) {
        this(repository, validationGateway, UiMessageService.englishFallback());
    }

    public Optional<FlinkRuntimeTarget> findTarget() {
        return repository.findById(SINGLETON_ID);
    }

    public FlinkRuntimeTarget requireTarget() {
        return findTarget()
                .orElseThrow(() -> new IllegalArgumentException(messages.get("runtimeTarget.error.notConfigured")));
    }

    public FlinkRuntimeTarget saveStandalone(SaveStandaloneRuntimeTargetRequest request) {
        if (!isHttpUrl(request.jobManagerUrl())) {
            throw new IllegalArgumentException(messages.get("runtimeTarget.error.jobManagerUrl"));
        }

        FlinkRuntimeTarget target = repository.findById(SINGLETON_ID).orElseGet(this::newTarget);
        clearTarget(target);
        target.setType(RuntimeTargetType.FLINK_STANDALONE);
        target.setJobManagerUrl(request.jobManagerUrl().trim());

        StandaloneValidationResponse validation = validationGateway.validateStandalone(target.getJobManagerUrl());
        if (validation == null || !validation.reachable()) {
            throw new IllegalArgumentException(validation == null
                    ? messages.get("runtimeTarget.error.validationNoResponse")
                    : validation.statusMessage());
        }
        applyStandaloneValidation(target, validation);
        target.setLastValidatedAt(Instant.now());
        return repository.save(target);
    }

    public FlinkRuntimeTarget revalidate() {
        FlinkRuntimeTarget target = requireTarget();
        applyValidationSnapshot(target);
        return repository.save(target);
    }

    private FlinkRuntimeTarget newTarget() {
        FlinkRuntimeTarget target = new FlinkRuntimeTarget();
        target.setId(SINGLETON_ID);
        return target;
    }

    private void clearTarget(FlinkRuntimeTarget target) {
        target.setJobManagerUrl(null);
        target.setStatusMessage(null);
        target.setFlinkVersion(null);
        target.setTaskManagerCount(null);
        target.setTotalSlots(null);
        target.setAvailableSlots(null);
        target.setLastValidatedAt(null);
    }

    private void applyValidationSnapshot(FlinkRuntimeTarget target) {
        StandaloneValidationResponse validation = null;
        String statusMessage = null;
        try {
            validation = validationGateway.validateStandalone(target.getJobManagerUrl());
            statusMessage = validation == null
                    ? messages.get("runtimeTarget.error.validationNoResponse")
                    : validation.statusMessage();
        } catch (RuntimeException ex) {
            statusMessage = ex.getMessage() == null || ex.getMessage().isBlank()
                    ? messages.get("runtimeTarget.error.validationFailed")
                    : ex.getMessage();
        }

        target.setStatus(validation != null && validation.reachable()
                ? RuntimeTargetStatus.CONNECTED
                : RuntimeTargetStatus.UNREACHABLE);
        target.setStatusMessage(statusMessage);
        target.setFlinkVersion(validation == null ? null : validation.flinkVersion());
        target.setTaskManagerCount(validation == null ? null : validation.taskManagerCount());
        target.setTotalSlots(validation == null ? null : validation.totalSlots());
        target.setAvailableSlots(validation == null ? null : validation.availableSlots());
        target.setLastValidatedAt(Instant.now());
    }

    private void applyStandaloneValidation(FlinkRuntimeTarget target, StandaloneValidationResponse validation) {
        target.setStatus(RuntimeTargetStatus.CONNECTED);
        target.setStatusMessage(validation.statusMessage());
        target.setFlinkVersion(validation.flinkVersion());
        target.setTaskManagerCount(validation.taskManagerCount());
        target.setTotalSlots(validation.totalSlots());
        target.setAvailableSlots(validation.availableSlots());
    }

    private boolean isHttpUrl(String value) {
        try {
            URI uri = new URI(value);
            return "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
        } catch (URISyntaxException | NullPointerException ex) {
            return false;
        }
    }

}
