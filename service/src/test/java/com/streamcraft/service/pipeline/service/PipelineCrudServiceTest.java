package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.SavePipelineRequest;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PipelineCrudServiceTest {

    @Mock
    private PipelineRepository repository;

    @Mock
    private PipelineDefinitionService definitionService;

    @Mock
    private PipelineRuntimeStateSupport runtimeStateSupport;

    private PipelineCrudService service;

    @BeforeEach
    void setUp() {
        service = new PipelineCrudService(repository, definitionService, runtimeStateSupport);
    }

    @Test
    void saveNormalizesDefinitionAndPersistsPipelineFields() {
        SavePipelineRequest request = new SavePipelineRequest(
                null,
                "orders",
                "Order pipeline",
                "raw-definition");
        when(definitionService.normalizeAndValidateForSave("raw-definition"))
                .thenReturn("normalized-definition");
        when(repository.save(any(Pipeline.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Pipeline saved = service.save(request);

        assertThat(saved.getName()).isEqualTo("orders");
        assertThat(saved.getDescription()).isEqualTo("Order pipeline");
        assertThat(saved.getDefinitionJson()).isEqualTo("normalized-definition");
        verify(definitionService).normalizeAndValidateForSave("raw-definition");
        verify(repository).save(saved);
    }

    @Test
    void saveUpdatesExistingPipelineWithoutReplacingItsIdentity() {
        Pipeline existing = new Pipeline();
        existing.setId(17L);
        when(repository.findById(17L)).thenReturn(Optional.of(existing));
        when(definitionService.normalizeAndValidateForSave("updated-definition"))
                .thenReturn("normalized-definition");
        when(repository.save(any(Pipeline.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Pipeline saved = service.save(new SavePipelineRequest(
                17L,
                "updated-orders",
                "updated description",
                "updated-definition"));

        assertThat(saved).isSameAs(existing);
        assertThat(saved.getId()).isEqualTo(17L);
        assertThat(saved.getName()).isEqualTo("updated-orders");
        assertThat(saved.getDefinitionJson()).isEqualTo("normalized-definition");
    }

    @Test
    void deleteRejectsPipelineThatIsStillRunning() {
        Pipeline running = new Pipeline();
        running.setId(17L);
        when(repository.findById(17L)).thenReturn(Optional.of(running));
        when(runtimeStateSupport.resolveRuntimeStatus(running)).thenReturn(PipelineRunStatus.RUNNING);

        assertThatThrownBy(() -> service.delete(17L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Pipeline must be stopped before deletion.");
        verify(repository, never()).delete(any(Pipeline.class));
    }

    @Test
    void deleteRemovesPipelineWhenItIsNotRunning() {
        Pipeline stopped = new Pipeline();
        stopped.setId(17L);
        when(repository.findById(17L)).thenReturn(Optional.of(stopped));
        when(runtimeStateSupport.resolveRuntimeStatus(stopped)).thenReturn(PipelineRunStatus.STOPPED);

        service.delete(17L);

        verify(repository).delete(stopped);
    }
}
