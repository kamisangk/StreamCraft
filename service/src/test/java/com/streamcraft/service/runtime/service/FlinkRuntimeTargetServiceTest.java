package com.streamcraft.service.runtime.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.streamcraft.service.runtime.client.RuntimeTargetValidationGateway;
import com.streamcraft.service.runtime.client.StandaloneValidationResponse;
import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.RuntimeTargetType;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.persistence.FlinkRuntimeTargetRepository;
import com.streamcraft.service.runtime.web.SaveStandaloneRuntimeTargetRequest;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FlinkRuntimeTargetServiceTest {

    @Mock
    private FlinkRuntimeTargetRepository repository;

    @Mock
    private RuntimeTargetValidationGateway validationGateway;

    @InjectMocks
    private FlinkRuntimeTargetService service;

    @Test
    void saveStandaloneStoresSingletonTargetAndValidatesRestEndpoint() {
        when(repository.findById(FlinkRuntimeTargetService.SINGLETON_ID)).thenReturn(Optional.empty());
        when(validationGateway.validateStandalone("http://flink:8081"))
                .thenReturn(new StandaloneValidationResponse(
                        true,
                        "CONNECTED",
                        "Flink Standalone cluster is reachable.",
                        "1.20.0",
                        2,
                        8,
                        6));
        ArgumentCaptor<FlinkRuntimeTarget> captor = ArgumentCaptor.forClass(FlinkRuntimeTarget.class);
        when(repository.save(captor.capture())).thenAnswer(invocation -> invocation.getArgument(0));

        FlinkRuntimeTarget saved = service.saveStandalone(standaloneRequest("http://flink:8081"));

        assertThat(saved.getId()).isEqualTo(FlinkRuntimeTargetService.SINGLETON_ID);
        assertThat(saved.getType()).isEqualTo(RuntimeTargetType.FLINK_STANDALONE);
        assertThat(saved.getStatus()).isEqualTo(RuntimeTargetStatus.CONNECTED);
        assertThat(saved.getJobManagerUrl()).isEqualTo("http://flink:8081");
        assertThat(captor.getValue().getId()).isEqualTo(FlinkRuntimeTargetService.SINGLETON_ID);
    }

    @Test
    void saveStandaloneRequestContainsJobManagerUrlOnly() {
        assertThat(Arrays.stream(SaveStandaloneRuntimeTargetRequest.class.getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName)
                .toList())
                .containsExactly("jobManagerUrl");
    }

    @Test
    void runtimeTargetServiceDoesNotExposeYarnSaveOperation() {
        assertThat(Arrays.stream(FlinkRuntimeTargetService.class.getDeclaredMethods())
                .map(java.lang.reflect.Method::getName)
                .toList())
                .doesNotContain("saveYarnApplication");
    }

    @Test
    void findTargetReturnsEmptyWhenSingletonHasNotBeenConfigured() {
        when(repository.findById(FlinkRuntimeTargetService.SINGLETON_ID)).thenReturn(Optional.empty());

        assertThat(service.findTarget()).isEmpty();
    }

    private SaveStandaloneRuntimeTargetRequest standaloneRequest(String jobManagerUrl) {
        return new SaveStandaloneRuntimeTargetRequest(jobManagerUrl);
    }
}
