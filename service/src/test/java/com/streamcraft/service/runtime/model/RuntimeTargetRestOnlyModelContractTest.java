package com.streamcraft.service.runtime.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.streamcraft.service.pipeline.client.StopFlinkJobRequest;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobRequest;
import java.lang.reflect.Field;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class RuntimeTargetRestOnlyModelContractTest {

    @Test
    void runtimeTargetTypeContainsStandaloneOnly() {
        assertThat(RuntimeTargetType.values())
                .extracting(Enum::name)
                .containsExactly("FLINK_STANDALONE");
    }

    @Test
    void standaloneSubmissionModeTypeNoLongerExists() {
        assertThatThrownBy(() -> Class.forName("com.streamcraft.service.runtime.model.StandaloneSubmissionMode"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void flinkRuntimeTargetDoesNotStoreCliOrYarnFields() {
        assertThat(Arrays.stream(FlinkRuntimeTarget.class.getDeclaredFields())
                .map(Field::getName))
                .doesNotContain(
                        "standaloneSubmissionMode",
                        "flinkHome",
                        "flinkConfDir",
                        "hadoopConfDir",
                        "yarnQueue",
                        "kerberosPrincipal",
                        "kerberosKeytab",
                        "krb5Conf");
    }

    @Test
    void submitFlinkJobRequestIsRestOnly() {
        assertThat(Arrays.stream(SubmitFlinkJobRequest.class.getRecordComponents())
                .map(RecordComponent::getName))
                .containsExactly("clusterBaseUrl", "pipelineDefinitionUrl", "testMode", "parallelism");
    }

    @Test
    void stopFlinkJobRequestIsRestOnly() {
        assertThat(Arrays.stream(StopFlinkJobRequest.class.getRecordComponents())
                .map(RecordComponent::getName))
                .containsExactly("clusterBaseUrl", "jobId");
    }
}
