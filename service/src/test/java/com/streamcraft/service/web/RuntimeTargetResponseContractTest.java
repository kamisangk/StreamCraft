package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.RuntimeTargetType;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.web.RuntimeTargetResponse;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class RuntimeTargetResponseContractTest {

    @Test
    void runtimeTargetResponseExposesRestRuntimeFieldsOnly() {
        assertThat(Arrays.stream(RuntimeTargetResponse.class.getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName)
                .toList())
                .containsExactly(
                        "configured",
                        "type",
                        "status",
                        "statusMessage",
                        "jobManagerUrl",
                        "flinkVersion",
                        "taskManagerCount",
                        "totalSlots",
                        "availableSlots",
                        "lastValidatedAt")
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
    void standaloneRuntimeTargetResponseMapsRestConnectionDetails() {
        FlinkRuntimeTarget target = new FlinkRuntimeTarget();
        target.setType(RuntimeTargetType.FLINK_STANDALONE);
        target.setStatus(RuntimeTargetStatus.CONNECTED);
        target.setStatusMessage("Flink Standalone cluster is reachable.");
        target.setJobManagerUrl("http://flink:8081");
        target.setFlinkVersion("1.20.0");
        target.setTaskManagerCount(2);
        target.setTotalSlots(8);
        target.setAvailableSlots(6);

        RuntimeTargetResponse response = RuntimeTargetResponse.from(target);

        assertThat(response.configured()).isTrue();
        assertThat(response.type()).isEqualTo("FLINK_STANDALONE");
        assertThat(response.status()).isEqualTo("CONNECTED");
        assertThat(response.statusMessage()).isEqualTo("Flink Standalone cluster is reachable.");
        assertThat(response.jobManagerUrl()).isEqualTo("http://flink:8081");
        assertThat(response.flinkVersion()).isEqualTo("1.20.0");
        assertThat(response.taskManagerCount()).isEqualTo(2);
        assertThat(response.totalSlots()).isEqualTo(8);
        assertThat(response.availableSlots()).isEqualTo(6);
    }
}
