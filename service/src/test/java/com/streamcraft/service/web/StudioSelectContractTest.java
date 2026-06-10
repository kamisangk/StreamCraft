package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class StudioSelectContractTest {

    @Test
    void sinkDeliveryGuaranteeSelectExposesAllSupportedGuarantees() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("\"sink-delivery-guarantee\"")
                .contains("NONE:")
                .contains("AT_LEAST_ONCE:")
                .contains("EXACTLY_ONCE:");
    }

    @Test
    void sourceAndSinkAuthSelectsExposeSupportedAuthModesAndScramMechanisms() throws Exception {
        String script = loadScript();

        assertThat(script)
                .containsPattern("(?s)\"source-auth-type\"\\s*:\\s*\\{.*?options\\s*:\\s*\\{\\s*NONE\\s*:\\s*\\{.*?SASL_PLAIN\\s*:\\s*\\{.*?SASL_SCRAM\\s*:\\s*\\{.*?}\\s*}\\s*}")
                .containsPattern("(?s)\"sink-auth-type\"\\s*:\\s*\\{.*?options\\s*:\\s*\\{\\s*NONE\\s*:\\s*\\{.*?SASL_PLAIN\\s*:\\s*\\{.*?SASL_SCRAM\\s*:\\s*\\{.*?}\\s*}\\s*}")
                .containsPattern("(?s)\"source-scram-mechanism\"\\s*:\\s*\\{.*?options\\s*:\\s*\\{\\s*\"SCRAM-SHA-256\"\\s*:\\s*\\{.*?\"SCRAM-SHA-512\"\\s*:\\s*\\{.*?}\\s*}\\s*}")
                .containsPattern("(?s)\"sink-scram-mechanism\"\\s*:\\s*\\{.*?options\\s*:\\s*\\{\\s*\"SCRAM-SHA-256\"\\s*:\\s*\\{.*?\"SCRAM-SHA-512\"\\s*:\\s*\\{.*?}\\s*}\\s*}");
    }

    @Test
    void authSelectDescriptionsUseI18nMessageKeys() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const t = window.StreamCraftI18n?.t")
                .contains("description: t(\"studio.select.auth.none\"")
                .contains("description: t(\"studio.select.auth.plain\"")
                .contains("description: t(\"studio.select.auth.scram\"")
                .contains("description: t(\"studio.select.scram.sha256\"")
                .contains("description: t(\"studio.select.scram.sha512\"")
                .contains("this.searchInput.placeholder = this.select.dataset.selectPlaceholder || this.meta.placeholder || t(\"studio.select.search.placeholder\"")
                .contains("empty.textContent = t(\"studio.select.empty\"");
    }

    private String loadScript() throws IOException {
        ClassPathResource resource = new ClassPathResource("static/js/studio-select.js");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
