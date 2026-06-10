package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class StudioPreviewScriptContractTest {

    @Test
    void scriptPostsCurrentDefinitionToPreviewEndpoint() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("preview-pipeline-button")
                .contains("fetch(\"/api/pipelines/preview\"")
                .contains("definitionJson: JSON.stringify(definition)")
                .contains("state.preview = { outputs: [], error: \"\", running: false }");
    }

    @Test
    void scriptRendersPreviewOutputsIntoScrollablePanels() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("renderPreviewResults()")
                .contains("pipeline-preview-results-panel")
                .contains("pipeline-preview-results-list")
                .contains("overflow-y-auto")
                .contains("output.nodeName")
                .contains("renderPreviewOutputRecords(output.records)")
                .contains("input.textContent = record;")
                .contains("JSON.stringify(record, null, 2)")
                .doesNotContain("JSON.stringify(output.records, null, 2)");
    }

    @Test
    void scriptDoesNotRequireSampleModeCheckboxForPreview() throws Exception {
        String script = loadScript();

        assertThat(script)
                .doesNotContain("source-use-mock")
                .doesNotContain("node.type === \"SOURCE\" && !Boolean(node.config?.useMockSource)");
    }

    @Test
    void scriptTreatsAggregateAsRunnableRuntimeOperator() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"AGGREGATE\"")
                .contains("runnableInRuntime: true");
    }

    private String loadScript() throws IOException {
        ClassPathResource resource = new ClassPathResource("static/js/studio-editor.js");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
