package com.streamcraft.core.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.streamcraft.core.runtime.ExecutionMode;
import org.junit.jupiter.api.Test;

class CoreCommandLineOptionsTest {

    @Test
    void parseKeepsDefinitionAuthToken() {
        CoreCommandLineOptions options = CoreCommandLineOptions.parse(new String[] {
                "--url", "http://localhost:8080/api/pipelines/1/definition",
                "--test-mode", "false",
                "--definition-auth-token", "test-internal-token",
                "--parallelism", "2"
        });

        assertEquals("test-internal-token", options.definitionAuthToken());
        assertEquals(2, options.parallelism());
        assertEquals(ExecutionMode.RUN, options.executionMode());
        assertNull(options.previewOutputFile());
    }

    @Test
    void parseKeepsPreviewExecutionModeAndOutputFile() {
        CoreCommandLineOptions options = CoreCommandLineOptions.parse(new String[] {
                "--file", "pipeline.json",
                "--execution-mode", "PREVIEW",
                "--preview-output-file", "preview-output.json"
        });

        assertEquals(ExecutionMode.PREVIEW, options.executionMode());
        assertEquals("preview-output.json", options.previewOutputFile());
    }
}
