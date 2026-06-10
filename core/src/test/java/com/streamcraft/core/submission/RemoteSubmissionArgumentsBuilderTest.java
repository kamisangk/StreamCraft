package com.streamcraft.core.submission;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.streamcraft.core.cli.CoreCommandLineOptions;
import com.streamcraft.core.runtime.ExecutionMode;
import java.util.List;
import org.junit.jupiter.api.Test;

class RemoteSubmissionArgumentsBuilderTest {

    @Test
    void buildCarriesDefinitionAuthToken() {
        RemoteSubmissionArgumentsBuilder builder = new RemoteSubmissionArgumentsBuilder();

        List<String> args = builder.build(new CoreCommandLineOptions(
                "http://localhost:8080/api/pipelines/1/definition",
                null,
                false,
                "test-internal-token",
                "http://localhost:8081",
                3,
                true,
                null,
                ExecutionMode.RUN,
                null));

        assertTrue(args.contains("--definition-auth-token"));
        assertEquals("test-internal-token", args.get(args.indexOf("--definition-auth-token") + 1));
    }
}
