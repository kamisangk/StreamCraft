package com.streamcraft.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.streamcraft.core.cli.CoreCommandLineOptions;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.runtime.ExecutionMode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PipelineDefinitionLoaderTest {

    @Test
    void loadFromUrlIncludesInternalTokenHeader() throws Exception {
        AtomicReference<String> headerValue = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/definition", exchange -> {
            headerValue.set(exchange.getRequestHeaders().getFirst("X-StreamCraft-Internal-Token"));
            byte[] body = """
                    {
                      "pipelineId": "pipeline-1",
                      "nodes": [],
                      "edges": []
                    }
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(body);
            }
        });
        server.start();

        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/definition";
            PipelineDefinitionLoader loader = new PipelineDefinitionLoader();

            PipelineDefinition definition = loader.load(new CoreCommandLineOptions(
                    url,
                    null,
                    false,
                    "test-internal-token",
                    null,
                    null,
                    false,
                    null,
                    ExecutionMode.RUN,
                    null));

            assertNotNull(definition);
            assertEquals("pipeline-1", definition.pipelineId());
            assertEquals("test-internal-token", headerValue.get());
        } finally {
            server.stop(0);
        }
    }
}
