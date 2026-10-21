package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class KafkaFetcherLifecycleTest {
    @Test
    void waitsForAssignmentBeforePollingAndClosesWhileIdle() throws Exception {
        var config = KafkaSourceConfig.from(new ObjectMapper().readTree("""
                {"bootstrapServers":"127.0.0.1:1","topics":["input"],
                 "groupId":"idle-fetcher-test","consumeMode":"earliest"}
                """));
        try (var fetcher = new KafkaSourceFetcher(config)) {
            fetcher.start();
            assertThrows(TimeoutException.class, () -> fetcher.availability().get(700, TimeUnit.MILLISECONDS));
            assertDoesNotThrow(fetcher::throwIfFailed);
        }
    }
}
