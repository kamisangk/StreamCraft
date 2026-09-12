package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

class ElasticsearchSourceSplitPlanningTest {
    @Test
    void createsStableIndexSliceIds() {
        ElasticsearchSourceConfig config = new ElasticsearchSourceConfig(
                List.of("http://localhost:9200"), "orders", List.of(), "{}",
                ElasticsearchSourceConfig.ReadMode.FULL, "", ElasticsearchSourceConfig.CursorType.STRING,
                "", 1000, 100, "1m", 0, "", "", ElasticsearchSourceConfig.AuthType.NONE,
                "", "", "", 3);

        List<ElasticsearchSourceSplit> splits = ElasticsearchSource.splitPlan(config);

        assertEquals(List.of(
                "elasticsearch:orders:slice-0-of-3",
                "elasticsearch:orders:slice-1-of-3",
                "elasticsearch:orders:slice-2-of-3"),
                splits.stream().map(ElasticsearchSourceSplit::splitId).toList());
        assertEquals(List.of(0, 1, 2), splits.stream().map(ElasticsearchSourceSplit::sliceId).toList());
        assertEquals(List.of(3, 3, 3), splits.stream().map(ElasticsearchSourceSplit::sliceMax).toList());
    }

    @Test
    void rejectsInvalidSliceIdentity() {
        ElasticsearchSourceConfig config = new ElasticsearchSourceConfig(
                List.of("http://localhost:9200"), "orders", List.of(), "{}",
                ElasticsearchSourceConfig.ReadMode.FULL, "", ElasticsearchSourceConfig.CursorType.STRING,
                "", 1000, 100, "1m", 0, "", "", ElasticsearchSourceConfig.AuthType.NONE,
                "", "", "", 3);
        IncrementalSourceState state = new IncrementalSourceState("STRING", null, null, null, null);
        assertThrows(IllegalArgumentException.class,
                () -> new ElasticsearchSourceSplit("bad", state, "", 0, 3, 3));
    }
}
