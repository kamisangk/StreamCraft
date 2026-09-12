package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.streamcraft.shared.influxdb.InfluxDbSourceConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

class InfluxDbSourceSplitPlanningTest {
    @Test
    void createsStableMeasurementSplits() {
        InfluxDbSourceConfig config = new InfluxDbSourceConfig(
                "http://localhost:8086", "metrics", "select * from cpu", "{}", "ms", 30, 1000,
                InfluxDbSourceConfig.ReadMode.FULL, "", InfluxDbSourceConfig.CursorType.STRING, "", 1000, 0,
                "", "", "", "", List.of("cpu", "mem"), 1, "", "", "", 0);
        List<InfluxDbSourceSplit> splits = InfluxDbSource.splitPlan(config);
        assertEquals(List.of("influxdb:measurement:cpu", "influxdb:measurement:mem"),
                splits.stream().map(InfluxDbSourceSplit::splitId).toList());
    }

    @Test
    void createsDeterministicTimeSplits() {
        InfluxDbSourceConfig config = new InfluxDbSourceConfig(
                "http://localhost:8086", "metrics", "select * from cpu", "{}", "ms", 30, 1000,
                InfluxDbSourceConfig.ReadMode.FULL, "time", InfluxDbSourceConfig.CursorType.LONG, "", 1000, 0,
                "", "time", "", "", List.of(), 2, "time", "0", "100", 0);
        List<InfluxDbSourceSplit> splits = InfluxDbSource.splitPlan(config);
        assertEquals(List.of("0", "50"), splits.stream().map(InfluxDbSourceSplit::rangeStart).toList());
        assertEquals(List.of("50", "100"), splits.stream().map(InfluxDbSourceSplit::rangeEnd).toList());
    }
}
