package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.streamcraft.shared.jdbc.JdbcSourceConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

class JdbcSourceSplitPlanningTest {

    @Test
    void createsDeterministicHalfOpenNumericRanges() {
        JdbcSourceConfig config = new JdbcSourceConfig(
                "jdbc:h2:mem:split",
                "org.h2.Driver",
                "",
                "",
                "select id, payload from events",
                "",
                JdbcSourceConfig.ReadMode.FULL,
                "",
                JdbcSourceConfig.CursorType.LONG,
                "",
                1000,
                100,
                0,
                "id",
                "",
                4,
                "id",
                "0",
                "100");

        List<JdbcSourceSplit> splits = JdbcSource.splitPlan(config);

        assertEquals(List.of("jdbc:id:0/4", "jdbc:id:1/4", "jdbc:id:2/4", "jdbc:id:3/4"),
                splits.stream().map(JdbcSourceSplit::splitId).toList());
        assertIterableEquals(List.of("0", "25", "50", "75"), splits.stream().map(JdbcSourceSplit::lowerBound).toList());
        assertIterableEquals(List.of("25", "50", "75", "100"), splits.stream().map(JdbcSourceSplit::upperBound).toList());
        assertEquals(true, splits.get(3).upperInclusive());
        assertEquals(false, splits.get(0).upperInclusive());
    }

    @Test
    void fallsBackToOneSplitWhenRangeCapabilityIsNotProven() {
        JdbcSourceConfig config = new JdbcSourceConfig(
                "jdbc:h2:mem:split",
                "org.h2.Driver",
                "",
                "",
                "select id from events",
                "",
                JdbcSourceConfig.ReadMode.FULL,
                "",
                JdbcSourceConfig.CursorType.STRING,
                "",
                1000,
                100,
                0,
                "id",
                "",
                4,
                "",
                "",
                "");

        List<JdbcSourceSplit> splits = JdbcSource.splitPlan(config);

        assertEquals(1, splits.size());
        assertEquals("jdbc-source", splits.get(0).splitId());
    }

    @Test
    void rejectsInvalidExplicitRange() {
        JdbcSourceConfig config = new JdbcSourceConfig(
                "jdbc:h2:mem:split",
                "org.h2.Driver",
                "",
                "",
                "select id from events",
                "",
                JdbcSourceConfig.ReadMode.FULL,
                "",
                JdbcSourceConfig.CursorType.LONG,
                "",
                1000,
                100,
                0,
                "id",
                "",
                2,
                "id",
                "100",
                "0");

        assertThrows(IllegalArgumentException.class, () -> JdbcSource.splitPlan(config));
    }
}
