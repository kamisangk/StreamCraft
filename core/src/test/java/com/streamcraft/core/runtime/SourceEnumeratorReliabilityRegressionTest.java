package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig;
import com.streamcraft.shared.jdbc.JdbcSourceConfig;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.source.*;
import org.junit.jupiter.api.Test;

class SourceEnumeratorReliabilityRegressionTest {
    @Test void esStaggeredRegistrationDistributesAcrossActualParallelism() throws Exception { check("es", 3); }
    @Test void influxStaggeredRegistrationDistributesAcrossActualParallelism() throws Exception { check("influx", 3); }
    @Test void jdbcStaggeredRegistrationDistributesAcrossActualParallelism() throws Exception { check("jdbc", 3); }
    @Test void esUsesActualParallelismWhenFewerReadersThanSplits() throws Exception { check("es", 1); }
    @Test void influxUsesActualParallelismWhenFewerReadersThanSplits() throws Exception { check("influx", 1); }
    @Test void jdbcUsesActualParallelismWhenFewerReadersThanSplits() throws Exception { check("jdbc", 1); }
    @Test void esReturnedSplitWaitsForAnyLiveReader() throws Exception { check("es", 3, true); }
    @Test void influxReturnedSplitWaitsForAnyLiveReader() throws Exception { check("influx", 3, true); }
    @Test void jdbcReturnedSplitWaitsForAnyLiveReader() throws Exception { check("jdbc", 3, true); }
    @Test void esReturnedSplitAvoidsStillRegisteredFailedReader() throws Exception { check("es", 3, false, true); }
    @Test void influxReturnedSplitAvoidsStillRegisteredFailedReader() throws Exception { check("influx", 3, false, true); }
    @Test void jdbcReturnedSplitAvoidsStillRegisteredFailedReader() throws Exception { check("jdbc", 3, false, true); }
    @Test void esReturnedSplitWaitsForReplacementAfterNoMoreSignal() throws Exception { check("es", 3, false, false, true); }
    @Test void influxReturnedSplitWaitsForReplacementAfterNoMoreSignal() throws Exception { check("influx", 3, false, false, true); }
    @Test void jdbcReturnedSplitWaitsForReplacementAfterNoMoreSignal() throws Exception { check("jdbc", 3, false, false, true); }
    @Test void esFiniteIncrementalSignalsAssignmentCompletion() throws Exception { check("es", 1, false, false, false, true); }
    @Test void influxFiniteIncrementalSignalsAssignmentCompletion() throws Exception { check("influx", 1, false, false, false, true); }
    @Test void jdbcFiniteIncrementalSignalsAssignmentCompletion() throws Exception { check("jdbc", 1, false, false, false, true); }

    private void check(String type, int parallelism) throws Exception { check(type, parallelism, false); }
    private void check(String type, int parallelism, boolean delayedRecovery) throws Exception {
        check(type, parallelism, delayedRecovery, false);
    }
    private void check(String type, int parallelism, boolean delayedRecovery, boolean failedStillRegistered) throws Exception {
        check(type, parallelism, delayedRecovery, failedStillRegistered, false);
    }
    private void check(String type, int parallelism, boolean delayedRecovery, boolean failedStillRegistered,
            boolean noMoreRecovery) throws Exception {
        check(type, parallelism, delayedRecovery, failedStillRegistered, noMoreRecovery, false);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void check(String type, int parallelism, boolean delayedRecovery, boolean failedStillRegistered,
            boolean noMoreRecovery, boolean finiteIncremental) throws Exception {
        Map<Integer, Object> registered = new LinkedHashMap<>();
        Map<Integer, List<SourceSplit>> assigned = new LinkedHashMap<>();
        List<Integer> completedAssignments = new ArrayList<>();
        SplitEnumeratorContext context = (SplitEnumeratorContext) Proxy.newProxyInstance(
                SplitEnumeratorContext.class.getClassLoader(), new Class<?>[] {SplitEnumeratorContext.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("registeredReaders")) return registered;
                    if (method.getName().equals("currentParallelism")) return parallelism;
                    if (method.getName().equals("signalNoMoreSplits")) completedAssignments.add((Integer) args[0]);
                    if (method.getName().equals("assignSplits")) ((SplitsAssignment<SourceSplit>) args[0]).assignment()
                            .forEach((id, splits) -> assigned.computeIfAbsent(id, ignored -> new ArrayList<>()).addAll(splits));
                    return null;
                });
        SplitEnumerator enumerator;
        if (type.equals("es")) {
            var config = new ElasticsearchSourceConfig(List.of("http://localhost:9200"), "orders", List.of(), "{}",
                    finiteIncremental ? ElasticsearchSourceConfig.ReadMode.INCREMENTAL : ElasticsearchSourceConfig.ReadMode.FULL,
                    "", ElasticsearchSourceConfig.CursorType.LONG,
                    "", 1, 2, "1m", finiteIncremental ? 1 : 0, "", "", ElasticsearchSourceConfig.AuthType.NONE, "", "", "", 3);
            enumerator = new ElasticsearchSourceEnumerator(context,
                    new ElasticsearchSourceEnumeratorState(ElasticsearchSource.splitPlan(config)), config);
        } else if (type.equals("influx")) {
            var config = new InfluxDbSourceConfig("http://localhost:8086", "db", "select * from cpu", "{}", "ms", 1, 1000,
                    finiteIncremental ? InfluxDbSourceConfig.ReadMode.INCREMENTAL : InfluxDbSourceConfig.ReadMode.FULL,
                    "", InfluxDbSourceConfig.CursorType.LONG, "", 1, finiteIncremental ? 1 : 0, "", "", "", "",
                    List.of("cpu", "mem", "disk"), 1, "", "", "", 0);
            enumerator = new InfluxDbSourceEnumerator(context,
                    new InfluxDbSourceEnumeratorState(InfluxDbSource.splitPlan(config)), config);
        } else {
            var config = new JdbcSourceConfig("jdbc:h2:mem:test", "org.h2.Driver", "", "", "select * from rows", "",
                    finiteIncremental ? JdbcSourceConfig.ReadMode.INCREMENTAL : JdbcSourceConfig.ReadMode.FULL,
                    "", JdbcSourceConfig.CursorType.LONG, "", 1, 2, finiteIncremental ? 1 : 0, "", "",
                    3, "id", "0", "30");
            enumerator = new JdbcSourceEnumerator(context, new JdbcSourceEnumeratorState(JdbcSource.splitPlan(config)), config);
        }
        enumerator.start();
        for (int i = 0; i < parallelism; i++) { registered.put(i, new Object()); enumerator.addReader(i); }
        if (finiteIncremental) {
            assertEquals(List.of(0), completedAssignments, "Finite incremental readers need assignment completion to terminate");
            enumerator.close(); return;
        }
        assertEquals(3, assigned.values().stream().mapToInt(List::size).sum());
        for (int i = 0; i < parallelism; i++) assertEquals(3 / parallelism, assigned.get(i).size());
        int failedId = failedStillRegistered ? 0 : parallelism - 1;
        List<SourceSplit> returned = assigned.remove(failedId);
        if (!failedStillRegistered) registered.remove(failedId);
        if (registered.isEmpty()) registered.put(0, new Object());
        int before = assigned.values().stream().mapToInt(List::size).sum();
        if (delayedRecovery) registered.clear();
        enumerator.addSplitsBack(returned, failedId);
        if (noMoreRecovery) {
            assertEquals(before, assigned.values().stream().mapToInt(List::size).sum(),
                    "Already finished readers cannot accept returned work");
            registered.put(failedId, new Object()); enumerator.addReader(failedId);
        }
        if (!delayedRecovery && !noMoreRecovery && parallelism > 1) enumerator.addReader(failedId == 0 ? 1 : 0);
        if (delayedRecovery || registered.size() == 1 && registered.containsKey(failedId)) {
            registered.put(0, new Object()); enumerator.addReader(0);
        }
        assertEquals(before + returned.size(), assigned.values().stream().mapToInt(List::size).sum(),
                "Returned splits must be assigned to a live reader when their former owner failed");
        if (failedStillRegistered) assertFalse(assigned.containsKey(failedId));
        enumerator.close();
    }
}
