package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.file.HdfsFileSinkConfig;
import com.streamcraft.shared.file.HdfsFileSinkConfigParser;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HdfsFileSinkLifecycleTest {

    @TempDir Path root;

    @Test
    void consecutiveCheckpointsRetainBothFiles() throws Exception {
        var sink = sink(new ArrayList<>(), false, new JobID(), 0);
        sink.invoke(row("one"), null);
        sink.snapshotState(checkpoint(1));
        sink.invoke(row("two"), null);
        sink.snapshotState(checkpoint(2));
        sink.notifyCheckpointComplete(2);
        assertEquals(2, publishedRows().size());
        sink.close();
    }

    @Test
    void abortedCheckpointCarriesFilesIntoLaterSnapshotAndRestore() throws Exception {
        var state = new ArrayList<byte[]>();
        var sink = sink(state, false, new JobID(), 0);
        sink.invoke(row("one"), null);
        sink.snapshotState(checkpoint(1));
        sink.notifyCheckpointAborted(1);
        assertTrue(publishedRows().isEmpty());
        sink.invoke(row("two"), null);
        sink.snapshotState(checkpoint(3));
        sink.close();

        var restored = sink(state, true, new JobID(), 0);
        assertEquals(2, publishedRows().size());
        restored.close();
    }

    @Test
    void abortedCheckpointFilesPublishAfterLaterSuccessWithoutRestart() throws Exception {
        var sink = sink(new ArrayList<>(), false, new JobID(), 0);
        sink.invoke(row("one"), null);
        sink.snapshotState(checkpoint(1));
        sink.notifyCheckpointAborted(1);
        sink.invoke(row("two"), null);
        sink.snapshotState(checkpoint(3));
        sink.notifyCheckpointComplete(3);
        assertEquals(2, publishedRows().size());
        sink.close();
    }

    @Test
    void rescaleRestoresEverySubtaskTransactionWithoutRowReplay() throws Exception {
        JobID job = new JobID();
        var firstState = new ArrayList<byte[]>();
        var secondState = new ArrayList<byte[]>();
        var first = sink(firstState, false, job, 0);
        var second = sink(secondState, false, job, 1);
        first.invoke(row("one"), null);
        second.invoke(row("two"), null);
        first.snapshotState(checkpoint(1));
        second.snapshotState(checkpoint(1));
        first.close();
        second.close();
        firstState.addAll(secondState);

        var restored = sink(firstState, true, job, 0);
        assertEquals(2, publishedRows().size());
        restored.close();
        var restoredAgain = sink(firstState, true, job, 0);
        assertEquals(2, publishedRows().size());
        restoredAgain.close();
    }

    @Test
    void concurrentJobsWithSameTaskNameUseDifferentPreparedDirectories() throws Exception {
        var firstState = new ArrayList<byte[]>();
        var secondState = new ArrayList<byte[]>();
        JobID firstJob = new JobID();
        JobID secondJob = new JobID();
        var first = sink(firstState, false, firstJob, 0);
        var second = sink(secondState, false, secondJob, 0);
        first.invoke(row("one"), null);
        second.invoke(row("two"), null);
        first.snapshotState(checkpoint(1));
        second.snapshotState(checkpoint(1));
        var serializer = new HdfsFileSinkStateSerializer();
        var firstTransaction = serializer.deserialize(1, firstState.get(0)).pendingTransactions().get(0).transaction();
        var secondTransaction = serializer.deserialize(1, secondState.get(0)).pendingTransactions().get(0).transaction();
        assertTrue(firstTransaction.jobId().startsWith(firstJob.toString() + "-"));
        assertTrue(secondTransaction.jobId().startsWith(secondJob.toString() + "-"));
        assertFalse(firstTransaction.equals(secondTransaction));
        first.notifyCheckpointComplete(1);
        second.notifyCheckpointComplete(1);
        assertEquals(2, publishedRows().size());
        first.close();
        second.close();
    }

    @Test
    void finishPublishesBoundedRowsAndIsIdempotent() throws Exception {
        var sink = sink(new ArrayList<>(), false, new JobID(), 0);
        sink.invoke(row("one"), null);
        sink.finish();
        sink.finish();
        sink.close();
        assertEquals(1, publishedRows().size());
    }

    @Test
    void finishPublishesPendingCheckpointsAndTrailingRows() throws Exception {
        var sink = sink(new ArrayList<>(), false, new JobID(), 0);
        sink.invoke(row("one"), null);
        sink.snapshotState(checkpoint(1));
        sink.invoke(row("two"), null);
        sink.finish();
        sink.close();
        assertEquals(2, publishedRows().size());
    }

    @Test
    void cancellationCloseDoesNotPublishUncheckpointedOrPreparedRows() throws Exception {
        var sink = sink(new ArrayList<>(), false, new JobID(), 0);
        sink.invoke(row("one"), null);
        sink.snapshotState(checkpoint(1));
        sink.invoke(row("two"), null);
        sink.close();
        assertTrue(publishedRows().isEmpty());
    }

    private HdfsFileSinkFactory.HdfsFileSinkFunction sink(
            List<byte[]> state, boolean restored, JobID job, int subtask) throws Exception {
        var json = new ObjectMapper().createObjectNode().put("fs.defaultFS", "file:///")
                .put("path", root.resolve("output").toUri().toString())
                .put("tmp_path", root.resolve("staging").toUri().toString())
                .put("file_format_type", "json");
        HdfsFileSinkConfig config = HdfsFileSinkConfigParser.parse(json, IllegalArgumentException::new);
        var constructor = HdfsFileSinkFactory.HdfsFileSinkFunction.class.getDeclaredConstructor(HdfsFileSinkConfig.class);
        constructor.setAccessible(true);
        var sink = constructor.newInstance(config);
        var jobInfo = proxy(JobInfo.class, (method, args) -> switch (method) {
            case "getJobId" -> job;
            case "getJobName" -> "same-job-name";
            default -> null;
        });
        var taskInfo = proxy(TaskInfo.class, (method, args) -> switch (method) {
            case "getIndexOfThisSubtask" -> subtask;
            case "getTaskName" -> "same-task-name";
            default -> null;
        });
        sink.setRuntimeContext(proxy(RuntimeContext.class, (method, args) -> switch (method) {
            case "getJobInfo" -> jobInfo;
            case "getTaskInfo" -> taskInfo;
            default -> null;
        }));
        var listState = proxy(ListState.class, (method, args) -> {
            if (method.equals("get")) return List.copyOf(state);
            if (method.equals("update")) {
                state.clear();
                for (Object entry : (List<?>) args[0]) state.add((byte[]) entry);
            }
            return null;
        });
        var store = proxy(OperatorStateStore.class, (method, args) -> listState);
        sink.initializeState(proxy(FunctionInitializationContext.class, (method, args) -> switch (method) {
            case "isRestored" -> restored;
            case "getRestoredCheckpointId" -> restored ? OptionalLong.of(3) : OptionalLong.empty();
            case "getOperatorStateStore" -> store;
            default -> null;
        }));
        sink.open(null);
        return sink;
    }

    private List<String> publishedRows() throws Exception {
        Path output = root.resolve("output");
        List<String> rows = new ArrayList<>();
        if (Files.exists(output)) {
            try (var files = Files.walk(output)) {
                for (Path path : files.filter(Files::isRegularFile).toList()) rows.addAll(Files.readAllLines(path));
            }
        }
        return rows;
    }

    private static DataEntity row(String id) {
        return new DataEntity(id, 1L, Map.of("id", id), Map.of());
    }

    private static FunctionSnapshotContext checkpoint(long id) {
        return new FunctionSnapshotContext() {
            public long getCheckpointId() { return id; }
            public long getCheckpointTimestamp() { return 0L; }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Call call) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (instance, method, args) -> call.invoke(method.getName(), args));
    }

    private interface Call {
        Object invoke(String method, Object[] args) throws Exception;
    }
}
