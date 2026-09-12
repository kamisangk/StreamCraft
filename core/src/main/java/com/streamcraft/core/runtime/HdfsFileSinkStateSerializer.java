package com.streamcraft.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class HdfsFileSinkStateSerializer implements SimpleVersionedSerializer<HdfsFileSinkState> {

    private static final int VERSION = 1;
    private static final int MAX_ENTRIES = 100_000;
    private static final int MAX_STRING_BYTES = 1_000_000;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(HdfsFileSinkState state) throws IOException {
        HdfsFileSinkState safeState = state == null ? new HdfsFileSinkState(List.of()) : state;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(VERSION);
            writeCount(output, safeState.pendingTransactions().size(), "transaction");
            for (HdfsFileSinkCommittable committable : safeState.pendingTransactions()) {
                writeTransaction(output, committable.transaction());
                writeCount(output, committable.files().size(), "file");
                for (HdfsFileSinkCommittable.FileCommit file : committable.files()) {
                    writeTransaction(output, file.transaction());
                    writeString(output, file.temporaryPath());
                    writeString(output, file.finalPath());
                    writeString(output, file.partition());
                }
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public HdfsFileSinkState deserialize(int version, byte[] data) throws IOException {
        if (version != VERSION || data == null) {
            throw new IOException("Invalid HDFS sink state version.");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(data))) {
            if (input.readInt() != VERSION) {
                throw new IOException("Invalid HDFS sink state marker.");
            }
            int transactionCount = readCount(input, "transaction");
            List<HdfsFileSinkCommittable> transactions = new ArrayList<>(transactionCount);
            for (int index = 0; index < transactionCount; index++) {
                HdfsFileSinkTransaction transaction = readTransaction(input);
                int fileCount = readCount(input, "file");
                List<HdfsFileSinkCommittable.FileCommit> files = new ArrayList<>(fileCount);
                for (int fileIndex = 0; fileIndex < fileCount; fileIndex++) {
                    HdfsFileSinkTransaction fileTransaction = readTransaction(input);
                    files.add(new HdfsFileSinkCommittable.FileCommit(
                            fileTransaction,
                            readString(input, "temporary path"),
                            readString(input, "final path"),
                            readString(input, "partition")));
                }
                transactions.add(new HdfsFileSinkCommittable(transaction, files));
            }
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in HDFS sink state.");
            }
            return new HdfsFileSinkState(transactions);
        } catch (IllegalArgumentException | EOFException exception) {
            throw new IOException("Invalid HDFS sink state.", exception);
        }
    }

    private static void writeTransaction(DataOutputStream output, HdfsFileSinkTransaction transaction)
            throws IOException {
        if (transaction == null) {
            throw new IOException("HDFS sink transaction must not be null.");
        }
        writeString(output, transaction.jobId());
        output.writeInt(transaction.subtaskId());
        output.writeLong(transaction.checkpointId());
    }

    private static HdfsFileSinkTransaction readTransaction(DataInputStream input) throws IOException {
        return new HdfsFileSinkTransaction(readString(input, "job id"), input.readInt(), input.readLong());
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new IOException("HDFS sink state string is too large.");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(DataInputStream input, String field) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > MAX_STRING_BYTES || length > input.available()) {
            throw new IOException("Invalid HDFS sink state " + field + " length.");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException("Truncated HDFS sink state " + field + ".");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeCount(DataOutputStream output, int count, String label) throws IOException {
        if (count < 0 || count > MAX_ENTRIES) {
            throw new IOException("Invalid HDFS sink state " + label + " count.");
        }
        output.writeInt(count);
    }

    private static int readCount(DataInputStream input, String label) throws IOException {
        int count = input.readInt();
        if (count < 0 || count > MAX_ENTRIES) {
            throw new IOException("Invalid HDFS sink state " + label + " count.");
        }
        return count;
    }
}
