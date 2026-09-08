package com.streamcraft.core.submission;

import java.io.File;
import java.util.List;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

public class PackagedProgramJobGraphFactory {

    public JobGraph create(String programJarPath,
                           String entryClassName,
                           List<String> programArgs,
                           Configuration configuration,
                           int defaultParallelism) throws Exception {
        return create(programJarPath, entryClassName, programArgs, configuration, defaultParallelism, null);
    }

    public JobGraph create(String programJarPath,
                           String entryClassName,
                           List<String> programArgs,
                           Configuration configuration,
                           int defaultParallelism,
                           String jobId) throws Exception {
        if (programJarPath == null || programJarPath.isBlank()) {
            throw new IllegalArgumentException("Program jar path is required.");
        }
        if (entryClassName == null || entryClassName.isBlank()) {
            throw new IllegalArgumentException("Entry class name is required.");
        }
        if (configuration == null) {
            throw new IllegalArgumentException("Flink configuration is required.");
        }

        try (PackagedProgram packagedProgram = PackagedProgram.newBuilder()
                .setJarFile(new File(programJarPath))
                .setEntryPointClassName(entryClassName)
                .setArguments(programArgs == null ? new String[0] : programArgs.toArray(String[]::new))
                .setConfiguration(configuration)
                .build()) {
            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(
                    packagedProgram, configuration, defaultParallelism, false);
            if (jobId != null && !jobId.isBlank()) {
                jobGraph.setJobID(JobID.fromHexString(jobId));
            }
            return jobGraph;
        }
    }
}
