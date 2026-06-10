package com.streamcraft.core.submission;

import com.streamcraft.core.cli.CoreCommandLineOptions;
import java.util.ArrayList;
import java.util.List;

public class RemoteSubmissionArgumentsBuilder {

    public List<String> build(CoreCommandLineOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("Core command line options are required.");
        }

        List<String> args = new ArrayList<>();
        if (hasText(options.url())) {
            args.add("--url");
            args.add(options.url());
        } else if (hasText(options.file())) {
            args.add("--file");
            args.add(options.file());
        } else {
            throw new IllegalArgumentException("Either --url or --file must be provided for remote submission.");
        }

        args.add("--test-mode");
        args.add(String.valueOf(options.testMode()));

        if (hasText(options.definitionAuthToken())) {
            args.add("--definition-auth-token");
            args.add(options.definitionAuthToken());
        }

        if (options.parallelism() != null) {
            args.add("--parallelism");
            args.add(String.valueOf(options.parallelism()));
        }
        return args;
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
