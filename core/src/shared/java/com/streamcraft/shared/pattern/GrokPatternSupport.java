package com.streamcraft.shared.pattern;

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.exception.GrokException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class GrokPatternSupport {

    private static final Pattern NAMED_GROUP_PATTERN = Pattern.compile("\\(\\?<([A-Za-z][A-Za-z0-9_]*)>");

    private GrokPatternSupport() {
    }

    public static void validate(String pattern, String label) {
        compile(pattern, label);
    }

    public static CompiledPattern compile(String pattern, String label) {
        if (pattern == null || pattern.isBlank()) {
            throw new IllegalArgumentException(label + " is required.");
        }

        try {
            if (pattern.contains("%{")) {
                return compileGrok(pattern, label);
            }
            return compileRegex(pattern, label);
        } catch (PatternSyntaxException exception) {
            throw new IllegalArgumentException(label + " is not a valid Grok/regex pattern.", exception);
        }
    }

    private static CompiledPattern compileGrok(String pattern, String label) {
        GrokCompiler compiler = GrokCompiler.newInstance();
        try {
            compiler.registerDefaultPatterns();
            Grok grok = compiler.compile(pattern, false);
            if (grok.capture("") == null) {
                throw new IllegalArgumentException(label + " must declare at least one capture field.");
            }
            return new CompiledPattern(grok);
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (GrokException exception) {
            throw new IllegalArgumentException(label + " is not a valid Grok pattern.", exception);
        }
    }

    private static CompiledPattern compileRegex(String pattern, String label) {
        Pattern compiledPattern = Pattern.compile(pattern);
        Matcher matcher = NAMED_GROUP_PATTERN.matcher(pattern);
        List<Capture> captures = new ArrayList<>();
        while (matcher.find()) {
            String groupName = matcher.group(1);
            captures.add(new Capture(groupName, groupName));
        }
        if (captures.isEmpty()) {
            throw new IllegalArgumentException(label + " must declare at least one capture field.");
        }
        return new CompiledPattern(compiledPattern, List.copyOf(captures));
    }

    public static final class CompiledPattern implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Pattern pattern;
        private final List<Capture> captures;
        private final String grokPattern;
        private transient Grok grok;

        private CompiledPattern(Pattern pattern, List<Capture> captures) {
            this.pattern = pattern;
            this.captures = captures;
            this.grokPattern = null;
            this.grok = null;
        }

        private CompiledPattern(Grok grok) {
            this.pattern = null;
            this.captures = List.of();
            this.grokPattern = grok.getOriginalGrokPattern();
            this.grok = grok;
        }

        public Map<String, String> extractFirst(String text) {
            if (grokPattern != null) {
                return extractWithGrok(text);
            }

            Matcher matcher = pattern.matcher(text == null ? "" : text);
            if (!matcher.find()) {
                return Map.of();
            }

            Map<String, String> extracted = new LinkedHashMap<>();
            for (Capture capture : captures) {
                String value = matcher.group(capture.groupName());
                if (value != null) {
                    extracted.put(capture.fieldPath(), value);
                }
            }
            return extracted;
        }

        private Map<String, String> extractWithGrok(String text) {
            ensureGrokInitialized();
            Map<String, Object> capture = grok.capture(text == null ? "" : text);
            if (capture == null || capture.isEmpty()) {
                return Map.of();
            }

            Map<String, String> extracted = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : capture.entrySet()) {
                if (entry.getValue() == null) {
                    continue;
                }
                extracted.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return extracted;
        }

        private void ensureGrokInitialized() {
            if (grok != null) {
                return;
            }
            GrokCompiler compiler = GrokCompiler.newInstance();
            try {
                compiler.registerDefaultPatterns();
                grok = compiler.compile(grokPattern, false);
            } catch (GrokException exception) {
                throw new IllegalStateException("Failed to recompile Grok pattern.", exception);
            }
        }
    }

    private record Capture(String fieldPath, String groupName) implements Serializable {
        private static final long serialVersionUID = 1L;
    }
}
