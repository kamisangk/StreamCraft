package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.core.runtime.transform.custom.CustomTransform;
import com.streamcraft.core.runtime.transform.custom.CustomTransformContext;
import java.lang.reflect.Constructor;
import java.util.Locale;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCodeTransformFactory implements TransformFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CustomCodeTransformFactory.class);

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        String language = node.config().path("language").asText("JAVA");
        String compilePattern = node.config().path("compilePattern").asText("SOURCE_CODE");
        String className = node.config().path("className").asText();
        String sourceCode = node.config().path("sourceCode").asText();
        ErrorStrategy errorStrategy = ErrorStrategy.from(node.config().path("errorStrategy").asText("KEEP_ORIGINAL"));

        validateConfig(language, compilePattern, className, sourceCode);

        return TransformOutputs.single(input.process(new ProcessFunction<DataEntity, DataEntity>() {
            private static final long serialVersionUID = 1L;
            private transient CustomTransform transform;
            private transient CustomTransformContext context;

            @Override
            public void open(OpenContext openContext) {
                transform = compile(className, sourceCode);
                context = new CustomTransformContext();
            }

            @Override
            public void processElement(
                    DataEntity value,
                    ProcessFunction<DataEntity, DataEntity>.Context processContext,
                    Collector<DataEntity> collector)
                    throws Exception {
                try {
                    DataEntity result = transform.process(value, context);
                    if (result != null) {
                        collector.collect(result);
                    }
                } catch (Exception exception) {
                    switch (errorStrategy) {
                        case KEEP_ORIGINAL -> {
                            LOG.warn(
                                    "Skipping Custom Code transform for record '{}' and keeping original because {}",
                                    value.id(),
                                    exception.getMessage());
                            collector.collect(value);
                        }
                        case SKIP -> LOG.warn(
                                "Skipping Custom Code transform record '{}' because {}",
                                value.id(),
                                exception.getMessage());
                        case FAIL -> throw exception;
                    }
                }
            }
        }).name(node.name()));
    }

    private static void validateConfig(String language, String compilePattern, String className, String sourceCode) {
        if (!"JAVA".equals(normalize(language))) {
            throw new IllegalArgumentException("Custom Code transform only supports JAVA language.");
        }
        if (!"SOURCE_CODE".equals(normalize(compilePattern))) {
            throw new IllegalArgumentException("Custom Code transform only supports SOURCE_CODE compilePattern.");
        }
        if (className == null || className.isBlank()) {
            throw new IllegalArgumentException("Custom Code transform className is required.");
        }
        if (sourceCode == null || sourceCode.isBlank()) {
            throw new IllegalArgumentException("Custom Code transform sourceCode is required.");
        }
    }

    private static CustomTransform compile(String className, String sourceCode) {
        try {
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.setParentClassLoader(Thread.currentThread().getContextClassLoader());
            compiler.cook(sourceCode);
            Class<?> compiledClass = compiler.getClassLoader().loadClass(className);
            if (!CustomTransform.class.isAssignableFrom(compiledClass)) {
                throw new IllegalArgumentException(
                        "Custom Code class must implement " + CustomTransform.class.getName() + ".");
            }
            Constructor<?> constructor = compiledClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return (CustomTransform) constructor.newInstance();
        } catch (Exception exception) {
            throw new IllegalArgumentException("Failed to compile Custom Code transform: " + exception.getMessage(), exception);
        }
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }

    private enum ErrorStrategy {
        KEEP_ORIGINAL,
        SKIP,
        FAIL;

        private static ErrorStrategy from(String value) {
            String normalized = normalize(value);
            for (ErrorStrategy strategy : values()) {
                if (strategy.name().equals(normalized)) {
                    return strategy;
                }
            }
            throw new IllegalArgumentException("Unsupported Custom Code errorStrategy: " + value);
        }
    }
}
