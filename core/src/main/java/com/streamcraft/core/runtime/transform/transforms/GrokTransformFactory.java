package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.fields.FieldPathSupport;
import com.streamcraft.shared.pattern.GrokPatternSupport;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class GrokTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        String inputField = node.config().path("inputField").asText("_streamcraft_message");
        String outputField = node.config().path("outputField").asText();
        String pattern = node.config().path("pattern").asText();

        return TransformOutputs.single(input.map(new RichMapFunction<DataEntity, DataEntity>() {
            private static final long serialVersionUID = 1L;
            private transient GrokPatternSupport.CompiledPattern compiledPattern;

            @Override
            public void open(OpenContext openContext) {
                compiledPattern = GrokPatternSupport.compile(pattern, "Grok pattern");
            }

            @Override
            public DataEntity map(DataEntity entity) {
                FieldPathSupport.Lookup sourceLookup = FieldPathSupport.lookup(entity.fields(), inputField);
                if (!sourceLookup.found() || sourceLookup.value() == null) {
                    return entity;
                }

                Map<String, String> extracted = compiledPattern.extractFirst(String.valueOf(sourceLookup.value()));
                if (extracted.isEmpty()) {
                    return entity;
                }

                DataEntity nextEntity = entity;
                for (Map.Entry<String, String> entry : extracted.entrySet()) {
                    String targetField = outputField == null || outputField.isBlank()
                            ? entry.getKey()
                            : outputField + "." + entry.getKey();
                    nextEntity = nextEntity.withField(targetField, entry.getValue());
                }
                return nextEntity;
            }
        }).name(node.name()));
    }
}
