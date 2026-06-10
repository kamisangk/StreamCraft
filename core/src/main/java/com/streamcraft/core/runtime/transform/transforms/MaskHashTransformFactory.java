package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.fields.FieldPathSupport;
import com.streamcraft.shared.maskhash.MaskHashConfig;
import com.streamcraft.shared.maskhash.MaskHashConfig.Action;
import com.streamcraft.shared.maskhash.MaskHashConfig.Rule;
import com.streamcraft.shared.maskhash.MaskHashConfigParser;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class MaskHashTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        MaskHashConfig config = MaskHashConfigParser.parse(node.config(), IllegalArgumentException::new);
        return TransformOutputs.single(input
                .map(new MaskHashMapFunction(config.rules()))
                .name(node.name()));
    }

    private static final class MaskHashMapFunction extends RichMapFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final List<Rule> rules;

        private MaskHashMapFunction(List<Rule> rules) {
            this.rules = rules;
        }

        @Override
        public DataEntity map(DataEntity entity) {
            DataEntity result = entity;
            for (Rule rule : rules) {
                FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(result.fields(), rule.sourceField());
                if (!lookup.found() || lookup.value() == null) {
                    continue;
                }

                String value = String.valueOf(lookup.value());
                Object transformed = rule.action() == Action.HASH
                        ? hash(value, rule)
                        : mask(value, rule);
                result = result.withField(rule.targetField(), transformed);
            }
            return result;
        }

        private String mask(String value, Rule rule) {
            int length = value.length();
            int keepFirst = Math.min(rule.keepFirst(), length);
            int keepLast = Math.min(rule.keepLast(), Math.max(0, length - keepFirst));
            int maskLength = Math.max(0, length - keepFirst - keepLast);
            return value.substring(0, keepFirst)
                    + rule.maskChar().repeat(maskLength)
                    + value.substring(length - keepLast);
        }

        private String hash(String value, Rule rule) {
            try {
                MessageDigest digest = MessageDigest.getInstance(algorithmName(rule));
                byte[] bytes = digest.digest((rule.salt() + value).getBytes(StandardCharsets.UTF_8));
                return HexFormat.of().formatHex(bytes);
            } catch (Exception exception) {
                throw new IllegalArgumentException("Unable to hash field " + rule.sourceField(), exception);
            }
        }

        private String algorithmName(Rule rule) {
            return switch (rule.algorithm()) {
                case SHA256 -> "SHA-256";
                case SHA512 -> "SHA-512";
                case MD5 -> "MD5";
            };
        }
    }
}
