package com.streamcraft.shared.dataquality;

import java.io.Serializable;
import java.util.List;

public record DataQualityConfig(
        List<Rule> rules,
        Mode mode,
        String errorField) implements Serializable {

    public static final String DEFAULT_ERROR_FIELD = "_streamcraft_quality_errors";

    public DataQualityConfig {
        rules = rules == null ? List.of() : List.copyOf(rules);
        mode = mode == null ? Mode.DIRTY_PORT : mode;
        errorField = errorField == null || errorField.isBlank() ? DEFAULT_ERROR_FIELD : errorField.trim();
    }

    public enum Mode {
        DISCARD,
        MARK_ERROR,
        DIRTY_PORT,
        FAIL
    }

    public enum RuleType {
        NOT_NULL,
        TYPE,
        RANGE,
        LENGTH,
        ENUM,
        REGEX
    }

    public enum ValueType {
        STRING,
        NUMBER,
        INTEGER,
        LONG,
        DOUBLE,
        BOOLEAN,
        ARRAY,
        OBJECT
    }

    public record Rule(
            String field,
            RuleType ruleType,
            ValueType valueType,
            Double min,
            Double max,
            Integer minLength,
            Integer maxLength,
            List<String> enumValues,
            String pattern,
            String customMessage) implements Serializable {

        public Rule {
            field = field == null ? "" : field.trim();
            ruleType = ruleType == null ? RuleType.NOT_NULL : ruleType;
            enumValues = enumValues == null ? List.of() : List.copyOf(enumValues);
            pattern = pattern == null ? "" : pattern.trim();
            customMessage = customMessage == null ? "" : customMessage.trim();
        }
    }
}
