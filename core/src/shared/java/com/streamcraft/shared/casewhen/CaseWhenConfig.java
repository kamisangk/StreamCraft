package com.streamcraft.shared.casewhen;

import java.io.Serializable;
import java.util.List;

public record CaseWhenConfig(
        String targetField,
        List<CaseRule> cases,
        ValueSpec defaultValue) implements Serializable {

    public CaseWhenConfig {
        targetField = targetField == null ? "" : targetField.trim();
        cases = cases == null ? List.of() : List.copyOf(cases);
        defaultValue = defaultValue == null ? ValueSpec.empty() : defaultValue;
    }

    public record CaseRule(String condition, ValueSpec value) implements Serializable {

        public CaseRule {
            condition = condition == null ? "" : condition.trim();
            value = value == null ? ValueSpec.empty() : value;
        }
    }

    public record ValueSpec(String expression, Object literal, boolean hasLiteral) implements Serializable {

        public ValueSpec {
            expression = expression == null ? "" : expression.trim();
        }

        public static ValueSpec expression(String expression) {
            return new ValueSpec(expression, null, false);
        }

        public static ValueSpec literal(Object literal) {
            return new ValueSpec("", literal, true);
        }

        public static ValueSpec empty() {
            return new ValueSpec("", null, false);
        }

        public boolean emptyValue() {
            return expression.isBlank() && !hasLiteral;
        }
    }
}
