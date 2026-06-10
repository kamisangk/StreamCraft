package com.streamcraft.shared.maskhash;

import java.io.Serializable;
import java.util.List;

public record MaskHashConfig(List<Rule> rules) implements Serializable {

    public MaskHashConfig {
        rules = rules == null ? List.of() : List.copyOf(rules);
    }

    public enum Action {
        MASK,
        HASH
    }

    public enum Algorithm {
        SHA256,
        SHA512,
        MD5
    }

    public record Rule(
            String sourceField,
            String targetField,
            Action action,
            Algorithm algorithm,
            String salt,
            String maskChar,
            int keepFirst,
            int keepLast) implements Serializable {

        public Rule {
            sourceField = sourceField == null ? "" : sourceField.trim();
            targetField = targetField == null || targetField.isBlank() ? sourceField : targetField.trim();
            action = action == null ? Action.MASK : action;
            algorithm = algorithm == null ? Algorithm.SHA256 : algorithm;
            salt = salt == null ? "" : salt;
            maskChar = maskChar == null || maskChar.isBlank() ? "*" : maskChar.substring(0, 1);
            keepFirst = Math.max(0, keepFirst);
            keepLast = Math.max(0, keepLast);
        }
    }
}
