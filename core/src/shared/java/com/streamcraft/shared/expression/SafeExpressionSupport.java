package com.streamcraft.shared.expression;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.expression.AccessException;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypeLocator;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public final class SafeExpressionSupport {

    private static final ExpressionParser PARSER = new SpelExpressionParser();
    private static final Pattern FORBIDDEN_TOKEN_PATTERN = Pattern.compile(
            "(?i)(\\bT\\s*\\(|\\bnew\\b|@|\\.class\\b|(^|[^\\w])class($|[^\\w])|"
                    + "\\bgetClass\\s*\\(|\\bforName\\s*\\(|\\bexec\\s*\\(|"
                    + "\\b(?:Runtime|ProcessBuilder|System|ClassLoader)\\b|"
                    + "\\b(?:java|javax|sun)\\.)");
    private static final Set<String> BLOCKED_NODE_TYPES = Set.of(
            "Assign",
            "BeanReference",
            "ConstructorReference",
            "MethodReference",
            "OpDec",
            "OpInc",
            "Projection",
            "Selection",
            "SelectionAll",
            "SelectionFirst",
            "SelectionLast",
            "TypeReference",
            "VariableReference");
    private static final Set<String> ALLOWED_FUNCTIONS = Set.of("abs", "sqrt", "pow", "max", "min");
    private static final TypeLocator DISABLED_TYPE_LOCATOR =
            typeName -> {
                throw new EvaluationException("Type references are disabled.");
            };
    private static final BeanResolver DISABLED_BEAN_RESOLVER =
            (context, beanName) -> {
                throw new EvaluationException("Bean references are disabled.");
            };
    private static final PropertyAccessor MAP_PROPERTY_ACCESSOR = new PropertyAccessor() {
        @Override
        public Class<?>[] getSpecificTargetClasses() {
            return new Class<?>[] {Map.class};
        }

        @Override
        public boolean canRead(EvaluationContext context, Object target, String name) {
            return target instanceof Map<?, ?> targetMap && targetMap.containsKey(name);
        }

        @Override
        public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
            if (!(target instanceof Map<?, ?> targetMap)) {
                throw new AccessException("Target is not a map.");
            }
            return new TypedValue(targetMap.get(name));
        }

        @Override
        public boolean canWrite(EvaluationContext context, Object target, String name) {
            return false;
        }

        @Override
        public void write(EvaluationContext context, Object target, String name, Object newValue)
                throws AccessException {
            throw new AccessException("Write access is disabled.");
        }
    };

    private SafeExpressionSupport() {
    }

    public static void validate(String expressionText, String label) {
        compile(expressionText, label);
    }

    public static CompiledExpression compile(String expressionText, String label) {
        if (expressionText == null || expressionText.isBlank()) {
            throw new IllegalArgumentException(label + " is required.");
        }
        if (FORBIDDEN_TOKEN_PATTERN.matcher(expressionText).find()) {
            throw new IllegalArgumentException(label + " contains unsupported constructs.");
        }

        Expression expression;
        try {
            expression = PARSER.parseExpression(expressionText);
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException(label + " is not a valid expression.", exception);
        }

        if (expression instanceof SpelExpression spelExpression) {
            validateAst(spelExpression.getAST(), label);
        }
        return new CompiledExpression(expression);
    }

    private static void validateAst(SpelNode node, String label) {
        if (node == null) {
            return;
        }

        String nodeType = node.getClass().getSimpleName();
        if ("FunctionReference".equals(nodeType)) {
            if (!isAllowedFunction(node.toStringAST())) {
                throw new IllegalArgumentException(label + " contains unsupported constructs.");
            }
        } else if (BLOCKED_NODE_TYPES.contains(nodeType)) {
            throw new IllegalArgumentException(label + " contains unsupported constructs.");
        }

        for (int index = 0; index < node.getChildCount(); index++) {
            validateAst(node.getChild(index), label);
        }
    }

    private static boolean isAllowedFunction(String ast) {
        for (String functionName : ALLOWED_FUNCTIONS) {
            if (ast.startsWith("#" + functionName + "(")) {
                return true;
            }
        }
        return false;
    }

    private static StandardEvaluationContext createEvaluationContext(Map<String, Object> fields) {
        Map<String, Object> normalizedFields = normalizeFields(fields);
        StandardEvaluationContext context = new StandardEvaluationContext(normalizedFields);
        context.setRootObject(normalizedFields);
        context.addPropertyAccessor(MAP_PROPERTY_ACCESSOR);
        context.setTypeLocator(DISABLED_TYPE_LOCATOR);
        context.setBeanResolver(DISABLED_BEAN_RESOLVER);
        context.setConstructorResolvers(List.of());
        context.setMethodResolvers(List.of());
        normalizedFields.forEach(context::setVariable);
        registerFunctions(context);
        return context;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeFields(Map<String, Object> fields) {
        if (fields == null || fields.isEmpty()) {
            return Map.of();
        }

        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nestedMap) {
                normalized.put(entry.getKey(), normalizeFields((Map<String, Object>) nestedMap));
                continue;
            }
            normalized.put(entry.getKey(), value);
        }
        return normalized;
    }

    private static void registerFunctions(StandardEvaluationContext context) {
        try {
            context.registerFunction("abs", Math.class.getMethod("abs", double.class));
            context.registerFunction("sqrt", Math.class.getMethod("sqrt", double.class));
            context.registerFunction("pow", Math.class.getMethod("pow", double.class, double.class));
            context.registerFunction("max", Math.class.getMethod("max", double.class, double.class));
            context.registerFunction("min", Math.class.getMethod("min", double.class, double.class));
        } catch (NoSuchMethodException exception) {
            throw new IllegalStateException("Unable to initialize safe expression functions.", exception);
        }
    }

    public static final class CompiledExpression {

        private final Expression expression;

        private CompiledExpression(Expression expression) {
            this.expression = expression;
        }

        public Object evaluate(Map<String, Object> fields) {
            return expression.getValue(createEvaluationContext(fields));
        }

        public Boolean evaluateBoolean(Map<String, Object> fields) {
            return expression.getValue(createEvaluationContext(fields), Boolean.class);
        }
    }
}
