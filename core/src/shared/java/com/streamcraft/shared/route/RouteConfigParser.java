package com.streamcraft.shared.route;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import com.streamcraft.shared.route.RouteConfig.MatchMode;
import com.streamcraft.shared.route.RouteConfig.RouteRule;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

public final class RouteConfigParser {

    private RouteConfigParser() {
    }

    public static RouteConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static RouteConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        MatchMode matchMode = parseEnum(text(safeConfig, "matchMode", "FIRST_MATCH"), MatchMode.class, "matchMode", error);
        boolean includeUnmatched = booleanValue(safeConfig, "includeUnmatched", true);
        String unmatchedPort = text(safeConfig, "unmatchedPort", "unmatched");
        validatePortId(unmatchedPort, error);
        List<RouteRule> routes = routes(safeConfig, error);
        if (routes.isEmpty()) {
            throw error.apply(ValidationError.routesRequired());
        }
        return new RouteConfig(matchMode, routes, includeUnmatched, unmatchedPort);
    }

    public static Set<String> outputPorts(JsonNode config, Function<String, IllegalArgumentException> error) {
        RouteConfig routeConfig = parse(config, error);
        Set<String> ports = new HashSet<>();
        routeConfig.routes().forEach(route -> ports.add(route.portId()));
        if (routeConfig.includeUnmatched()) {
            ports.add(routeConfig.unmatchedPort());
        }
        return Set.copyOf(ports);
    }

    private static List<RouteRule> routes(JsonNode config, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path("routes");
        if (value == null || !value.isArray()) {
            return List.of();
        }
        List<RouteRule> result = new ArrayList<>();
        Set<String> portIds = new HashSet<>();
        for (JsonNode item : value) {
            String portId = text(item, "portId", "");
            String condition = text(item, "condition", "");
            validatePortId(portId, error);
            if (!portIds.add(portId)) {
                throw error.apply(ValidationError.portIdUnique(portId));
            }
            if (condition.isBlank()) {
                throw error.apply(ValidationError.conditionRequired());
            }
            SafeExpressionSupport.validate(condition, "Route condition");
            result.add(new RouteRule(portId, condition));
        }
        return result;
    }

    private static void validatePortId(String portId, ValidationErrorFactory error) {
        if (portId == null || !portId.matches("[A-Za-z0-9_-]+")) {
            throw error.apply(ValidationError.invalidPortId(portId));
        }
    }

    private static boolean booleanValue(JsonNode config, String fieldName, boolean fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        return value == null || value.isMissingNode() || value.isNull() ? fallback : value.asBoolean(fallback);
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
    }

    private static <T extends Enum<T>> T parseEnum(
            String value,
            Class<T> enumType,
            String fieldName,
            ValidationErrorFactory error) {
        try {
            return Enum.valueOf(enumType, value.trim().toUpperCase(Locale.ROOT));
        } catch (Exception exception) {
            throw error.apply(ValidationError.unsupportedValue(fieldName, value));
        }
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }

    public record ValidationError(String messageKey, String defaultMessage, Object... args) {

        static ValidationError routesRequired() {
            return new ValidationError(
                    "pipeline.validation.route.routesRequired",
                    "Route config routes must contain at least one item.");
        }

        static ValidationError conditionRequired() {
            return new ValidationError(
                    "pipeline.validation.route.conditionRequired",
                    "Route config condition is required.");
        }

        static ValidationError invalidPortId(String portId) {
            return new ValidationError(
                    "pipeline.validation.route.invalidPortId",
                    "Route config portId is invalid: " + portId,
                    portId);
        }

        static ValidationError portIdUnique(String portId) {
            return new ValidationError(
                    "pipeline.validation.route.portIdUnique",
                    "Route config portId must be unique: " + portId,
                    portId);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.route.unsupportedValue",
                    "Route config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
