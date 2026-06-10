package com.streamcraft.shared.route;

import java.io.Serializable;
import java.util.List;

public record RouteConfig(
        MatchMode matchMode,
        List<RouteRule> routes,
        boolean includeUnmatched,
        String unmatchedPort) implements Serializable {

    public RouteConfig {
        matchMode = matchMode == null ? MatchMode.FIRST_MATCH : matchMode;
        routes = routes == null ? List.of() : List.copyOf(routes);
        unmatchedPort = unmatchedPort == null || unmatchedPort.isBlank() ? "unmatched" : unmatchedPort.trim();
    }

    public enum MatchMode {
        FIRST_MATCH,
        ALL_MATCHES
    }

    public record RouteRule(String portId, String condition) implements Serializable {

        public RouteRule {
            portId = portId == null ? "" : portId.trim();
            condition = condition == null ? "" : condition.trim();
        }
    }
}
