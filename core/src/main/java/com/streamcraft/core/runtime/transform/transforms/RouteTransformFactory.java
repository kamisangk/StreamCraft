package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import com.streamcraft.shared.route.RouteConfig;
import com.streamcraft.shared.route.RouteConfig.MatchMode;
import com.streamcraft.shared.route.RouteConfig.RouteRule;
import com.streamcraft.shared.route.RouteConfigParser;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class RouteTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        RouteConfig config = RouteConfigParser.parse(node.config(), IllegalArgumentException::new);
        Map<String, OutputTag<DataEntity>> tags = new LinkedHashMap<>();
        config.routes().forEach(route -> tags.put(route.portId(), outputTag(node, route.portId())));
        if (config.includeUnmatched()) {
            tags.put(config.unmatchedPort(), outputTag(node, config.unmatchedPort()));
        }

        SingleOutputStreamOperator<DataEntity> mainStream = input
                .process(new RouteProcessFunction(config.matchMode(), config.routes(), config.includeUnmatched(), config.unmatchedPort(), tags))
                .name(node.name());

        Map<String, DataStream<DataEntity>> outputs = new LinkedHashMap<>();
        tags.forEach((portId, tag) -> outputs.put(portId, mainStream.getSideOutput(tag)));
        return new TransformOutputs(outputs);
    }

    private static OutputTag<DataEntity> outputTag(PipelineNode node, String portId) {
        return new OutputTag<>(node.id() + "-" + portId) {
            private static final long serialVersionUID = 1L;
        };
    }

    private static final class RouteProcessFunction extends ProcessFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final MatchMode matchMode;
        private final List<RouteRule> routes;
        private final boolean includeUnmatched;
        private final String unmatchedPort;
        private final Map<String, OutputTag<DataEntity>> tags;
        private transient List<CompiledRouteRule> compiledRoutes;

        private RouteProcessFunction(
                MatchMode matchMode,
                List<RouteRule> routes,
                boolean includeUnmatched,
                String unmatchedPort,
                Map<String, OutputTag<DataEntity>> tags) {
            this.matchMode = matchMode;
            this.routes = routes;
            this.includeUnmatched = includeUnmatched;
            this.unmatchedPort = unmatchedPort;
            this.tags = tags;
        }

        @Override
        public void open(OpenContext openContext) {
            compiledRoutes = routes.stream()
                    .map(route -> new CompiledRouteRule(
                            route.portId(),
                            SafeExpressionSupport.compile(route.condition(), "Route condition")))
                    .toList();
        }

        @Override
        public void processElement(DataEntity entity, Context context, Collector<DataEntity> collector) {
            boolean matched = false;
            for (CompiledRouteRule route : compiledRoutes) {
                if (matches(route, entity)) {
                    context.output(tags.get(route.portId()), entity);
                    matched = true;
                    if (matchMode == MatchMode.FIRST_MATCH) {
                        return;
                    }
                }
            }
            if (!matched && includeUnmatched) {
                context.output(tags.get(unmatchedPort), entity);
            }
        }

        private boolean matches(CompiledRouteRule route, DataEntity entity) {
            try {
                return Boolean.TRUE.equals(route.condition().evaluateBoolean(entity.fields()));
            } catch (Exception exception) {
                return false;
            }
        }
    }

    private record CompiledRouteRule(
            String portId,
            SafeExpressionSupport.CompiledExpression condition) {
    }
}
