package com.streamcraft.core.runtime.transform.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.route.RouteConfig;
import com.streamcraft.shared.route.RouteConfigParser;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.LinkedHashMap;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

class RouteTransformFactoryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void evaluationFailureFailsWithRouteAndRecordContext() throws Exception {
        Object processFunction = processFunction("amount > 10", "high");
        Method matches = matchesMethod(processFunction);
        Object route = compiledRoute("high", "amount > 10");

        InvocationTargetException thrown = assertThrows(InvocationTargetException.class,
                () -> matches.invoke(processFunction, route,
                        new DataEntity("record-17", 0L, Map.of("amount", "bad"), Map.of())));

        assertEquals(IllegalArgumentException.class, thrown.getCause().getClass());
        assertTrue(thrown.getCause().getMessage().contains("high"));
        assertTrue(thrown.getCause().getMessage().contains("record-17"));
        assertNotNull(thrown.getCause().getCause());
    }

    @Test
    void validFalseConditionEmitsToDefaultUnmatchedPort() throws Exception {
        RouteConfig config = RouteConfigParser.parse(MAPPER.readTree(
                "{\"routes\":[{\"portId\":\"high\",\"condition\":\"amount > 10\"}]}"),
                IllegalArgumentException::new);
        assertTrue(config.includeUnmatched());
        assertEquals("unmatched", config.unmatchedPort());
        @SuppressWarnings("unchecked")
        ProcessFunction<DataEntity, DataEntity> function =
                (ProcessFunction<DataEntity, DataEntity>) processFunction("amount > 10", "high");
        Map<String, Object> outputs = new LinkedHashMap<>();
        ProcessFunction<DataEntity, DataEntity>.Context context = function.new Context() {
            @Override
            public Long timestamp() {
                return null;
            }

            @Override
            public TimerService timerService() {
                return null;
            }

            @Override
            public <X> void output(OutputTag<X> tag, X value) {
                outputs.put(tag.getId(), value);
            }
        };
        DataEntity entity = new DataEntity("record-18", 0L, Map.of("amount", 3), Map.of());
        function.processElement(entity, context, null);
        assertEquals(Map.of("unmatched", entity), outputs);
    }

    private static Method matchesMethod(Object processFunction) throws Exception {
        Method matches = processFunction.getClass().getDeclaredMethod("matches",
                Class.forName(
                        "com.streamcraft.core.runtime.transform.transforms.RouteTransformFactory$CompiledRouteRule"),
                DataEntity.class);
        matches.setAccessible(true);
        return matches;
    }

    private static Object processFunction(String condition, String portId) throws Exception {
        RouteConfig.RouteRule rule = new RouteConfig.RouteRule(portId, condition);
        Class<?> functionClass = Class.forName(
                "com.streamcraft.core.runtime.transform.transforms.RouteTransformFactory$RouteProcessFunction");
        Constructor<?> constructor = functionClass.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        Object function = constructor.newInstance(RouteConfig.MatchMode.FIRST_MATCH, java.util.List.of(rule), true,
                "unmatched", Map.of("high", new OutputTag<DataEntity>("high") {},
                        "unmatched", new OutputTag<DataEntity>("unmatched") {}));
        var compiledRoutes = functionClass.getDeclaredField("compiledRoutes");
        compiledRoutes.setAccessible(true);
        compiledRoutes.set(function, java.util.List.of(compiledRoute(portId, condition)));
        return function;
    }

    private static Object compiledRoute(String portId, String condition) throws Exception {
        Class<?> compiledRouteClass = Class.forName(
                "com.streamcraft.core.runtime.transform.transforms.RouteTransformFactory$CompiledRouteRule");
        Constructor<?> constructor = compiledRouteClass.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        return constructor.newInstance(portId,
                com.streamcraft.shared.expression.SafeExpressionSupport.compile(condition, "Route condition"));
    }
}
