package com.streamcraft.core.runtime.transform;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.transform.transforms.AggregateTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.CaseWhenTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.CastTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.CustomCodeTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.DataQualityTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.DeduplicateTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.DeserializeTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.ExplodeTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.EvalTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.FilterTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.LookupEnrichTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.LookupJoinTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.MaskHashTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.GrokTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.FlattenTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.PruneTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.PutTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.RenameTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.RouteTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.SerializeTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.StreamJoinTransformFactory;
import com.streamcraft.core.runtime.transform.transforms.TimeDeriveTransformFactory;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TransformOperatorFactory {

    private static final Map<PipelineOperator, TransformFactory> DEFAULT_FACTORIES = createDefaultFactories();

    private final Map<PipelineOperator, TransformFactory> factories;

    public TransformOperatorFactory() {
        this(DEFAULT_FACTORIES);
    }

    TransformOperatorFactory(Map<PipelineOperator, TransformFactory> factories) {
        this.factories = factories;
    }

    public static Set<PipelineOperator> supportedOperators() {
        return DEFAULT_FACTORIES.keySet();
    }

    public TransformOutputs apply(Map<String, DataStream<DataEntity>> inputsByPort, PipelineNode node) {
        TransformFactory factory = factories.get(node.operator());
        if (factory == null) {
            throw new IllegalArgumentException("Unsupported transform operator: " + node.operator());
        }
        return factory.apply(inputsByPort, node);
    }

    private static Map<PipelineOperator, TransformFactory> createDefaultFactories() {
        Map<PipelineOperator, TransformFactory> factories = new EnumMap<>(PipelineOperator.class);
        factories.put(PipelineOperator.PUT, new PutTransformFactory());
        factories.put(PipelineOperator.PRUNE, new PruneTransformFactory());
        factories.put(PipelineOperator.RENAME, new RenameTransformFactory());
        factories.put(PipelineOperator.DESERIALIZE, new DeserializeTransformFactory());
        factories.put(PipelineOperator.SERIALIZE, new SerializeTransformFactory());
        factories.put(PipelineOperator.FILTER, new FilterTransformFactory());
        factories.put(PipelineOperator.GROK, new GrokTransformFactory());
        factories.put(PipelineOperator.CAST, new CastTransformFactory());
        factories.put(PipelineOperator.EVAL, new EvalTransformFactory());
        factories.put(PipelineOperator.CUSTOM_CODE, new CustomCodeTransformFactory());
        factories.put(PipelineOperator.AGGREGATE, new AggregateTransformFactory());
        factories.put(PipelineOperator.DEDUPLICATE, new DeduplicateTransformFactory());
        factories.put(PipelineOperator.LOOKUP_ENRICH, new LookupEnrichTransformFactory());
        factories.put(PipelineOperator.LOOKUP_JOIN, new LookupJoinTransformFactory());
        factories.put(PipelineOperator.STREAM_JOIN, new StreamJoinTransformFactory());
        factories.put(PipelineOperator.FLATTEN, new FlattenTransformFactory());
        factories.put(PipelineOperator.EXPLODE, new ExplodeTransformFactory());
        factories.put(PipelineOperator.DATA_QUALITY, new DataQualityTransformFactory());
        factories.put(PipelineOperator.TIME_DERIVE, new TimeDeriveTransformFactory());
        factories.put(PipelineOperator.MASK_HASH, new MaskHashTransformFactory());
        factories.put(PipelineOperator.CASE_WHEN, new CaseWhenTransformFactory());
        factories.put(PipelineOperator.ROUTE, new RouteTransformFactory());
        return Map.copyOf(factories);
    }
}
