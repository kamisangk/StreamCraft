package com.streamcraft.core.runtime;

import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import java.util.Objects;

/**
 * The factories used by a pipeline runtime.
 *
 * <p>The bundle keeps runtime construction explicit while leaving the runtime
 * execution code independent from how its collaborators are assembled.</p>
 */
public final class PipelineRuntimeDependencies {

    private final KafkaSourceFactory kafkaSourceFactory;
    private final MockSourceFactory mockSourceFactory;
    private final ElasticsearchSourceFactory elasticsearchSourceFactory;
    private final InfluxDbSourceFactory influxDbSourceFactory;
    private final HdfsFileSourceFactory hdfsFileSourceFactory;
    private final JdbcSourceFactory jdbcSourceFactory;
    private final KafkaSinkFactory kafkaSinkFactory;
    private final JdbcSinkFactory jdbcSinkFactory;
    private final ElasticsearchSinkFactory elasticsearchSinkFactory;
    private final InfluxDbSinkFactory influxDbSinkFactory;
    private final HdfsFileSinkFactory hdfsFileSinkFactory;
    private final TransformOperatorFactory transformFactory;

    private PipelineRuntimeDependencies(Builder builder) {
        this.kafkaSourceFactory = require(builder.kafkaSourceFactory, "kafkaSourceFactory");
        this.mockSourceFactory = require(builder.mockSourceFactory, "mockSourceFactory");
        this.elasticsearchSourceFactory = require(builder.elasticsearchSourceFactory, "elasticsearchSourceFactory");
        this.influxDbSourceFactory = require(builder.influxDbSourceFactory, "influxDbSourceFactory");
        this.hdfsFileSourceFactory = require(builder.hdfsFileSourceFactory, "hdfsFileSourceFactory");
        this.jdbcSourceFactory = require(builder.jdbcSourceFactory, "jdbcSourceFactory");
        this.kafkaSinkFactory = require(builder.kafkaSinkFactory, "kafkaSinkFactory");
        this.jdbcSinkFactory = require(builder.jdbcSinkFactory, "jdbcSinkFactory");
        this.elasticsearchSinkFactory = require(builder.elasticsearchSinkFactory, "elasticsearchSinkFactory");
        this.influxDbSinkFactory = require(builder.influxDbSinkFactory, "influxDbSinkFactory");
        this.hdfsFileSinkFactory = require(builder.hdfsFileSinkFactory, "hdfsFileSinkFactory");
        this.transformFactory = require(builder.transformFactory, "transformFactory");
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Returns the factory set used by the default runtime constructor. */
    public static PipelineRuntimeDependencies defaults() {
        return builder()
                .kafkaSourceFactory(new KafkaSourceFactory())
                .mockSourceFactory(new MockSourceFactory())
                .elasticsearchSourceFactory(new ElasticsearchSourceFactory())
                .influxDbSourceFactory(new InfluxDbSourceFactory())
                .hdfsFileSourceFactory(new HdfsFileSourceFactory())
                .jdbcSourceFactory(new JdbcSourceFactory())
                .kafkaSinkFactory(new KafkaSinkFactory())
                .jdbcSinkFactory(new JdbcSinkFactory())
                .elasticsearchSinkFactory(new ElasticsearchSinkFactory())
                .influxDbSinkFactory(new InfluxDbSinkFactory())
                .hdfsFileSinkFactory(new HdfsFileSinkFactory())
                .transformFactory(new TransformOperatorFactory())
                .build();
    }

    public KafkaSourceFactory kafkaSourceFactory() {
        return kafkaSourceFactory;
    }

    public MockSourceFactory mockSourceFactory() {
        return mockSourceFactory;
    }

    public ElasticsearchSourceFactory elasticsearchSourceFactory() {
        return elasticsearchSourceFactory;
    }

    public InfluxDbSourceFactory influxDbSourceFactory() {
        return influxDbSourceFactory;
    }

    public HdfsFileSourceFactory hdfsFileSourceFactory() {
        return hdfsFileSourceFactory;
    }

    public JdbcSourceFactory jdbcSourceFactory() {
        return jdbcSourceFactory;
    }

    public KafkaSinkFactory kafkaSinkFactory() {
        return kafkaSinkFactory;
    }

    public JdbcSinkFactory jdbcSinkFactory() {
        return jdbcSinkFactory;
    }

    public ElasticsearchSinkFactory elasticsearchSinkFactory() {
        return elasticsearchSinkFactory;
    }

    public InfluxDbSinkFactory influxDbSinkFactory() {
        return influxDbSinkFactory;
    }

    public HdfsFileSinkFactory hdfsFileSinkFactory() {
        return hdfsFileSinkFactory;
    }

    public TransformOperatorFactory transformFactory() {
        return transformFactory;
    }

    private static <T> T require(T value, String name) {
        return Objects.requireNonNull(value, name + " is required");
    }

    public static final class Builder {

        private KafkaSourceFactory kafkaSourceFactory;
        private MockSourceFactory mockSourceFactory;
        private ElasticsearchSourceFactory elasticsearchSourceFactory;
        private InfluxDbSourceFactory influxDbSourceFactory;
        private HdfsFileSourceFactory hdfsFileSourceFactory;
        private JdbcSourceFactory jdbcSourceFactory;
        private KafkaSinkFactory kafkaSinkFactory;
        private JdbcSinkFactory jdbcSinkFactory;
        private ElasticsearchSinkFactory elasticsearchSinkFactory;
        private InfluxDbSinkFactory influxDbSinkFactory;
        private HdfsFileSinkFactory hdfsFileSinkFactory;
        private TransformOperatorFactory transformFactory;

        public Builder kafkaSourceFactory(KafkaSourceFactory value) {
            this.kafkaSourceFactory = value;
            return this;
        }

        public Builder mockSourceFactory(MockSourceFactory value) {
            this.mockSourceFactory = value;
            return this;
        }

        public Builder elasticsearchSourceFactory(ElasticsearchSourceFactory value) {
            this.elasticsearchSourceFactory = value;
            return this;
        }

        public Builder influxDbSourceFactory(InfluxDbSourceFactory value) {
            this.influxDbSourceFactory = value;
            return this;
        }

        public Builder hdfsFileSourceFactory(HdfsFileSourceFactory value) {
            this.hdfsFileSourceFactory = value;
            return this;
        }

        public Builder jdbcSourceFactory(JdbcSourceFactory value) {
            this.jdbcSourceFactory = value;
            return this;
        }

        public Builder kafkaSinkFactory(KafkaSinkFactory value) {
            this.kafkaSinkFactory = value;
            return this;
        }

        public Builder jdbcSinkFactory(JdbcSinkFactory value) {
            this.jdbcSinkFactory = value;
            return this;
        }

        public Builder elasticsearchSinkFactory(ElasticsearchSinkFactory value) {
            this.elasticsearchSinkFactory = value;
            return this;
        }

        public Builder influxDbSinkFactory(InfluxDbSinkFactory value) {
            this.influxDbSinkFactory = value;
            return this;
        }

        public Builder hdfsFileSinkFactory(HdfsFileSinkFactory value) {
            this.hdfsFileSinkFactory = value;
            return this;
        }

        public Builder transformFactory(TransformOperatorFactory value) {
            this.transformFactory = value;
            return this;
        }

        public PipelineRuntimeDependencies build() {
            return new PipelineRuntimeDependencies(this);
        }
    }
}
