package com.streamcraft.core.runtime;

import java.io.IOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

final class KafkaAdminMetadataClient implements KafkaMetadataClient {

    private final AdminClient adminClient;

    KafkaAdminMetadataClient(KafkaSourceConfig config) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        KafkaAuthProperties.apply(config, properties);
        adminClient = AdminClient.create(properties);
    }

    @Override
    public List<KafkaPartitionSplit> discover(KafkaSourceConfig config) throws Exception {
        Map<String, TopicDescription> topics = adminClient.describeTopics(config.topics()).allTopicNames().get();
        List<TopicPartition> partitions = topics.values().stream()
                .flatMap(topic -> topic.partitions().stream()
                        .map(partition -> new TopicPartition(topic.name(), partition.partition())))
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .toList();
        Map<TopicPartition, Long> offsets = resolveStartOffsets(config, partitions);
        return partitions.stream()
                .map(partition -> new KafkaPartitionSplit(
                        partition.topic(), partition.partition(), offsets.get(partition)))
                .toList();
    }

    private Map<TopicPartition, Long> resolveStartOffsets(KafkaSourceConfig config, List<TopicPartition> partitions)
            throws Exception {
        if ("committed".equals(config.consumeMode())) {
            Map<TopicPartition, OffsetAndMetadata> committed = adminClient
                    .listConsumerGroupOffsets(config.groupId())
                    .partitionsToOffsetAndMetadata()
                    .get();
            Map<TopicPartition, Long> offsets = new HashMap<>();
            for (TopicPartition partition : partitions) {
                OffsetAndMetadata offset = committed.get(partition);
                if (offset == null) {
                    throw new IllegalStateException(
                            "Kafka Source has no committed offset for group '" + config.groupId()
                                    + "' and partition " + partition + ".");
                }
                offsets.put(partition, offset.offset());
            }
            return offsets;
        }

        OffsetSpec spec = "earliest".equals(config.consumeMode()) ? OffsetSpec.earliest() : OffsetSpec.latest();
        Map<TopicPartition, OffsetSpec> requests = new HashMap<>();
        for (TopicPartition partition : partitions) {
            requests.put(partition, spec);
        }
        Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> result =
                adminClient.listOffsets(requests).all().get();
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (TopicPartition partition : partitions) {
            offsets.put(partition, result.get(partition).offset());
        }
        return offsets;
    }

    @Override
    public void close() throws IOException {
        adminClient.close(Duration.ofSeconds(5));
    }
}
