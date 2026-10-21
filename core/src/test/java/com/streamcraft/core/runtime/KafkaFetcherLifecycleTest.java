package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class KafkaFetcherLifecycleTest {
    @Test
    void waitsForAssignmentBeforePollingAndClosesWhileIdle() throws Exception {
        var config = KafkaSourceConfig.from(new ObjectMapper().readTree("""
                {"bootstrapServers":"127.0.0.1:1","topics":["input"],
                 "groupId":"idle-fetcher-test","consumeMode":"earliest"}
                """));
        try (var fetcher = new KafkaSourceFetcher(config)) {
            fetcher.start();
            assertThrows(TimeoutException.class, () -> fetcher.availability().get(700, TimeUnit.MILLISECONDS));
            assertDoesNotThrow(fetcher::throwIfFailed);
        }
    }

    @Test
    @Timeout(90)
    void liveBackpressureBoundsQueueAndDrainsEveryOffsetWhileCommitsProgress() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("streamcraft.kafka.integration"),
                "Set streamcraft.kafka.integration=true to run the live backpressure regression.");
        String bootstrap = System.getProperty("streamcraft.kafka.bootstrapServers", "192.168.31.191:9092");
        String run = UUID.randomUUID().toString().replace("-", "");
        String topic = "streamcraft_fetcher_backpressure_" + run;
        String group = "streamcraft_fetcher_backpressure_group_" + run;
        var properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        properties.setProperty("default.api.timeout.ms", "10000");
        properties.setProperty("request.timeout.ms", "10000");
        try (AdminClient admin = AdminClient.create(properties)) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
                var producerProperties = new Properties();
                producerProperties.putAll(properties);
                producerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                producerProperties.setProperty("max.block.ms", "10000");
                producerProperties.setProperty("delivery.timeout.ms", "15000");
                try (var producer = new KafkaProducer<String, String>(producerProperties)) {
                    var sends = new ArrayList<java.util.concurrent.Future<?>>();
                    for (int offset = 0; offset < 3_000; offset++) {
                        sends.add(producer.send(new ProducerRecord<>(topic, "key", Integer.toString(offset))));
                    }
                    producer.flush();
                    for (var send : sends) send.get(10, TimeUnit.SECONDS);
                }
                var json = new ObjectMapper().createObjectNode().put("bootstrapServers", bootstrap)
                        .put("groupId", group).put("consumeMode", "earliest").put("commitOnCheckpoint", true);
                json.putArray("topics").add(topic);
                var partition = new TopicPartition(topic, 0);
                try (var fetcher = new KafkaSourceFetcher(KafkaSourceConfig.from(json))) {
                    var queueField = KafkaSourceFetcher.class.getDeclaredField("records");
                    queueField.setAccessible(true);
                    Queue<?> queue = (Queue<?>) queueField.get(fetcher);
                    fetcher.start();
                    fetcher.addSplits(List.of(new KafkaPartitionSplit(topic, 0, 0)));
                    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                    while (queue.size() <= 1_500 && System.nanoTime() < deadline) {
                        fetcher.throwIfFailed();
                        Thread.sleep(25);
                    }
                    assertTrue(queue.size() > 1_500, "Queue must reach the pause threshold without draining");
                    Thread.sleep(750);
                    assertTrue(queue.size() <= 2_000, "Undrained fetch queue must stay bounded");
                    fetcher.throwIfFailed();
                    int pausedSize = queue.size();

                    // Offset zero is safe before emission; this probes callbacks while fetching is paused.
                    fetcher.submitCheckpointCommit(1, Map.of(partition, new OffsetAndMetadata(0)));
                    KafkaSourceFetcher.CommitResult commit = null;
                    deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                    while (commit == null && System.nanoTime() < deadline) {
                        fetcher.throwIfFailed();
                        commit = fetcher.pollCommitResult();
                        if (commit == null) Thread.sleep(25);
                    }
                    assertEquals(new KafkaSourceFetcher.CommitResult(1, true), commit);
                    assertEquals(0L, admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                            .get(10, TimeUnit.SECONDS).get(partition).offset());

                    var offsets = new HashSet<Long>();
                    int total = 0;
                    deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                    while (total < 3_000 && System.nanoTime() < deadline) {
                        fetcher.throwIfFailed();
                        assertTrue(queue.size() <= 2_000);
                        var record = fetcher.pollRecord();
                        if (record == null) {
                            Thread.sleep(10);
                            continue;
                        }
                        assertEquals(total, record.offset());
                        assertEquals(Integer.toString(total), record.value());
                        assertEquals(topic, record.topic());
                        assertEquals(0, record.partition());
                        assertTrue(offsets.add(record.offset()), "Offset must be delivered once");
                        total++;
                    }
                    assertEquals(3_000, total);
                    assertEquals(3_000, offsets.size());
                    assertDoesNotThrow(fetcher::throwIfFailed);
                    System.out.println("Live Kafka backpressure: pausedQueue=" + pausedSize
                            + ", total=" + total + ", uniqueOffsets=" + offsets.size() + ", pausedCommit=true");
                }
            } finally {
                try {
                    admin.deleteTopics(List.of(topic)).all().get(15, TimeUnit.SECONDS);
                } finally {
                    try {
                        admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
                    } catch (java.util.concurrent.ExecutionException exception) {
                        if (!(exception.getCause() instanceof GroupIdNotFoundException)) throw exception;
                    }
                }
                System.out.println("Deleted own Kafka backpressure topic " + topic + " and group " + group);
            }
        }
    }
}
