package org.swm.kafkapractice.health;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaClusterHealthIndicator implements HealthIndicator {

    private static final List<String> WATCHED_TOPICS = List.of("orders", "orders.DLT");
    private static final long TIMEOUT_SEC = 3L;

    private final KafkaAdmin kafkaAdmin;

    @Override
    public Health health() {
        Properties adminProps = new Properties();
        adminProps.putAll(kafkaAdmin.getConfigurationProperties());

        try (AdminClient client = AdminClient.create(adminProps)) {
            DescribeClusterResult cluster = client.describeCluster();
            Collection<Node> nodes = cluster.nodes().get(TIMEOUT_SEC, TimeUnit.SECONDS);

            int urp = countUnderReplicated(client);

            Health.Builder builder = (urp == 0) ? Health.up() : Health.outOfService();
            return builder
                    .withDetail("brokerCount", nodes.size())
                    .withDetail("controller",
                            cluster.controller().get(TIMEOUT_SEC, TimeUnit.SECONDS).idString())
                    .withDetail("underReplicatedPartitions", urp)
                    .withDetail("watchedTopics", WATCHED_TOPICS)
                    .build();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down().withException(e).build();
        } catch (Exception e) {
            log.warn("Kafka health check failed", e);
            return Health.down().withException(e).build();
        }
    }

    private int countUnderReplicated(AdminClient client) {
        try {
            DescribeTopicsResult result = client.describeTopics(WATCHED_TOPICS);
            Map<String, TopicDescription> descriptions =
                    result.allTopicNames().get(TIMEOUT_SEC, TimeUnit.SECONDS);

            int urp = 0;
            for (TopicDescription topic : descriptions.values()) {
                for (var partition : topic.partitions()) {
                    if (partition.isr().size() < partition.replicas().size()) {
                        urp++;
                    }
                }
            }
            return urp;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        } catch (Exception e) {
            log.warn("Failed to compute URP", e);
            return -1;
        }
    }
}
