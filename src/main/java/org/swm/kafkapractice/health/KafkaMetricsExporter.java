package org.swm.kafkapractice.health;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMetricsExporter {

    private static final List<String> WATCHED_TOPICS = List.of("orders", "orders.DLT");

    private final KafkaAdmin kafkaAdmin;
    private final MeterRegistry meterRegistry;

    private final AtomicInteger urpGauge = new AtomicInteger(0);
    private final AtomicInteger brokerCountGauge = new AtomicInteger(0);

    @PostConstruct
    public void register() {
        meterRegistry.gauge("kafka.cluster.under_replicated_partitions", urpGauge);
        meterRegistry.gauge("kafka.cluster.broker_count", brokerCountGauge);
    }

    @Scheduled(fixedDelay = 5_000)
    public void poll() {
        Properties props = new Properties();
        props.putAll(kafkaAdmin.getConfigurationProperties());

        try (AdminClient client = AdminClient.create(props)) {
            brokerCountGauge.set(
                    client.describeCluster().nodes().get(3, TimeUnit.SECONDS).size());

            DescribeTopicsResult topicsResult = client.describeTopics(WATCHED_TOPICS);
            Map<String, TopicDescription> topics =
                    topicsResult.allTopicNames().get(3, TimeUnit.SECONDS);

            int urp = 0;
            for (TopicDescription topic : topics.values()) {
                for (var partition : topic.partitions()) {
                    if (partition.isr().size() < partition.replicas().size()) {
                        urp++;
                    }
                }
            }
            urpGauge.set(urp);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Metric poll interrupted");
        } catch (Exception e) {
            // 폴링 실패는 게이지 미갱신만 → 알람은 별도
            log.warn("Kafka metric poll failed", e);
        }
    }
}
