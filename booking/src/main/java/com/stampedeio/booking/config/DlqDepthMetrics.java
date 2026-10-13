package com.stampedeio.booking.config;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * STAM-398 / AC1: dlq_depth as a gauge, not a counter -- nothing consumes
 * payments.events.dlq today (CLAUDE.md §4.5's DLQ is investigate-by-hand),
 * so "depth" is the topic's total record count: sum(latest - earliest)
 * offset across partitions. Returns 0 if the topic doesn't exist yet
 * rather than failing the scrape -- a fresh cluster with no DLQ messages
 * is the healthy state, not an error.
 */
@Configuration
@ConditionalOnProperty(name = "spring.kafka.bootstrap-servers")
public class DlqDepthMetrics {

    private static final String DLQ_TOPIC = "payments.events.dlq";

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public Admin kafkaAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return Admin.create(props);
    }

    @Bean
    public Gauge dlqDepthGauge(Admin kafkaAdminClient, MeterRegistry meterRegistry) {
        return Gauge.builder("dlq_depth", kafkaAdminClient, this::currentDepth)
                .description("Records currently sitting in payments.events.dlq")
                .register(meterRegistry);
    }

    private double currentDepth(Admin admin) {
        try {
            List<TopicPartition> partitions = admin.describeTopics(List.of(DLQ_TOPIC))
                    .topicNameValues().get(DLQ_TOPIC).get(5, TimeUnit.SECONDS)
                    .partitions().stream()
                    .map(p -> new TopicPartition(DLQ_TOPIC, p.partition()))
                    .toList();
            if (partitions.isEmpty()) {
                return 0;
            }

            Map<TopicPartition, OffsetSpec> earliestSpecs = partitions.stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()));
            Map<TopicPartition, OffsetSpec> latestSpecs = partitions.stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

            Map<TopicPartition, ListOffsetsResultInfo> earliest =
                    admin.listOffsets(earliestSpecs).all().get(5, TimeUnit.SECONDS);
            Map<TopicPartition, ListOffsetsResultInfo> latest =
                    admin.listOffsets(latestSpecs).all().get(5, TimeUnit.SECONDS);

            long total = 0;
            for (TopicPartition tp : partitions) {
                total += latest.get(tp).offset() - earliest.get(tp).offset();
            }
            return total;
        } catch (Exception ex) {
            // Topic doesn't exist yet, or Kafka is briefly unreachable --
            // either way 0 is the right scrape value, not a thrown exception
            // that would poison the whole /actuator/prometheus response.
            return 0;
        }
    }
}
