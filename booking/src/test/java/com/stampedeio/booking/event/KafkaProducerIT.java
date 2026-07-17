package com.stampedeio.booking.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.stampedeio.booking.catalog.CatalogClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@Tag("integration")
@Import(KafkaProducerIT.StubCatalogConfig.class)
class KafkaProducerIT {

    private static final String TOPIC = "reservations.events";

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static org.testcontainers.containers.KafkaContainer kafka =
            new org.testcontainers.containers.KafkaContainer(
                    DockerImageName.parse("confluentinc/cp-kafka:7.9.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    @DisplayName("AC4: hello-world producer publishes EventEnvelope to reservations.events and consumer reads it back")
    void publish_event_envelope_to_reservations_events() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();

        EventEnvelope envelope = EventEnvelope.create(
                "ReservationCreated",
                1,
                correlationId,
                reservationId,
                Map.of("showId", showId.toString(), "seats", 2)
        );

        kafkaTemplate.send(TOPIC, showId.toString(), envelope).get();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "catalog.reservations-projector",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        ))) {
            consumer.subscribe(java.util.List.of(TOPIC));

            ConsumerRecords<String, String> records = ConsumerRecords.empty();
            long deadline = System.currentTimeMillis() + 5_000;
            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                records = consumer.poll(Duration.ofMillis(500));
            }

            assertThat(records).isNotEmpty();

            var record = records.iterator().next();
            assertThat(record.key()).isEqualTo(showId.toString());
            assertThat(record.value()).contains("ReservationCreated");
            assertThat(record.value()).contains(reservationId.toString());
            assertThat(record.value()).contains(correlationId.toString());
            assertThat(record.value()).contains("eventId");
            assertThat(record.value()).contains("occurredAt");
            assertThat(record.value()).contains("version");
            assertThat(record.value()).contains("payload");
        }
    }

    @Test
    @DisplayName("AC5: different consumer groups can independently consume the same topic")
    void different_consumer_groups_receive_same_event() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        EventEnvelope envelope = EventEnvelope.create(
                "ReservationConfirmed",
                1,
                UUID.randomUUID(),
                reservationId,
                Map.of("showId", showId.toString())
        );

        kafkaTemplate.send(TOPIC, showId.toString(), envelope).get();

        String[] consumerGroups = {
                "catalog.reservations-projector",
                "notification.booking-events"
        };

        for (String groupId : consumerGroups) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, groupId,
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
            ))) {
                consumer.subscribe(java.util.List.of(TOPIC));

                ConsumerRecords<String, String> records = ConsumerRecords.empty();
                long deadline = System.currentTimeMillis() + 5_000;
                while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                    records = consumer.poll(Duration.ofMillis(500));
                }

                assertThat(records)
                        .as("Consumer group '%s' should receive the event", groupId)
                        .isNotEmpty();
            }
        }
    }

    @TestConfiguration
    static class StubCatalogConfig {
        @Bean
        @Primary
        CatalogClient stubCatalogClient() {
            return (showId, seatIds) -> {};
        }
    }
}
