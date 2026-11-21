package com.stampedeio.booking.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.repository.OutboxRepository;
import com.stampedeio.booking.service.ReservationService;

/**
 * STAM-451: "published" in the outbox must mean "acked by Kafka". The
 * producer is warmed first so it holds topic metadata, which is the state
 * where {@code send()} returns at once with the record only buffered in
 * memory. With the broker paused, a poll must leave the row unpublished,
 * so a crash before the broker returns can't lose the event.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "outbox.poll.interval-ms=3600000")
@Testcontainers
@Tag("integration")
@Import(OutboxIT.StubCatalogConfig.class)
@DisplayName("OutboxBrokerOutageIT: rows stay unpublished until Kafka acks them")
class OutboxBrokerOutageIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"));

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
    private ReservationService reservationService;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Test
    @DisplayName("broker paused: poll leaves the row unpublished; broker back: next poll delivers and stamps it")
    void row_is_marked_published_only_after_the_broker_acks() {
        // Warm the producer: metadata for reservations.events is now cached.
        hold();
        outboxPublisher.poll();
        assertThat(outboxRepository.findUnpublished()).isEmpty();

        var docker = kafka.getDockerClient();
        docker.pauseContainerCmd(kafka.getContainerId()).exec();
        UUID reservationId;
        UUID outboxRowId;
        try {
            reservationId = hold();
            outboxPublisher.poll();

            List<OutboxMessage> stillPending = outboxRepository.findUnpublished();
            assertThat(stillPending)
                    .as("a row Kafka never acked must not be marked published")
                    .extracting(OutboxMessage::getAggregateId)
                    .containsExactly(reservationId);
            outboxRowId = stillPending.get(0).getId();
        } finally {
            docker.unpauseContainerCmd(kafka.getContainerId()).exec();
        }

        outboxPublisher.poll();
        assertThat(outboxRepository.findUnpublished()).isEmpty();
        // The resend carries the row id as eventId, so consumers dedupe any retry.
        assertThat(recordsFor(reservationId)).anySatisfy(v -> assertThat(v)
                .contains("SeatsHeld").contains("\"eventId\":\"" + outboxRowId + "\""));
    }

    private UUID hold() {
        return reservationService.hold(UUID.randomUUID(), UUID.randomUUID(),
                new CreateReservationRequest(UUID.randomUUID(), List.of(UUID.randomUUID())))
                .response().reservationId();
    }

    private List<String> recordsFor(UUID reservationId) {
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-outage-it",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            consumer.subscribe(List.of("reservations.events"));
            long deadline = System.currentTimeMillis() + 10_000;
            while (values.isEmpty() && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (r.key().equals(reservationId.toString())) {
                        values.add(r.value());
                    }
                }
            }
        }
        return values;
    }
}
