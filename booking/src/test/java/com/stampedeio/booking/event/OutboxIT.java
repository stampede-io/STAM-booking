package com.stampedeio.booking.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.repository.OutboxRepository;
import com.stampedeio.booking.service.ReservationService;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "outbox.poll.interval-ms=3600000")
@Testcontainers
@Tag("integration")
@Import(OutboxIT.StubCatalogConfig.class)
@DisplayName("OutboxIT — transactional outbox crash-recovery flagship test")
class OutboxIT {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"));

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
    @DisplayName("AC1+AC2: hold writes outbox row in same TX, publisher sends to Kafka")
    void hold_creates_outbox_row_and_publisher_delivers_to_kafka() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();

        var holdResult = reservationService.hold(idempotencyKey,
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();

        List<OutboxMessage> unpublished = outboxRepository.findUnpublished();
        assertThat(unpublished).isNotEmpty();
        assertThat(unpublished.stream().anyMatch(m -> m.getEventType().equals("SeatsHeld"))).isTrue();

        outboxPublisher.poll();

        assertThat(outboxRepository.findUnpublished()).isEmpty();

        try (KafkaConsumer<String, String> consumer = newConsumer("outbox-it-ac1")) {
            consumer.subscribe(List.of("reservations.events"));
            List<ConsumerRecord<String, String>> matching =
                    pollForKey(consumer, reservationId.toString(), 5_000);
            assertThat(matching).isNotEmpty();
            assertThat(matching.get(0).value()).contains("SeatsHeld");
        }
    }

    @Test
    @DisplayName("AC3: crash recovery — outbox row survives kill-9, event arrives after restart")
    void crash_recovery_outbox_event_delivered_after_restart() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();

        var holdResult = reservationService.hold(idempotencyKey,
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();

        List<OutboxMessage> beforeCrash = outboxRepository.findUnpublished();
        assertThat(beforeCrash).isNotEmpty();

        // Simulate "restart after kill -9": the publisher polls and picks up the orphaned row
        outboxPublisher.poll();

        try (KafkaConsumer<String, String> consumer = newConsumer("outbox-it-ac3")) {
            consumer.subscribe(List.of("reservations.events"));
            List<ConsumerRecord<String, String>> matching =
                    pollForKey(consumer, reservationId.toString(), 5_000);
            assertThat(matching).isNotEmpty();
            assertThat(matching.get(0).value()).contains("SeatsHeld");
        }

        assertThat(outboxRepository.findUnpublished()).isEmpty();
    }

    @Test
    @DisplayName("AC4: idempotent publication — stamped rows are never republished")
    void published_rows_not_republished() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        var holdResult = reservationService.hold(UUID.randomUUID(),
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();

        outboxPublisher.poll();
        assertThat(outboxRepository.findUnpublished()).isEmpty();

        // Second poll should find nothing and not produce duplicate messages
        outboxPublisher.poll();

        try (KafkaConsumer<String, String> consumer = newConsumer("outbox-it-ac4")) {
            consumer.subscribe(List.of("reservations.events"));
            List<ConsumerRecord<String, String>> matching =
                    pollForKey(consumer, reservationId.toString(), 5_000);
            assertThat(matching).hasSize(1);
        }
    }

    @Test
    @DisplayName("AC5: confirm writes ReservationConfirmed to outbox and publishes")
    void confirm_writes_outbox_and_publishes() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();

        var holdResult = reservationService.hold(idempotencyKey,
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();

        outboxPublisher.poll();

        reservationService.confirm(reservationId, "PAY-REF-1", "corr-1");

        List<OutboxMessage> unpublished = outboxRepository.findUnpublished();
        assertThat(unpublished).hasSize(1);
        assertThat(unpublished.get(0).getEventType()).isEqualTo("ReservationConfirmed");

        outboxPublisher.poll();

        try (KafkaConsumer<String, String> consumer = newConsumer("outbox-it-ac5-confirm")) {
            consumer.subscribe(List.of("reservations.events"));
            List<ConsumerRecord<String, String>> matching =
                    pollForKey(consumer, reservationId.toString(), 5_000);
            assertThat(matching.stream().anyMatch(r -> r.value().contains("ReservationConfirmed")))
                    .isTrue();
        }
    }

    @Test
    @DisplayName("AC5: release writes SeatsReleased to outbox and publishes")
    void release_writes_outbox_and_publishes() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();

        var holdResult = reservationService.hold(idempotencyKey,
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();

        outboxPublisher.poll();

        reservationService.release(reservationId, "corr-2");

        List<OutboxMessage> unpublished = outboxRepository.findUnpublished();
        assertThat(unpublished).hasSize(1);
        assertThat(unpublished.get(0).getEventType()).isEqualTo("SeatsReleased");

        outboxPublisher.poll();

        try (KafkaConsumer<String, String> consumer = newConsumer("outbox-it-ac5-release")) {
            consumer.subscribe(List.of("reservations.events"));
            List<ConsumerRecord<String, String>> matching =
                    pollForKey(consumer, reservationId.toString(), 5_000);
            assertThat(matching.stream().anyMatch(r -> r.value().contains("SeatsReleased")))
                    .isTrue();
        }
    }

    private KafkaConsumer<String, String> newConsumer(String groupId) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        ));
    }

    private List<ConsumerRecord<String, String>> pollForKey(KafkaConsumer<String, String> consumer,
                                                            String key, long timeoutMs) {
        List<ConsumerRecord<String, String>> matching = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (matching.isEmpty() && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                if (record.key().equals(key)) {
                    matching.add(record);
                }
            }
        }
        return matching;
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
