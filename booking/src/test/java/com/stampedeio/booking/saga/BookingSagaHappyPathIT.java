package com.stampedeio.booking.saga;

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
import com.stampedeio.booking.api.ReservationResponse;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.SagaInstance;
import com.stampedeio.booking.domain.SagaState;
import com.stampedeio.booking.event.OutboxPublisher;
import com.stampedeio.booking.repository.SagaInstanceRepository;
import com.stampedeio.booking.service.ReservationService;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "outbox.poll.interval-ms=3600000")
@Testcontainers
@Tag("integration")
@Import(BookingSagaHappyPathIT.StubCatalogConfig.class)
@DisplayName("BookingSagaHappyPathIT — full happy path: SeatsHeld → AuthorizePayment → PaymentAuthorized → Confirmed")
class BookingSagaHappyPathIT {

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
    private SagaInstanceRepository sagaInstanceRepository;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private BookingSagaOrchestrator sagaOrchestrator;

    @Test
    @DisplayName("AC1–AC5: full happy path from POST /reservations through saga to CONFIRMED")
    void full_happy_path() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();

        // Step 1: Create a HELD reservation
        ReservationService.HoldResult holdResult = reservationService.hold(idempotencyKey,
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();
        assertThat(holdResult.response().status()).isEqualTo("HELD");

        // AC1 step 1: SeatsHeld written to outbox, publish it
        outboxPublisher.poll();

        // Step 2: Start saga — emits AuthorizePayment command to payments.commands
        SagaInstance saga = sagaOrchestrator.startSaga(reservationId);

        // AC2: saga_instances row shows current state and step mid-flight
        assertThat(saga.getState()).isEqualTo(SagaState.PAYMENT_REQUESTED.name());
        assertThat(saga.getStep()).isEqualTo("EMIT_AUTHORIZE_PAYMENT");

        // Publish the AuthorizePayment command
        outboxPublisher.poll();

        // AC3: Verify AuthorizePayment command lands on payments.commands with correlationId
        String correlationId;
        try (KafkaConsumer<String, String> consumer = newConsumer("saga-it-commands")) {
            consumer.subscribe(List.of("payments.commands"));
            List<ConsumerRecord<String, String>> commands =
                    pollForKey(consumer, reservationId.toString(), 10_000);
            assertThat(commands).isNotEmpty();
            String commandValue = commands.get(0).value();
            assertThat(commandValue).contains("AuthorizePayment");
            assertThat(commandValue).contains("correlationId");

            correlationId = extractField(commandValue, "correlationId");
            assertThat(correlationId).isNotNull();
        }

        // AC1 step 3+4: Simulate PaymentAuthorized → confirm reservation → emit ReservationConfirmed
        sagaOrchestrator.handlePaymentAuthorized(reservationId, UUID.fromString(correlationId));

        // AC4: GET reservation returns status: CONFIRMED
        ReservationResponse confirmed = reservationService.get(reservationId);
        assertThat(confirmed.status()).isEqualTo("CONFIRMED");

        // AC5: saga_instances row shows state: COMPLETED, not left intermediate
        SagaInstance completedSaga = sagaInstanceRepository.findByReservationId(reservationId).orElseThrow();
        assertThat(completedSaga.getState()).isEqualTo(SagaState.COMPLETED.name());
        assertThat(completedSaga.getStep()).isEqualTo("RESERVATION_CONFIRMED");

        // Publish ReservationConfirmed event
        outboxPublisher.poll();

        // AC3: Verify the same correlationId traces through SeatsHeld → AuthorizePayment → ReservationConfirmed
        try (KafkaConsumer<String, String> consumer = newConsumer("saga-it-events")) {
            consumer.subscribe(List.of("reservations.events"));
            List<ConsumerRecord<String, String>> events =
                    pollForKey(consumer, reservationId.toString(), 10_000);

            boolean hasSeatsHeld = events.stream()
                    .anyMatch(r -> r.value().contains("SeatsHeld"));
            boolean hasConfirmed = events.stream()
                    .anyMatch(r -> r.value().contains("ReservationConfirmed"));
            assertThat(hasSeatsHeld).isTrue();
            assertThat(hasConfirmed).isTrue();

            // ReservationConfirmed envelope carries the same correlationId
            String confirmedEvent = events.stream()
                    .filter(r -> r.value().contains("ReservationConfirmed"))
                    .findFirst()
                    .map(ConsumerRecord::value)
                    .orElseThrow();
            assertThat(confirmedEvent).contains(correlationId);
        }
    }

    @Test
    @DisplayName("AC2: saga_instances row shows state and step at any point mid-flight")
    void saga_state_inspectable_mid_flight() {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();

        ReservationService.HoldResult holdResult = reservationService.hold(idempotencyKey,
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();

        // Before saga starts — no saga row
        assertThat(sagaInstanceRepository.findByReservationId(reservationId)).isEmpty();

        // Start saga
        sagaOrchestrator.startSaga(reservationId);

        // Mid-flight: saga in PAYMENT_REQUESTED
        SagaInstance saga = sagaInstanceRepository.findByReservationId(reservationId).orElseThrow();
        assertThat(saga.getState()).isEqualTo("PAYMENT_REQUESTED");
        assertThat(saga.getStep()).isEqualTo("EMIT_AUTHORIZE_PAYMENT");
        assertThat(saga.getSagaType()).isEqualTo("BookingSaga");
    }

    @Test
    @DisplayName("Saga start is idempotent — calling twice returns same saga")
    void saga_start_is_idempotent() {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        ReservationService.HoldResult holdResult = reservationService.hold(UUID.randomUUID(),
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();

        SagaInstance first = sagaOrchestrator.startSaga(reservationId);
        SagaInstance second = sagaOrchestrator.startSaga(reservationId);

        assertThat(first.getId()).isEqualTo(second.getId());
    }

    private String extractField(String json, String fieldName) {
        int idx = json.indexOf("\"" + fieldName + "\"");
        if (idx == -1) return null;
        int colonIdx = json.indexOf(":", idx);
        int startQuote = json.indexOf("\"", colonIdx + 1);
        int endQuote = json.indexOf("\"", startQuote + 1);
        return json.substring(startQuote + 1, endQuote);
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
