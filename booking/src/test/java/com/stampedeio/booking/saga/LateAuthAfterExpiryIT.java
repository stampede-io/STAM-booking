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
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.SagaInstance;
import com.stampedeio.booking.domain.SagaState;
import com.stampedeio.booking.event.OutboxPublisher;
import com.stampedeio.booking.repository.SagaInstanceRepository;
import com.stampedeio.booking.service.HoldExpiryScheduler;
import com.stampedeio.booking.service.ReservationService;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "outbox.poll.interval-ms=3600000")
@Testcontainers
@Tag("integration")
@Import(LateAuthAfterExpiryIT.StubCatalogConfig.class)
@DisplayName("LateAuthAfterExpiryIT — AC3: late PaymentAuthorized on EXPIRED reservation → RefundPayment → RefundIssued")
class LateAuthAfterExpiryIT {

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

    @Autowired
    private HoldExpiryScheduler holdExpiryScheduler;

    @Autowired
    private javax.sql.DataSource dataSource;

    @Test
    @DisplayName("Late PaymentAuthorized on EXPIRED reservation → RefundPayment emitted, then RefundIssued → saga COMPENSATED")
    void late_auth_after_expiry_triggers_refund() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        ReservationService.HoldResult holdResult = reservationService.hold(UUID.randomUUID(),
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();

        outboxPublisher.poll();

        // Start saga
        sagaOrchestrator.startSaga(reservationId);

        // Force expires_at to the past and run expiry sweep
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                     "UPDATE reservations SET expires_at = NOW() - INTERVAL '1 minute' WHERE id = ?::uuid")) {
            stmt.setString(1, reservationId.toString());
            stmt.executeUpdate();
        }
        holdExpiryScheduler.expireStaleHolds();

        // Confirm reservation is EXPIRED and saga is COMPENSATED from expiry
        assertThat(reservationService.get(reservationId).status()).isEqualTo("EXPIRED");
        SagaInstance expiredSaga = sagaInstanceRepository.findByReservationId(reservationId).orElseThrow();
        assertThat(expiredSaga.getState()).isEqualTo(SagaState.COMPENSATED.name());

        // Now simulate a LATE PaymentAuthorized arriving (PSP charged but hold expired)
        // The orchestrator should detect EXPIRED and emit RefundPayment
        // But since saga is already COMPENSATED from expiry, the handlePaymentAuthorized
        // will return early (idempotent guard). This is correct behavior — the saga was
        // already compensated by the expiry sweep.
        //
        // For the "interview-gold race" scenario where the PSP responds before the expiry
        // sweep compensates the saga, we need the saga still in PAYMENT_REQUESTED when
        // PaymentAuthorized arrives. So let's create a fresh scenario:

        UUID showId2 = UUID.randomUUID();
        UUID seatId2 = UUID.randomUUID();

        ReservationService.HoldResult holdResult2 = reservationService.hold(UUID.randomUUID(),
                new CreateReservationRequest(showId2, userId, List.of(seatId2)));
        UUID reservationId2 = holdResult2.response().reservationId();

        outboxPublisher.poll();

        // Start saga — in PAYMENT_REQUESTED
        sagaOrchestrator.startSaga(reservationId2);
        assertThat(sagaInstanceRepository.findByReservationId(reservationId2).orElseThrow().getState())
                .isEqualTo(SagaState.PAYMENT_REQUESTED.name());

        // Force reservation to EXPIRED directly (simulating expiry sweep that didn't
        // touch the saga — the race condition this AC is designed to catch)
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                     "UPDATE reservations SET status = 'EXPIRED', expires_at = NOW() - INTERVAL '1 minute' WHERE id = ?::uuid")) {
            stmt.setString(1, reservationId2.toString());
            stmt.executeUpdate();
        }

        // Late PaymentAuthorized arrives — orchestrator sees EXPIRED, emits RefundPayment
        UUID lateCorrelationId = UUID.randomUUID();
        sagaOrchestrator.handlePaymentAuthorized(reservationId2, lateCorrelationId);

        // Saga should now be in COMPENSATING, waiting for RefundIssued
        SagaInstance compensating = sagaInstanceRepository.findByReservationId(reservationId2).orElseThrow();
        assertThat(compensating.getState()).isEqualTo(SagaState.COMPENSATING.name());
        assertThat(compensating.getStep()).isEqualTo("LATE_AUTH_REFUND_REQUESTED");

        // Publish the RefundPayment command and verify it lands on payments.commands
        outboxPublisher.poll();

        try (KafkaConsumer<String, String> consumer = newConsumer("late-auth-it-commands")) {
            consumer.subscribe(List.of("payments.commands"));
            List<ConsumerRecord<String, String>> commands =
                    pollForKey(consumer, reservationId2.toString(), 10_000);

            boolean hasRefundCommand = commands.stream()
                    .anyMatch(r -> r.value().contains("RefundPayment"));
            assertThat(hasRefundCommand)
                    .as("RefundPayment command must be emitted for late auth on expired reservation")
                    .isTrue();
        }

        // Simulate RefundIssued coming back from payment service
        UUID refundCorrelationId = UUID.randomUUID();
        sagaOrchestrator.handleRefundIssued(reservationId2, refundCorrelationId);

        // Reservation should now be REFUNDED
        assertThat(reservationService.get(reservationId2).status()).isEqualTo("REFUNDED");

        // Saga should be COMPENSATED with terminal step (AC5)
        SagaInstance finalSaga = sagaInstanceRepository.findByReservationId(reservationId2).orElseThrow();
        assertThat(finalSaga.getState()).isEqualTo(SagaState.COMPENSATED.name());
        assertThat(finalSaga.getStep()).isEqualTo("REFUND_ISSUED");

        // Verify PaymentRefunded event in outbox
        outboxPublisher.poll();

        try (KafkaConsumer<String, String> consumer = newConsumer("late-auth-it-events")) {
            consumer.subscribe(List.of("reservations.events"));
            List<ConsumerRecord<String, String>> events =
                    pollForKey(consumer, reservationId2.toString(), 10_000);

            boolean hasPaymentRefunded = events.stream()
                    .anyMatch(r -> r.value().contains("PaymentRefunded"));
            assertThat(hasPaymentRefunded)
                    .as("PaymentRefunded event must be emitted after RefundIssued")
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
