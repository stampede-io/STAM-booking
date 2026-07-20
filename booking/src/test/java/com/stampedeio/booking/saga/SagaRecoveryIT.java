package com.stampedeio.booking.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.stampedeio.booking.BookingApplication;
import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.SagaInstance;
import com.stampedeio.booking.domain.SagaState;
import com.stampedeio.booking.event.OutboxPublisher;
import com.stampedeio.booking.repository.SagaInstanceRepository;
import com.stampedeio.booking.service.ReservationService;
import com.stampedeio.booking.service.SagaRecoverySweep;

@Testcontainers
@Tag("integration")
@DisplayName("SagaRecoveryIT — AC3: pause → kill context → restart → sweep recovers saga")
class SagaRecoveryIT {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"));

    @Test
    @DisplayName("Saga in PAYMENT_REQUESTED survives context kill — recovery sweep re-emits AuthorizePayment, saga converges")
    void saga_recovers_after_context_restart() throws Exception {
        // ---- Phase 1: Boot context, create reservation, start saga ----
        ConfigurableApplicationContext ctx1 = bootContext();
        try {
            ReservationService reservationService = ctx1.getBean(ReservationService.class);
            BookingSagaOrchestrator orchestrator = ctx1.getBean(BookingSagaOrchestrator.class);
            OutboxPublisher outboxPublisher = ctx1.getBean(OutboxPublisher.class);
            SagaInstanceRepository sagaRepo = ctx1.getBean(SagaInstanceRepository.class);

            UUID showId = UUID.randomUUID();
            UUID userId = UUID.randomUUID();
            UUID seatId = UUID.randomUUID();

            ReservationService.HoldResult holdResult = reservationService.hold(UUID.randomUUID(),
                    new CreateReservationRequest(showId, userId, List.of(seatId)));
            UUID reservationId = holdResult.response().reservationId();
            assertThat(holdResult.response().status()).isEqualTo("HELD");

            outboxPublisher.poll();

            orchestrator.startSaga(reservationId);
            SagaInstance midFlight = sagaRepo.findByReservationId(reservationId).orElseThrow();
            assertThat(midFlight.getState()).isEqualTo(SagaState.PAYMENT_REQUESTED.name());

            outboxPublisher.poll();

            // ---- Phase 2: Backdate updated_at to make saga look stale ----
            DataSource ds = ctx1.getBean(DataSource.class);
            try (var conn = ds.getConnection();
                 var stmt = conn.prepareStatement(
                         "UPDATE saga_instances SET updated_at = NOW() - INTERVAL '15 minutes' WHERE reservation_id = ?::uuid")) {
                stmt.setString(1, reservationId.toString());
                stmt.executeUpdate();
            }

            // ---- Phase 3: Kill the application context (simulates kill -9) ----
            ctx1.close();
            ctx1 = null;

            // ---- Phase 4: Restart — recovery sweep fires on ApplicationReadyEvent ----
            ConfigurableApplicationContext ctx2 = bootContext();
            try {
                SagaInstanceRepository sagaRepo2 = ctx2.getBean(SagaInstanceRepository.class);

                // The sweep ran at startup — verify it re-emitted AuthorizePayment
                SagaInstance recovered = sagaRepo2.findByReservationId(reservationId).orElseThrow();
                assertThat(recovered.getStep()).isEqualTo("RECOVERY_REEMIT_AUTHORIZE_PAYMENT");

                // Verify AuthorizePayment command was re-emitted to Kafka
                OutboxPublisher outboxPublisher2 = ctx2.getBean(OutboxPublisher.class);
                outboxPublisher2.poll();

                try (KafkaConsumer<String, String> consumer = newConsumer("recovery-it-commands")) {
                    consumer.subscribe(List.of("payments.commands"));
                    List<ConsumerRecord<String, String>> commands =
                            pollForKey(consumer, reservationId.toString(), 10_000);
                    assertThat(commands).hasSizeGreaterThanOrEqualTo(2);
                    long authorizeCount = commands.stream()
                            .filter(r -> r.value().contains("AuthorizePayment"))
                            .count();
                    assertThat(authorizeCount)
                            .as("Original + recovery re-emit of AuthorizePayment")
                            .isGreaterThanOrEqualTo(2);
                }

                // ---- Phase 5: Simulate payment response → saga converges to COMPLETED ----
                BookingSagaOrchestrator orchestrator2 = ctx2.getBean(BookingSagaOrchestrator.class);
                orchestrator2.handlePaymentAuthorized(reservationId, UUID.randomUUID());

                SagaInstance completed = sagaRepo2.findByReservationId(reservationId).orElseThrow();
                assertThat(completed.getState()).isEqualTo(SagaState.COMPLETED.name());
            } finally {
                ctx2.close();
            }
        } finally {
            if (ctx1 != null) {
                ctx1.close();
            }
        }
    }

    @Test
    @DisplayName("AC4: stale saga compensated — seats released, SeatsReleased emitted, state = COMPENSATED")
    void stale_saga_in_started_is_compensated() throws Exception {
        ConfigurableApplicationContext ctx = bootContext();
        try {
            ReservationService reservationService = ctx.getBean(ReservationService.class);
            BookingSagaOrchestrator orchestrator = ctx.getBean(BookingSagaOrchestrator.class);
            SagaInstanceRepository sagaRepo = ctx.getBean(SagaInstanceRepository.class);
            SagaRecoverySweep sweep = ctx.getBean(SagaRecoverySweep.class);
            OutboxPublisher outboxPublisher = ctx.getBean(OutboxPublisher.class);
            DataSource ds = ctx.getBean(DataSource.class);

            UUID showId = UUID.randomUUID();
            UUID userId = UUID.randomUUID();
            UUID seatId = UUID.randomUUID();

            ReservationService.HoldResult holdResult = reservationService.hold(UUID.randomUUID(),
                    new CreateReservationRequest(showId, userId, List.of(seatId)));
            UUID reservationId = holdResult.response().reservationId();

            orchestrator.startSaga(reservationId);

            // Force saga into COMPENSATING state and backdate
            try (var conn = ds.getConnection();
                 var stmt = conn.prepareStatement(
                         "UPDATE saga_instances SET state = 'COMPENSATING', step = 'STUCK', " +
                                 "updated_at = NOW() - INTERVAL '15 minutes' WHERE reservation_id = ?::uuid")) {
                stmt.setString(1, reservationId.toString());
                stmt.executeUpdate();
            }

            sweep.sweep();

            SagaInstance compensated = sagaRepo.findByReservationId(reservationId).orElseThrow();
            assertThat(compensated.getState()).isEqualTo(SagaState.COMPENSATED.name());
            assertThat(compensated.getStep()).isEqualTo("RECOVERY_SWEEP_COMPENSATED");

            assertThat(reservationService.get(reservationId).status()).isEqualTo("RELEASED");

            outboxPublisher.poll();

            try (KafkaConsumer<String, String> consumer = newConsumer("recovery-it-compensate")) {
                consumer.subscribe(List.of("reservations.events"));
                List<ConsumerRecord<String, String>> events =
                        pollForKey(consumer, reservationId.toString(), 10_000);
                boolean hasSeatsReleased = events.stream()
                        .anyMatch(r -> r.value().contains("SeatsReleased"));
                assertThat(hasSeatsReleased)
                        .as("SeatsReleased event must be emitted on compensation")
                        .isTrue();
            }
        } finally {
            ctx.close();
        }
    }

    @Test
    @DisplayName("AC5: no stale sagas → no DB updates, DEBUG log only")
    void no_stale_sagas_is_noop() {
        ConfigurableApplicationContext ctx = bootContext();
        try {
            SagaRecoverySweep sweep = ctx.getBean(SagaRecoverySweep.class);
            SagaInstanceRepository sagaRepo = ctx.getBean(SagaInstanceRepository.class);
            long countBefore = sagaRepo.count();

            sweep.sweep();

            assertThat(sagaRepo.count()).isEqualTo(countBefore);
        } finally {
            ctx.close();
        }
    }

    private ConfigurableApplicationContext bootContext() {
        return new SpringApplicationBuilder(BookingApplication.class, StubCatalogConfig.class)
                .run(
                        "--spring.datasource.url=" + postgres.getJdbcUrl(),
                        "--spring.datasource.username=" + postgres.getUsername(),
                        "--spring.datasource.password=" + postgres.getPassword(),
                        "--spring.data.redis.host=" + redis.getHost(),
                        "--spring.data.redis.port=" + redis.getMappedPort(6379),
                        "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "--outbox.poll.interval-ms=3600000",
                        "--saga.recovery.stale-after=PT1S",
                        "--server.port=0");
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
