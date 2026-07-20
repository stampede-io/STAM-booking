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
import com.stampedeio.booking.service.ReservationService;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "outbox.poll.interval-ms=3600000")
@Testcontainers
@Tag("integration")
@Import(PaymentFailedCompensationIT.StubCatalogConfig.class)
@DisplayName("PaymentFailedCompensationIT — AC1: PaymentFailed → RELEASED + SeatsReleased + COMPENSATED")
class PaymentFailedCompensationIT {

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
    @DisplayName("PaymentFailed → reservation RELEASED, SeatsReleased emitted, saga COMPENSATED")
    void payment_failed_compensates_saga() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        ReservationService.HoldResult holdResult = reservationService.hold(UUID.randomUUID(),
                new CreateReservationRequest(showId, userId, List.of(seatId)));
        UUID reservationId = holdResult.response().reservationId();
        assertThat(holdResult.response().status()).isEqualTo("HELD");

        outboxPublisher.poll();

        SagaInstance saga = sagaOrchestrator.startSaga(reservationId);
        assertThat(saga.getState()).isEqualTo(SagaState.PAYMENT_REQUESTED.name());

        outboxPublisher.poll();

        // Extract correlationId from the AuthorizePayment command
        String correlationId;
        try (KafkaConsumer<String, String> consumer = newConsumer("pf-comp-it-commands")) {
            consumer.subscribe(List.of("payments.commands"));
            List<ConsumerRecord<String, String>> commands =
                    pollForKey(consumer, reservationId.toString(), 10_000);
            assertThat(commands).isNotEmpty();
            correlationId = extractField(commands.get(0).value(), "correlationId");
        }

        // Simulate PaymentFailed
        sagaOrchestrator.handlePaymentFailed(reservationId, UUID.fromString(correlationId));

        // Verify reservation is RELEASED
        assertThat(reservationService.get(reservationId).status()).isEqualTo("RELEASED");

        // Verify saga is COMPENSATED with terminal step
        SagaInstance compensated = sagaInstanceRepository.findByReservationId(reservationId).orElseThrow();
        assertThat(compensated.getState()).isEqualTo(SagaState.COMPENSATED.name());
        assertThat(compensated.getStep()).isEqualTo("SEATS_RELEASED");

        // Publish and verify SeatsReleased event
        outboxPublisher.poll();

        try (KafkaConsumer<String, String> consumer = newConsumer("pf-comp-it-events")) {
            consumer.subscribe(List.of("reservations.events"));
            List<ConsumerRecord<String, String>> events =
                    pollForKey(consumer, reservationId.toString(), 10_000);

            boolean hasSeatsReleased = events.stream()
                    .anyMatch(r -> r.value().contains("SeatsReleased"));
            assertThat(hasSeatsReleased)
                    .as("SeatsReleased event must be emitted on PaymentFailed compensation")
                    .isTrue();
        }
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
