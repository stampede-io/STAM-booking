package com.stampedeio.booking.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import com.stampedeio.booking.event.OutboxPublisher;
import com.stampedeio.booking.service.ReservationService;

/**
 * STAM-452: bytes on payments.events that aren't JSON fail in the
 * deserializer, before the listener. They must be dead-lettered as-is, and
 * the PaymentFailed queued behind them on the same partition must still
 * compensate the saga.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"outbox.poll.interval-ms=3600000", "booking.kafka.retry.initial-interval=200"})
@Testcontainers
@Tag("integration")
@Import(PaymentFailedCompensationIT.StubCatalogConfig.class)
@DisplayName("PaymentEventPoisonIT: a non-JSON record is dead-lettered and doesn't block the saga")
class PaymentEventPoisonIT {

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
    private OutboxPublisher outboxPublisher;

    @Autowired
    private BookingSagaOrchestrator sagaOrchestrator;

    @Test
    void non_json_record_is_dead_lettered_and_the_next_event_is_still_consumed() throws Exception {
        UUID userId = UUID.randomUUID();
        UUID reservationId = reservationService.hold(UUID.randomUUID(), userId,
                new CreateReservationRequest(UUID.randomUUID(), List.of(UUID.randomUUID())))
                .response().reservationId();
        reservationService.setPaymentMethod(reservationId, userId, "pm_card_visa");
        sagaOrchestrator.startSaga(reservationId, userId);
        outboxPublisher.poll();

        byte[] garbage = "not-json-at-all".getBytes(StandardCharsets.UTF_8);
        UUID eventId = UUID.randomUUID();
        String paymentFailed = """
                {"eventId":"%s","eventType":"PaymentFailed","version":1,"occurredAt":"2026-01-01T00:00:00Z",\
                "correlationId":"%s","aggregateId":"%s","payload":"{}"}"""
                .formatted(eventId, UUID.randomUUID(), reservationId);
        // Same key, so PaymentFailed sits behind the poison on one partition.
        try (KafkaProducer<String, byte[]> raw = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            raw.send(new ProducerRecord<>("payments.events", reservationId.toString(), garbage)).get();
            raw.send(new ProducerRecord<>("payments.events", reservationId.toString(),
                    paymentFailed.getBytes(StandardCharsets.UTF_8))).get();
        }

        String status = null;
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            status = reservationService.get(reservationId, userId).status();
            if ("RELEASED".equals(status)) {
                break;
            }
            Thread.sleep(250);
        }
        assertThat(status).as("PaymentFailed behind the poison must still compensate").isEqualTo("RELEASED");

        ConsumerRecord<String, byte[]> dlq = dlqRecord(reservationId.toString());
        assertThat(dlq.value()).isEqualTo(garbage);
        Header exceptionClass = dlq.headers().lastHeader("exception-class");
        assertThat(new String(exceptionClass.value(), StandardCharsets.UTF_8)).contains("DeserializationException");
    }

    private ConsumerRecord<String, byte[]> dlqRecord(String key) {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "poison-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            consumer.subscribe(List.of("payments.events.dlq"));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(r.key())) {
                        return r;
                    }
                }
            }
        }
        throw new AssertionError("no DLQ record with key " + key);
    }
}
