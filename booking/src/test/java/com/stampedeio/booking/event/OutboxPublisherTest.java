package com.stampedeio.booking.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.repository.OutboxRepository;

@DisplayName("OutboxPublisher polling logic")
class OutboxPublisherTest {

    private OutboxRepository outboxRepository;
    private KafkaTemplate<String, Object> kafkaTemplate;
    private OutboxPublisher publisher;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        outboxRepository = mock(OutboxRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        publisher = new OutboxPublisher(outboxRepository, kafkaTemplate);
    }

    @Test
    @DisplayName("AC2: publishes unpublished rows to Kafka and stamps published_at")
    void poll_publishes_and_stamps() {
        UUID aggregateId = UUID.randomUUID();
        // Real outbox rows always have a correlationId by the time the
        // poller reads them (CLAUDE.md §4.3's transactional outbox) -- the
        // 4-arg constructor's null id is purely a never-persisted-entity
        // test artifact, not a real production state.
        OutboxMessage msg = new OutboxMessage("Reservation", aggregateId, "SeatsHeld",
                "{\"status\":\"HELD\"}", UUID.randomUUID(), "reservations.events");
        when(outboxRepository.findUnpublished()).thenReturn(List.of(msg));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxRepository.saveAll(any())).thenAnswer(inv -> inv.getArgument(0));

        publisher.poll();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("reservations.events");
        assertThat(captor.getValue().key()).isEqualTo(aggregateId.toString());
        assertThat(msg.getPublishedAt()).isNotNull();
    }

    @Test
    @DisplayName("AC4: does not publish already-stamped rows (query only returns unpublished)")
    void poll_skips_when_empty() {
        when(outboxRepository.findUnpublished()).thenReturn(Collections.emptyList());

        publisher.poll();

        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("AC2: sends correct EventEnvelope fields, including the correlationId Kafka header (STAM-406)")
    void poll_sends_envelope_with_correct_fields() {
        UUID aggregateId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage("Reservation", aggregateId, "ReservationConfirmed",
                "{\"status\":\"CONFIRMED\"}", correlationId, "reservations.events");
        when(outboxRepository.findUnpublished()).thenReturn(List.of(msg));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxRepository.saveAll(any())).thenAnswer(inv -> inv.getArgument(0));

        publisher.poll();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, Object> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("reservations.events");
        assertThat(record.key()).isEqualTo(aggregateId.toString());

        EventEnvelope envelope = (EventEnvelope) record.value();
        assertThat(envelope.eventType()).isEqualTo("ReservationConfirmed");
        assertThat(envelope.aggregateId()).isEqualTo(aggregateId);
        assertThat(envelope.payload()).isEqualTo("{\"status\":\"CONFIRMED\"}");

        assertThat(record.headers().lastHeader("correlationId").value())
                .isEqualTo(correlationId.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
