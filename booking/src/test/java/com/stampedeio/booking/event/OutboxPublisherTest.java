package com.stampedeio.booking.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
        OutboxMessage msg = new OutboxMessage("Reservation", aggregateId, "SeatsHeld",
                "{\"status\":\"HELD\"}");
        when(outboxRepository.findUnpublished()).thenReturn(List.of(msg));
        when(kafkaTemplate.send(any(String.class), any(String.class), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxRepository.saveAll(any())).thenAnswer(inv -> inv.getArgument(0));

        publisher.poll();

        verify(kafkaTemplate).send(eq("reservations.events"), eq(aggregateId.toString()), any(EventEnvelope.class));
        assertThat(msg.getPublishedAt()).isNotNull();
    }

    @Test
    @DisplayName("AC4: does not publish already-stamped rows (query only returns unpublished)")
    void poll_skips_when_empty() {
        when(outboxRepository.findUnpublished()).thenReturn(Collections.emptyList());

        publisher.poll();

        verify(kafkaTemplate, never()).send(any(String.class), any(String.class), any());
    }

    @Test
    @DisplayName("AC2: sends correct EventEnvelope fields")
    void poll_sends_envelope_with_correct_fields() {
        UUID aggregateId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage("Reservation", aggregateId, "ReservationConfirmed",
                "{\"status\":\"CONFIRMED\"}");
        when(outboxRepository.findUnpublished()).thenReturn(List.of(msg));
        when(kafkaTemplate.send(any(String.class), any(String.class), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxRepository.saveAll(any())).thenAnswer(inv -> inv.getArgument(0));

        publisher.poll();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq("reservations.events"), eq(aggregateId.toString()), captor.capture());
        EventEnvelope envelope = captor.getValue();
        assertThat(envelope.eventType()).isEqualTo("ReservationConfirmed");
        assertThat(envelope.aggregateId()).isEqualTo(aggregateId);
        assertThat(envelope.payload()).isEqualTo("{\"status\":\"CONFIRMED\"}");
    }
}
