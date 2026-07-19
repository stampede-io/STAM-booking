package com.stampedeio.booking.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.domain.ReservationStatus;
import org.springframework.transaction.support.TransactionTemplate;

import com.stampedeio.booking.repository.OutboxRepository;
import com.stampedeio.booking.repository.ReservationEventRepository;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.ReservationSeatRepository;
import com.stampedeio.booking.service.HoldMirrorService;
import com.stampedeio.booking.service.ReservationService;

@DisplayName("Outbox writes on state transitions")
class OutboxWriteTest {

    private ReservationRepository reservationRepository;
    private OutboxRepository outboxRepository;
    private ReservationEventRepository reservationEventRepository;
    private ReservationService service;

    private final Clock clock = Clock.fixed(Instant.parse("2026-07-19T10:00:00Z"), ZoneOffset.UTC);

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        reservationRepository = mock(ReservationRepository.class);
        ReservationSeatRepository reservationSeatRepository = mock(ReservationSeatRepository.class);
        reservationEventRepository = mock(ReservationEventRepository.class);
        outboxRepository = mock(OutboxRepository.class);
        CatalogClient catalogClient = mock(CatalogClient.class);
        HoldMirrorService holdMirrorService = mock(HoldMirrorService.class);
        TransactionTemplate transactionTemplate = mock(TransactionTemplate.class);

        doNothing().when(catalogClient).validateSeatsForShow(any(), any());
        when(reservationEventRepository.findMaxSeqByAggregateId(any())).thenReturn(Optional.empty());
        when(outboxRepository.save(any(OutboxMessage.class))).thenAnswer(inv -> inv.getArgument(0));
        when(reservationEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(transactionTemplate.execute(any())).thenAnswer(inv -> {
            org.springframework.transaction.support.TransactionCallback<?> callback = inv.getArgument(0);
            return callback.doInTransaction(null);
        });

        service = new ReservationService(
                reservationRepository, reservationSeatRepository, reservationEventRepository,
                outboxRepository, catalogClient, holdMirrorService, transactionTemplate, clock);
    }

    @Test
    @DisplayName("AC5: hold() writes SeatsHeld to outbox in same transaction")
    void hold_writes_seats_held_outbox() {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        CreateReservationRequest req = new CreateReservationRequest(showId, userId, List.of(seatId));

        when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
        when(reservationRepository.saveAndFlush(any(Reservation.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        service.hold(key, req);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        OutboxMessage msg = captor.getValue();
        assertThat(msg.getAggregateType()).isEqualTo("Reservation");
        assertThat(msg.getEventType()).isEqualTo("SeatsHeld");
        assertThat(msg.getPayload()).contains("\"status\":\"HELD\"");
        assertThat(msg.getPublishedAt()).isNull();
    }

    @Test
    @DisplayName("AC5: confirm() writes ReservationConfirmed to outbox")
    void confirm_writes_reservation_confirmed_outbox() {
        UUID reservationId = UUID.randomUUID();
        Reservation reservation = buildHeldReservation(reservationId);
        when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
        when(reservationRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.confirm(reservationId, "PAY-123", "corr-1");

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        OutboxMessage msg = captor.getValue();
        assertThat(msg.getEventType()).isEqualTo("ReservationConfirmed");
        assertThat(msg.getAggregateId()).isEqualTo(reservationId);
        assertThat(msg.getPayload()).contains("\"status\":\"CONFIRMED\"");
    }

    @Test
    @DisplayName("AC5: release() writes SeatsReleased to outbox")
    void release_writes_seats_released_outbox() {
        UUID reservationId = UUID.randomUUID();
        Reservation reservation = buildHeldReservation(reservationId);
        when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
        when(reservationRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.release(reservationId, "corr-2");

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        OutboxMessage msg = captor.getValue();
        assertThat(msg.getEventType()).isEqualTo("SeatsReleased");
        assertThat(msg.getPayload()).contains("\"status\":\"RELEASED\"");
    }

    private Reservation buildHeldReservation(UUID id) {
        Reservation r = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        try {
            var idField = Reservation.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(r, id);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        r.addSeat(UUID.randomUUID());
        return r;
    }
}
