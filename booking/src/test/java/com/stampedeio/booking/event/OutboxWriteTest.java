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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.JsonNode;
import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.catalog.CatalogClient;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final ObjectMapper objectMapper = new ObjectMapper();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        MDC.clear();
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
                outboxRepository, catalogClient, holdMirrorService, transactionTemplate, clock,
                objectMapper);
    }

    @Test
    @DisplayName("AC5: hold() writes SeatsHeld to outbox in same transaction")
    void hold_writes_seats_held_outbox() {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        CreateReservationRequest req = new CreateReservationRequest(showId, List.of(seatId));

        when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
        when(reservationRepository.saveAndFlush(any(Reservation.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        service.hold(key, userId, req);

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

        service.confirm(reservationId, reservation.getUserId(), "PAY-123", "corr-1");

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

        service.release(reservationId, reservation.getUserId(), "corr-2");

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        OutboxMessage msg = captor.getValue();
        assertThat(msg.getEventType()).isEqualTo("SeatsReleased");
        assertThat(msg.getPayload()).contains("\"status\":\"RELEASED\"");
    }

    @Test
    @DisplayName("STAM-444: hold() carries the gateway's correlation ID onto the outbox row")
    void hold_outboxCorrelationId_matchesGatewaySuppliedId() {
        String gatewayCorrelationId = UUID.randomUUID().toString();
        MDC.put("correlationId", gatewayCorrelationId);

        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        CreateReservationRequest req = new CreateReservationRequest(showId, List.of(seatId));

        when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
        when(reservationRepository.saveAndFlush(any(Reservation.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        service.hold(key, userId, req);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getCorrelationId()).isEqualTo(UUID.fromString(gatewayCorrelationId));
    }

    @Test
    @DisplayName("STAM-444: hold() without an inbound correlation ID still mints one, doesn't fail")
    void hold_withoutCorrelationId_mintsOne() {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        CreateReservationRequest req = new CreateReservationRequest(showId, List.of(seatId));

        when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
        when(reservationRepository.saveAndFlush(any(Reservation.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        service.hold(key, userId, req);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getCorrelationId()).isNotNull();
    }

    @Test
    @DisplayName("STAM-444: a correlation ID containing a quote and backslash doesn't corrupt the outbox payload")
    void confirm_hostileCorrelationId_producesValidJsonWithFieldsUnchanged() throws Exception {
        UUID reservationId = UUID.randomUUID();
        Reservation reservation = buildHeldReservation(reservationId);
        when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
        when(reservationRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        String hostileId = "trace\"};{\\injected";

        service.confirm(reservationId, reservation.getUserId(), "PAY-123", hostileId);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        String payload = captor.getValue().getPayload();

        JsonNode node = objectMapper.readTree(payload); // throws if the JSON is malformed
        assertThat(node.get("status").asText()).isEqualTo("CONFIRMED");
        assertThat(node.get("reservationId").asText()).isEqualTo(reservationId.toString());
        assertThat(node.get("correlationId").asText()).isEqualTo(hostileId);
        // not a valid UUID, so the typed column correctly stores null rather than guessing
        assertThat(captor.getValue().getCorrelationId()).isNull();
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
