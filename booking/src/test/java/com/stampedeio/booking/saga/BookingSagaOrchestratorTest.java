package com.stampedeio.booking.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.exception.UnprocessableEntityException;
import com.stampedeio.booking.repository.OutboxRepository;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.SagaInstanceRepository;
import com.stampedeio.booking.service.HoldMirrorService;
import com.stampedeio.booking.service.ReservationService;

@DisplayName("STAM-444: saga start threads the gateway's correlation ID")
class BookingSagaOrchestratorTest {

    private SagaInstanceRepository sagaInstanceRepository;
    private ReservationRepository reservationRepository;
    private OutboxRepository outboxRepository;
    private BookingSagaOrchestrator orchestrator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        MDC.clear();
        sagaInstanceRepository = mock(SagaInstanceRepository.class);
        reservationRepository = mock(ReservationRepository.class);
        outboxRepository = mock(OutboxRepository.class);
        ReservationService reservationService = mock(ReservationService.class);
        HoldMirrorService holdMirrorService = mock(HoldMirrorService.class);
        CatalogClient catalogClient = mock(CatalogClient.class);
        when(catalogClient.totalPriceCentsForSeats(any(), any())).thenReturn(5000L);

        orchestrator = new BookingSagaOrchestrator(
                sagaInstanceRepository, reservationRepository, outboxRepository,
                reservationService, holdMirrorService, objectMapper, catalogClient);
    }

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    @DisplayName("startSaga() carries the gateway's correlation ID onto the AuthorizePayment outbox row")
    void startSaga_outboxCorrelationId_matchesGatewaySuppliedId() {
        String gatewayCorrelationId = UUID.randomUUID().toString();
        MDC.put("correlationId", gatewayCorrelationId);

        UUID reservationId = UUID.randomUUID();
        Reservation reservation = buildHeldReservation(reservationId);
        when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
        when(sagaInstanceRepository.findByReservationId(reservationId)).thenReturn(Optional.empty());
        when(sagaInstanceRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        orchestrator.startSaga(reservationId);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getCorrelationId()).isEqualTo(UUID.fromString(gatewayCorrelationId));
    }

    @Test
    @DisplayName("startSaga() without an inbound correlation ID still mints a valid one")
    void startSaga_withoutCorrelationId_mintsOne() {
        UUID reservationId = UUID.randomUUID();
        Reservation reservation = buildHeldReservation(reservationId);
        when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
        when(sagaInstanceRepository.findByReservationId(reservationId)).thenReturn(Optional.empty());
        when(sagaInstanceRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        orchestrator.startSaga(reservationId);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getCorrelationId()).isNotNull();
    }

    @Test
    @DisplayName("startSaga() discards a malformed MDC correlation ID rather than writing a null outbox column")
    void startSaga_malformedCorrelationId_mintsValidReplacement() {
        MDC.put("correlationId", "not-a-uuid");

        UUID reservationId = UUID.randomUUID();
        Reservation reservation = buildHeldReservation(reservationId);
        when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
        when(sagaInstanceRepository.findByReservationId(reservationId)).thenReturn(Optional.empty());
        when(sagaInstanceRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        orchestrator.startSaga(reservationId);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getCorrelationId()).isNotNull();
    }

    @Test
    @DisplayName("STAM-442: startSaga() rejects a reservation with no payment method set")
    void startSaga_withoutPaymentMethod_throwsUnprocessableEntity() {
        UUID reservationId = UUID.randomUUID();
        Reservation reservation = buildHeldReservation(reservationId);
        reservation.setPaymentMethodId(null);
        when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

        assertThrows(UnprocessableEntityException.class, () -> orchestrator.startSaga(reservationId));
        verify(outboxRepository, never()).save(any());
    }

    @Test
    @DisplayName("STAM-442: AuthorizePayment payload carries paymentMethodId and a catalog-computed amountCents")
    void startSaga_outboxPayload_containsPaymentMethodAndAmount() throws Exception {
        UUID reservationId = UUID.randomUUID();
        Reservation reservation = buildHeldReservation(reservationId);
        when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
        when(sagaInstanceRepository.findByReservationId(reservationId)).thenReturn(Optional.empty());
        when(sagaInstanceRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        orchestrator.startSaga(reservationId);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        JsonNode payload = objectMapper.readTree(captor.getValue().getPayload());
        assertThat(payload.get("paymentMethodId").asText()).isEqualTo("pm_card_visa");
        assertThat(payload.get("amountCents").asLong()).isEqualTo(5000L);
    }

    private Reservation buildHeldReservation(UUID id) {
        Reservation r = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        try {
            var idField = Reservation.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(r, id);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        r.addSeat(UUID.randomUUID());
        r.setPaymentMethodId("pm_card_visa");
        return r;
    }
}
