package com.stampedeio.booking.saga;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.config.CorrelationIds;
import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.domain.ReservationStateMachine;
import com.stampedeio.booking.domain.ReservationStatus;
import com.stampedeio.booking.domain.SagaInstance;
import com.stampedeio.booking.domain.SagaState;
import com.stampedeio.booking.domain.SeatHoldStatus;
import com.stampedeio.booking.exception.ResourceNotFoundException;
import com.stampedeio.booking.exception.UnprocessableEntityException;
import com.stampedeio.booking.repository.OutboxRepository;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.SagaInstanceRepository;
import com.stampedeio.booking.service.HoldMirrorService;
import com.stampedeio.booking.service.ReservationService;

@Component
public class BookingSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(BookingSagaOrchestrator.class);
    private static final String SAGA_TYPE = "BookingSaga";
    private static final String TOPIC_PAYMENTS_COMMANDS = "payments.commands";
    private static final String TOPIC_RESERVATIONS_EVENTS = "reservations.events";

    private final SagaInstanceRepository sagaInstanceRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxRepository outboxRepository;
    private final ReservationService reservationService;
    private final HoldMirrorService holdMirrorService;
    private final ObjectMapper objectMapper;
    private final CatalogClient catalogClient;

    public BookingSagaOrchestrator(SagaInstanceRepository sagaInstanceRepository,
                                   ReservationRepository reservationRepository,
                                   OutboxRepository outboxRepository,
                                   ReservationService reservationService,
                                   HoldMirrorService holdMirrorService,
                                   ObjectMapper objectMapper,
                                   CatalogClient catalogClient) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.reservationRepository = reservationRepository;
        this.outboxRepository = outboxRepository;
        this.reservationService = reservationService;
        this.holdMirrorService = holdMirrorService;
        this.objectMapper = objectMapper;
        this.catalogClient = catalogClient;
    }

    @Transactional
    public SagaInstance startSaga(UUID reservationId, UUID callerUserId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));

        // STAM-447: 404, not 403 — matches ReservationService's ownership check.
        if (!reservation.getUserId().equals(callerUserId)) {
            throw new ResourceNotFoundException("Reservation", reservationId);
        }

        if (reservation.getStatus() != ReservationStatus.HELD) {
            throw new IllegalStateException(
                    "Cannot start saga for reservation in state " + reservation.getStatus());
        }

        if (reservation.getPaymentMethodId() == null || reservation.getPaymentMethodId().isBlank()) {
            throw new UnprocessableEntityException(
                    "No payment method set for reservation " + reservationId
                            + " — call PATCH .../payment-method first");
        }

        // Idempotent: if saga already exists for this reservation, return it
        return sagaInstanceRepository.findByReservationId(reservationId)
                .orElseGet(() -> {
                    // STAM-443: the gateway forwards the caller's correlation ID into MDC.
                    // Compensation/recovery paths below run from a Kafka consumer or scheduler
                    // with no request in flight, so they correctly mint their own.
                    UUID correlationId = CorrelationIds.currentOrNew();
                    SagaInstance saga = new SagaInstance(SAGA_TYPE, reservation);
                    saga.advance(SagaState.PAYMENT_REQUESTED, "EMIT_AUTHORIZE_PAYMENT");

                    String commandPayload = buildAuthorizePaymentPayload(reservation, correlationId);
                    outboxRepository.save(new OutboxMessage(
                            "Reservation", reservation.getId(), "AuthorizePayment", commandPayload,
                            correlationId, TOPIC_PAYMENTS_COMMANDS));

                    SagaInstance saved = sagaInstanceRepository.save(saga);
                    log.info("Saga started for reservation={} correlationId={}", reservationId, correlationId);
                    return saved;
                });
    }

    @Transactional
    public void handlePaymentAuthorized(UUID reservationId, UUID correlationId) {
        SagaInstance saga = sagaInstanceRepository.findByReservationId(reservationId)
                .orElseThrow(() -> new IllegalStateException(
                        "No saga found for reservation " + reservationId));

        if (SagaState.COMPLETED.name().equals(saga.getState())
                || SagaState.COMPENSATED.name().equals(saga.getState())) {
            log.info("Saga already terminal for reservation={}, ignoring duplicate", reservationId);
            return;
        }

        Reservation reservation = saga.getReservation();

        if (reservation.getStatus() == ReservationStatus.EXPIRED) {
            log.warn("Late PaymentAuthorized for EXPIRED reservation={}, issuing refund", reservationId);
            saga.advance(SagaState.COMPENSATING, "LATE_AUTH_REFUND_REQUESTED");

            UUID refundCorrelationId = UUID.randomUUID();
            String commandPayload = buildPayload(reservation, refundCorrelationId);
            outboxRepository.save(new OutboxMessage(
                    "Reservation", reservation.getId(), "RefundPayment", commandPayload,
                    refundCorrelationId, TOPIC_PAYMENTS_COMMANDS));

            sagaInstanceRepository.save(saga);
            return;
        }

        ReservationStateMachine.transition(reservation.getStatus(), ReservationStatus.CONFIRMED);
        reservation.setStatus(ReservationStatus.CONFIRMED);
        reservation.getSeats().forEach(seat -> seat.setStatus(SeatHoldStatus.CONFIRMED));
        reservationRepository.save(reservation);

        holdMirrorService.remove(reservationId);

        reservationService.appendEvent(reservation, "RESERVATION_CONFIRMED",
                correlationId.toString(), null);

        String eventPayload = buildPayload(reservation, correlationId);
        outboxRepository.save(new OutboxMessage(
                "Reservation", reservation.getId(), "ReservationConfirmed", eventPayload,
                correlationId, TOPIC_RESERVATIONS_EVENTS));

        saga.advance(SagaState.COMPLETED, "RESERVATION_CONFIRMED");
        sagaInstanceRepository.save(saga);

        log.info("Saga completed for reservation={} correlationId={}", reservationId, correlationId);
    }

    @Transactional
    public void handleRefundIssued(UUID reservationId, UUID correlationId) {
        SagaInstance saga = sagaInstanceRepository.findByReservationId(reservationId)
                .orElseThrow(() -> new IllegalStateException(
                        "No saga found for reservation " + reservationId));

        if (SagaState.COMPENSATED.name().equals(saga.getState())) {
            log.info("Saga already compensated for reservation={}, ignoring duplicate RefundIssued", reservationId);
            return;
        }

        Reservation reservation = saga.getReservation();

        ReservationStateMachine.transition(reservation.getStatus(), ReservationStatus.REFUNDED);
        reservation.setStatus(ReservationStatus.REFUNDED);
        reservationRepository.save(reservation);

        reservationService.appendEvent(reservation, "PAYMENT_REFUNDED",
                correlationId.toString(), null);

        String eventPayload = buildPayload(reservation, correlationId);
        outboxRepository.save(new OutboxMessage(
                "Reservation", reservation.getId(), "PaymentRefunded", eventPayload,
                correlationId, TOPIC_RESERVATIONS_EVENTS));

        saga.advance(SagaState.COMPENSATED, "REFUND_ISSUED");
        sagaInstanceRepository.save(saga);

        log.info("Saga compensated (refund) for reservation={} correlationId={}", reservationId, correlationId);
    }

    @Transactional
    public void handlePaymentFailed(UUID reservationId, UUID correlationId) {
        SagaInstance saga = sagaInstanceRepository.findByReservationId(reservationId)
                .orElseThrow(() -> new IllegalStateException(
                        "No saga found for reservation " + reservationId));

        if (SagaState.COMPENSATED.name().equals(saga.getState())
                || SagaState.COMPLETED.name().equals(saga.getState())) {
            log.info("Saga already terminal for reservation={}, ignoring", reservationId);
            return;
        }

        saga.advance(SagaState.COMPENSATING, "RELEASING_SEATS");

        Reservation reservation = saga.getReservation();
        ReservationStateMachine.transition(reservation.getStatus(), ReservationStatus.RELEASED);
        reservation.setStatus(ReservationStatus.RELEASED);
        reservation.getSeats().forEach(seat -> seat.setStatus(SeatHoldStatus.RELEASED));
        reservationRepository.save(reservation);

        holdMirrorService.remove(reservationId);

        reservationService.appendEvent(reservation, "RESERVATION_RELEASED",
                correlationId.toString(), null);

        String eventPayload = buildPayload(reservation, correlationId);
        outboxRepository.save(new OutboxMessage(
                "Reservation", reservation.getId(), "SeatsReleased", eventPayload,
                correlationId, TOPIC_RESERVATIONS_EVENTS));

        saga.advance(SagaState.COMPENSATED, "SEATS_RELEASED");
        sagaInstanceRepository.save(saga);

        log.info("Saga compensated for reservation={} correlationId={}", reservationId, correlationId);
    }

    @Transactional
    public void recoverStaleSaga(UUID sagaId) {
        SagaInstance saga = sagaInstanceRepository.findById(sagaId)
                .orElseThrow(() -> new IllegalStateException("Saga not found: " + sagaId));
        String state = saga.getState();
        Reservation reservation = saga.getReservation();

        if (SagaState.PAYMENT_REQUESTED.name().equals(state)) {
            UUID correlationId = UUID.randomUUID();
            String commandPayload = buildAuthorizePaymentPayload(reservation, correlationId);
            outboxRepository.save(new OutboxMessage(
                    "Reservation", reservation.getId(), "AuthorizePayment", commandPayload,
                    correlationId, TOPIC_PAYMENTS_COMMANDS));
            saga.setStep("RECOVERY_REEMIT_AUTHORIZE_PAYMENT");
            sagaInstanceRepository.save(saga);
            log.info("Recovery: re-emitted AuthorizePayment for saga={} reservation={}",
                    saga.getId(), reservation.getId());
            return;
        }

        if (reservation.getStatus() == ReservationStatus.HELD) {
            ReservationStateMachine.transition(reservation.getStatus(), ReservationStatus.RELEASED);
            reservation.setStatus(ReservationStatus.RELEASED);
            reservation.getSeats().forEach(seat -> seat.setStatus(SeatHoldStatus.RELEASED));
            reservationRepository.save(reservation);
            holdMirrorService.remove(reservation.getId());

            UUID correlationId = UUID.randomUUID();
            reservationService.appendEvent(reservation, "RESERVATION_RELEASED",
                    correlationId.toString(), null);

            String eventPayload = buildPayload(reservation, correlationId);
            outboxRepository.save(new OutboxMessage(
                    "Reservation", reservation.getId(), "SeatsReleased", eventPayload,
                    correlationId, TOPIC_RESERVATIONS_EVENTS));
        }

        saga.advance(SagaState.COMPENSATED, "RECOVERY_SWEEP_COMPENSATED");
        sagaInstanceRepository.save(saga);
        log.info("Recovery: compensated saga={} reservation={}", saga.getId(), reservation.getId());
    }

    private String buildPayload(Reservation reservation, UUID correlationId) {
        List<String> seatIds = reservation.getSeats().stream()
                .map(s -> s.getSeatId().toString())
                .toList();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("reservationId", reservation.getId());
        payload.put("showId", reservation.getShowId());
        payload.put("seatIds", seatIds);
        payload.put("status", reservation.getStatus());
        payload.put("correlationId", correlationId);
        return writeJson(payload, reservation.getId());
    }

    /**
     * STAM-442: the AuthorizePayment command additionally carries paymentMethodId
     * (set via PATCH .../payment-method before submit-payment) and amountCents,
     * computed server-side from catalog's seat prices rather than trusted from
     * the client.
     */
    private String buildAuthorizePaymentPayload(Reservation reservation, UUID correlationId) {
        List<UUID> seatIds = reservation.getSeats().stream()
                .map(s -> s.getSeatId())
                .toList();
        long amountCents = catalogClient.totalPriceCentsForSeats(reservation.getShowId(), seatIds);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("reservationId", reservation.getId());
        payload.put("showId", reservation.getShowId());
        payload.put("seatIds", seatIds.stream().map(UUID::toString).toList());
        payload.put("status", reservation.getStatus());
        payload.put("correlationId", correlationId);
        payload.put("paymentMethodId", reservation.getPaymentMethodId());
        payload.put("amountCents", amountCents);
        return writeJson(payload, reservation.getId());
    }

    private String writeJson(Map<String, Object> payload, UUID reservationId) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga payload for reservation " + reservationId, e);
        }
    }
}
