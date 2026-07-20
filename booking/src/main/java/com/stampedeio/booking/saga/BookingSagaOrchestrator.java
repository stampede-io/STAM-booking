package com.stampedeio.booking.saga;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.domain.ReservationStateMachine;
import com.stampedeio.booking.domain.ReservationStatus;
import com.stampedeio.booking.domain.SagaInstance;
import com.stampedeio.booking.domain.SagaState;
import com.stampedeio.booking.domain.SeatHoldStatus;
import com.stampedeio.booking.exception.ResourceNotFoundException;
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

    public BookingSagaOrchestrator(SagaInstanceRepository sagaInstanceRepository,
                                   ReservationRepository reservationRepository,
                                   OutboxRepository outboxRepository,
                                   ReservationService reservationService,
                                   HoldMirrorService holdMirrorService) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.reservationRepository = reservationRepository;
        this.outboxRepository = outboxRepository;
        this.reservationService = reservationService;
        this.holdMirrorService = holdMirrorService;
    }

    @Transactional
    public SagaInstance startSaga(UUID reservationId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));

        if (reservation.getStatus() != ReservationStatus.HELD) {
            throw new IllegalStateException(
                    "Cannot start saga for reservation in state " + reservation.getStatus());
        }

        // Idempotent: if saga already exists for this reservation, return it
        return sagaInstanceRepository.findByReservationId(reservationId)
                .orElseGet(() -> {
                    UUID correlationId = UUID.randomUUID();
                    SagaInstance saga = new SagaInstance(SAGA_TYPE, reservation);
                    saga.advance(SagaState.PAYMENT_REQUESTED, "EMIT_AUTHORIZE_PAYMENT");

                    String commandPayload = buildPayload(reservation, correlationId);
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

    private String buildPayload(Reservation reservation, UUID correlationId) {
        String seatIds = reservation.getSeats().stream()
                .map(s -> "\"" + s.getSeatId() + "\"")
                .collect(java.util.stream.Collectors.joining(","));
        return "{\"reservationId\":\"" + reservation.getId()
                + "\",\"showId\":\"" + reservation.getShowId()
                + "\",\"seatIds\":[" + seatIds + "]"
                + ",\"status\":\"" + reservation.getStatus()
                + "\",\"correlationId\":\"" + correlationId + "\"}";
    }
}
