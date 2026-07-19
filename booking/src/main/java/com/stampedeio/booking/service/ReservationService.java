package com.stampedeio.booking.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.api.ReservationResponse;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.domain.ReservationEvent;
import com.stampedeio.booking.domain.ReservationStateMachine;
import com.stampedeio.booking.domain.ReservationStatus;
import com.stampedeio.booking.domain.SeatHoldStatus;
import com.stampedeio.booking.exception.ConflictException;
import com.stampedeio.booking.exception.ResourceNotFoundException;
import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.repository.OutboxRepository;
import com.stampedeio.booking.repository.ReservationEventRepository;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.ReservationSeatRepository;

@Service
public class ReservationService {

    private final ReservationRepository reservationRepository;
    private final ReservationSeatRepository reservationSeatRepository;
    private final ReservationEventRepository reservationEventRepository;
    private final OutboxRepository outboxRepository;
    private final CatalogClient catalogClient;
    private final HoldMirrorService holdMirrorService;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;

    public ReservationService(ReservationRepository reservationRepository,
                              ReservationSeatRepository reservationSeatRepository,
                              ReservationEventRepository reservationEventRepository,
                              OutboxRepository outboxRepository,
                              CatalogClient catalogClient,
                              HoldMirrorService holdMirrorService,
                              TransactionTemplate transactionTemplate,
                              Clock clock) {
        this.reservationRepository = reservationRepository;
        this.reservationSeatRepository = reservationSeatRepository;
        this.reservationEventRepository = reservationEventRepository;
        this.outboxRepository = outboxRepository;
        this.catalogClient = catalogClient;
        this.holdMirrorService = holdMirrorService;
        this.transactionTemplate = transactionTemplate;
        this.clock = clock;
    }

    public HoldResult hold(UUID idempotencyKey, CreateReservationRequest request) {
        Optional<Reservation> existing = reservationRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            return new HoldResult(ReservationResponse.from(existing.get(), Instant.now(clock)), true);
        }

        catalogClient.validateSeatsForShow(request.showId(), request.seatIds());

        Reservation reservation = new Reservation(request.showId(), request.userId(), idempotencyKey);
        request.seatIds().forEach(reservation::addSeat);

        try {
            Reservation saved = transactionTemplate.execute(status -> {
                Reservation persisted = reservationRepository.saveAndFlush(reservation);
                String correlationId = UUID.randomUUID().toString();
                appendEvent(persisted, "SEATS_HELD", correlationId, null);
                appendOutbox(persisted, "SeatsHeld", correlationId);
                return persisted;
            });

            long ttlSeconds = Duration.between(Instant.now(clock), saved.getExpiresAt()).getSeconds();
            holdMirrorService.mirror(saved.getId(), ttlSeconds);
            return new HoldResult(ReservationResponse.from(saved, Instant.now(clock)), false);
        } catch (DataIntegrityViolationException ex) {
            UUID conflictingSeat = reservationSeatRepository
                    .findFirstConflictingSeatId(request.showId(), request.seatIds())
                    .orElse(null);
            if (conflictingSeat != null) {
                throw new ConflictException("Seat " + conflictingSeat + " is already held");
            }
            Reservation replayed = reservationRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> ex);
            return new HoldResult(ReservationResponse.from(replayed, Instant.now(clock)), true);
        }
    }

    @Transactional
    public ReservationResponse confirm(UUID reservationId, String paymentReference, String correlationId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));

        ReservationStateMachine.transition(reservation.getStatus(), ReservationStatus.CONFIRMED);
        reservation.setStatus(ReservationStatus.CONFIRMED);

        reservation.getSeats().forEach(seat -> seat.setStatus(SeatHoldStatus.CONFIRMED));

        holdMirrorService.remove(reservationId);

        appendEvent(reservation, "RESERVATION_CONFIRMED", correlationId, paymentReference);
        appendOutbox(reservation, "ReservationConfirmed", correlationId);

        return ReservationResponse.from(reservationRepository.save(reservation), Instant.now(clock));
    }

    @Transactional
    public ReservationResponse release(UUID reservationId, String correlationId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));

        ReservationStateMachine.transition(reservation.getStatus(), ReservationStatus.RELEASED);
        reservation.setStatus(ReservationStatus.RELEASED);

        reservation.getSeats().forEach(seat -> seat.setStatus(SeatHoldStatus.RELEASED));

        holdMirrorService.remove(reservationId);

        appendEvent(reservation, "RESERVATION_RELEASED", correlationId, null);
        appendOutbox(reservation, "SeatsReleased", correlationId);

        return ReservationResponse.from(reservationRepository.save(reservation), Instant.now(clock));
    }

    @Transactional(readOnly = true)
    public ReservationResponse get(UUID reservationId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));
        return ReservationResponse.from(reservation, Instant.now(clock));
    }

    void appendOutbox(Reservation reservation, String eventType, String correlationId) {
        String seatIds = reservation.getSeats().stream()
                .map(s -> "\"" + s.getSeatId() + "\"")
                .collect(java.util.stream.Collectors.joining(","));
        String payload = "{\"reservationId\":\"" + reservation.getId()
                + "\",\"showId\":\"" + reservation.getShowId()
                + "\",\"seatIds\":[" + seatIds + "]"
                + ",\"status\":\"" + reservation.getStatus()
                + "\",\"correlationId\":\"" + correlationId + "\"}";
        outboxRepository.save(new OutboxMessage("Reservation", reservation.getId(), eventType, payload));
    }

    void appendEvent(Reservation reservation, String eventType, String correlationId,
                     String paymentReference) {
        int nextSeq = reservationEventRepository.findMaxSeqByAggregateId(reservation.getId())
                .map(s -> s + 1)
                .orElse(1);

        String occurredAt = Instant.now(clock).toString();
        StringBuilder payload = new StringBuilder();
        payload.append("{\"state\":\"").append(reservation.getStatus())
                .append("\",\"occurredAt\":\"").append(occurredAt)
                .append("\",\"correlationId\":\"").append(correlationId).append("\"");
        if (paymentReference != null) {
            payload.append(",\"paymentReference\":\"").append(paymentReference).append("\"");
        }
        payload.append("}");

        reservationEventRepository.save(
                new ReservationEvent(reservation.getId(), nextSeq, eventType, payload.toString()));
    }

    public record HoldResult(ReservationResponse response, boolean idempotentReplay) {
    }
}
