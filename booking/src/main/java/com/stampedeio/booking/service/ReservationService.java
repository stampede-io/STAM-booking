package com.stampedeio.booking.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.api.ReservationResponse;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.config.CorrelationIds;
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

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final ReservationRepository reservationRepository;
    private final ReservationSeatRepository reservationSeatRepository;
    private final ReservationEventRepository reservationEventRepository;
    private final OutboxRepository outboxRepository;
    private final CatalogClient catalogClient;
    private final HoldMirrorService holdMirrorService;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;
    private final ObjectMapper objectMapper;

    public ReservationService(ReservationRepository reservationRepository,
                              ReservationSeatRepository reservationSeatRepository,
                              ReservationEventRepository reservationEventRepository,
                              OutboxRepository outboxRepository,
                              CatalogClient catalogClient,
                              HoldMirrorService holdMirrorService,
                              TransactionTemplate transactionTemplate,
                              Clock clock,
                              ObjectMapper objectMapper) {
        this.reservationRepository = reservationRepository;
        this.reservationSeatRepository = reservationSeatRepository;
        this.reservationEventRepository = reservationEventRepository;
        this.outboxRepository = outboxRepository;
        this.catalogClient = catalogClient;
        this.holdMirrorService = holdMirrorService;
        this.transactionTemplate = transactionTemplate;
        this.clock = clock;
        this.objectMapper = objectMapper;
    }

    /**
     * The gateway (STAM-443) forwards the caller's correlation ID into MDC via
     * CorrelationIdFilter before the controller runs. CorrelationIds.currentOrNew()
     * always returns a valid UUID, so the outbox row's typed correlation_id column
     * is never null even if the inbound header was malformed.
     */
    private static String currentCorrelationId() {
        return CorrelationIds.currentOrNew().toString();
    }

    public HoldResult hold(UUID idempotencyKey, UUID callerUserId, CreateReservationRequest request) {
        Optional<Reservation> existing = reservationRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            checkOwnership(existing.get(), callerUserId);
            return new HoldResult(ReservationResponse.from(existing.get(), Instant.now(clock)), true);
        }

        catalogClient.validateSeatsForShow(request.showId(), request.seatIds());

        Reservation reservation = new Reservation(request.showId(), callerUserId, idempotencyKey);
        request.seatIds().forEach(reservation::addSeat);

        String correlationId = currentCorrelationId();
        try {
            Reservation saved = transactionTemplate.execute(status -> {
                Reservation persisted = reservationRepository.saveAndFlush(reservation);
                appendEvent(persisted, "SEATS_HELD", correlationId, null);
                appendOutbox(persisted, "SeatsHeld", correlationId);
                return persisted;
            });

            long ttlSeconds = Duration.between(Instant.now(clock), saved.getExpiresAt()).getSeconds();
            holdMirrorService.mirror(saved.getId(), ttlSeconds);
            log.info("Hold created reservation={} correlationId={}", saved.getId(), correlationId);
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
    public ReservationResponse confirm(UUID reservationId, UUID callerUserId, String paymentReference,
                                       String correlationId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));
        checkOwnership(reservation, callerUserId);

        ReservationStateMachine.transition(reservation.getStatus(), ReservationStatus.CONFIRMED);
        reservation.setStatus(ReservationStatus.CONFIRMED);

        reservation.getSeats().forEach(seat -> seat.setStatus(SeatHoldStatus.CONFIRMED));

        holdMirrorService.remove(reservationId);

        appendEvent(reservation, "RESERVATION_CONFIRMED", correlationId, paymentReference);
        appendOutbox(reservation, "ReservationConfirmed", correlationId);

        return ReservationResponse.from(reservationRepository.save(reservation), Instant.now(clock));
    }

    @Transactional
    public ReservationResponse release(UUID reservationId, UUID callerUserId, String correlationId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));
        checkOwnership(reservation, callerUserId);

        ReservationStateMachine.transition(reservation.getStatus(), ReservationStatus.RELEASED);
        reservation.setStatus(ReservationStatus.RELEASED);

        reservation.getSeats().forEach(seat -> seat.setStatus(SeatHoldStatus.RELEASED));

        holdMirrorService.remove(reservationId);

        appendEvent(reservation, "RESERVATION_RELEASED", correlationId, null);
        appendOutbox(reservation, "SeatsReleased", correlationId);

        return ReservationResponse.from(reservationRepository.save(reservation), Instant.now(clock));
    }

    @Transactional
    public ReservationResponse setPaymentMethod(UUID reservationId, UUID callerUserId, String paymentMethodId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));
        checkOwnership(reservation, callerUserId);

        if (reservation.getStatus() != ReservationStatus.HELD) {
            throw new ConflictException(
                    "Cannot set payment method for reservation in state " + reservation.getStatus());
        }

        reservation.setPaymentMethodId(paymentMethodId);
        return ReservationResponse.from(reservationRepository.save(reservation), Instant.now(clock));
    }

    @Transactional(readOnly = true)
    public ReservationResponse get(UUID reservationId, UUID callerUserId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));
        checkOwnership(reservation, callerUserId);
        return ReservationResponse.from(reservation, Instant.now(clock));
    }

    /**
     * STAM-447: 404, not 403 — a caller who doesn't own the reservation gets
     * the same response as one that doesn't exist, so the ID itself never
     * confirms ownership either way.
     */
    private static void checkOwnership(Reservation reservation, UUID callerUserId) {
        if (!reservation.getUserId().equals(callerUserId)) {
            throw new ResourceNotFoundException("Reservation", reservation.getId());
        }
    }

    public void appendOutbox(Reservation reservation, String eventType, String correlationId) {
        List<String> seatIds = reservation.getSeats().stream()
                .map(s -> s.getSeatId().toString())
                .toList();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("reservationId", reservation.getId());
        payload.put("showId", reservation.getShowId());
        payload.put("seatIds", seatIds);
        payload.put("status", reservation.getStatus());
        payload.put("correlationId", correlationId);

        UUID corrId = parseUuidOrNull(correlationId);
        outboxRepository.save(new OutboxMessage(
                "Reservation", reservation.getId(), eventType, writeJson(payload),
                corrId, "reservations.events"));
    }

    public void appendEvent(Reservation reservation, String eventType, String correlationId,
                            String paymentReference) {
        int nextSeq = reservationEventRepository.findMaxSeqByAggregateId(reservation.getId())
                .map(s -> s + 1)
                .orElse(1);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("state", reservation.getStatus());
        payload.put("occurredAt", Instant.now(clock).toString());
        payload.put("correlationId", correlationId);
        if (paymentReference != null) {
            payload.put("paymentReference", paymentReference);
        }

        reservationEventRepository.save(
                new ReservationEvent(reservation.getId(), nextSeq, eventType, writeJson(payload)));
    }

    /** Outbox/event payloads carry caller-supplied strings (correlation ID,
     * payment reference); building them by concatenation lets a value
     * containing a quote corrupt the JSON. */
    private String writeJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize payload: " + payload.keySet(), e);
        }
    }

    private static UUID parseUuidOrNull(String value) {
        if (value == null) return null;
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public record HoldResult(ReservationResponse response, boolean idempotentReplay) {
    }
}
