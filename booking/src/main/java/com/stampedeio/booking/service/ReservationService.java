package com.stampedeio.booking.service;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.api.ReservationResponse;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.exception.ConflictException;
import com.stampedeio.booking.exception.ResourceNotFoundException;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.ReservationSeatRepository;

@Service
public class ReservationService {

    private final ReservationRepository reservationRepository;
    private final ReservationSeatRepository reservationSeatRepository;
    private final CatalogClient catalogClient;
    private final Clock clock;

    public ReservationService(ReservationRepository reservationRepository,
                              ReservationSeatRepository reservationSeatRepository,
                              CatalogClient catalogClient,
                              Clock clock) {
        this.reservationRepository = reservationRepository;
        this.reservationSeatRepository = reservationSeatRepository;
        this.catalogClient = catalogClient;
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
            Reservation saved = reservationRepository.saveAndFlush(reservation);
            return new HoldResult(ReservationResponse.from(saved, Instant.now(clock)), false);
        } catch (DataIntegrityViolationException ex) {
            UUID conflictingSeat = reservationSeatRepository
                    .findFirstConflictingSeatId(request.showId(), request.seatIds())
                    .orElse(null);
            if (conflictingSeat != null) {
                throw new ConflictException("Seat " + conflictingSeat + " is already held");
            }
            // Race: another request with the same idempotency-key won the insert.
            Reservation replayed = reservationRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> ex);
            return new HoldResult(ReservationResponse.from(replayed, Instant.now(clock)), true);
        }
    }

    @Transactional(readOnly = true)
    public ReservationResponse get(UUID reservationId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResourceNotFoundException("Reservation", reservationId));
        return ReservationResponse.from(reservation, Instant.now(clock));
    }

    public record HoldResult(ReservationResponse response, boolean idempotentReplay) {
    }
}
