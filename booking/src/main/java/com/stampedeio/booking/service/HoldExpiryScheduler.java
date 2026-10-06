package com.stampedeio.booking.service;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.saga.BookingSagaOrchestrator;

@Component
public class HoldExpiryScheduler {

    private static final Logger log = LoggerFactory.getLogger(HoldExpiryScheduler.class);

    private final ReservationRepository reservationRepository;
    private final BookingSagaOrchestrator sagaOrchestrator;
    private final Clock clock;

    public HoldExpiryScheduler(ReservationRepository reservationRepository,
                               BookingSagaOrchestrator sagaOrchestrator,
                               Clock clock) {
        this.reservationRepository = reservationRepository;
        this.sagaOrchestrator = sagaOrchestrator;
        this.clock = clock;
    }

    // STAM-446: this is the only sweep trigger. @Scheduled(fixedRate = ...)
    // with no initialDelay already fires as soon as the scheduler starts, so
    // a separate ApplicationReadyEvent listener calling the same method raced
    // it on every boot — both read the same expired holds, both computed the
    // same next event seq, and the loser's insert crashed the app on an
    // ApplicationReadyEvent listener, which aborts Spring Boot startup.
    @Scheduled(fixedRate = 30_000)
    public void scheduledExpiry() {
        expireStaleHolds();
    }

    public void expireStaleHolds() {
        List<Reservation> expired = reservationRepository.findExpiredHolds(Instant.now(clock));
        if (expired.isEmpty()) {
            return;
        }

        int processed = 0;
        for (Reservation reservation : expired) {
            UUID reservationId = reservation.getId();
            try {
                sagaOrchestrator.expireHeldReservation(reservationId);
                processed++;
            } catch (DataIntegrityViolationException e) {
                // A concurrent sweep (this instance's previous run still
                // finishing, or — once there's more than one replica —
                // another instance) committed this reservation's expiry
                // first. Not an error: the reservation is expired either way.
                log.info("Reservation={} already expired by a concurrent sweep, skipping", reservationId);
            }
        }
        log.info("Processed {} of {} stale hold(s) for expiry", processed, expired.size());
    }
}
