package com.stampedeio.booking.service;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.domain.ReservationSeat;
import com.stampedeio.booking.domain.ReservationStatus;
import com.stampedeio.booking.domain.SeatHoldStatus;
import com.stampedeio.booking.repository.ReservationRepository;

@Component
public class HoldExpiryScheduler {

    private static final Logger log = LoggerFactory.getLogger(HoldExpiryScheduler.class);

    private final ReservationRepository reservationRepository;
    private final HoldMirrorService holdMirrorService;
    private final Clock clock;

    public HoldExpiryScheduler(ReservationRepository reservationRepository,
                               HoldMirrorService holdMirrorService,
                               Clock clock) {
        this.reservationRepository = reservationRepository;
        this.holdMirrorService = holdMirrorService;
        this.clock = clock;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        expireStaleHolds();
    }

    @Scheduled(fixedRate = 30_000)
    public void scheduledExpiry() {
        expireStaleHolds();
    }

    @Transactional
    public void expireStaleHolds() {
        List<Reservation> expired = reservationRepository.findExpiredHolds(Instant.now(clock));
        if (expired.isEmpty()) {
            return;
        }
        for (Reservation reservation : expired) {
            reservation.setStatus(ReservationStatus.EXPIRED);
            for (ReservationSeat seat : reservation.getSeats()) {
                seat.setStatus(SeatHoldStatus.RELEASED);
            }
            holdMirrorService.remove(reservation.getId());
        }
        reservationRepository.saveAll(expired);
        log.info("Expired {} stale hold(s)", expired.size());
    }
}
