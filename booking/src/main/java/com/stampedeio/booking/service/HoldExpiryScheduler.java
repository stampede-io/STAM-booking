package com.stampedeio.booking.service;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.domain.ReservationStateMachine;
import com.stampedeio.booking.domain.ReservationStatus;
import com.stampedeio.booking.domain.SagaInstance;
import com.stampedeio.booking.domain.SagaState;
import com.stampedeio.booking.domain.SeatHoldStatus;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.SagaInstanceRepository;

@Component
public class HoldExpiryScheduler {

    private static final Logger log = LoggerFactory.getLogger(HoldExpiryScheduler.class);

    private final ReservationRepository reservationRepository;
    private final SagaInstanceRepository sagaInstanceRepository;
    private final ReservationService reservationService;
    private final HoldMirrorService holdMirrorService;
    private final Clock clock;

    public HoldExpiryScheduler(ReservationRepository reservationRepository,
                               SagaInstanceRepository sagaInstanceRepository,
                               ReservationService reservationService,
                               HoldMirrorService holdMirrorService,
                               Clock clock) {
        this.reservationRepository = reservationRepository;
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.reservationService = reservationService;
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
            ReservationStateMachine.transition(reservation.getStatus(), ReservationStatus.EXPIRED);
            reservation.setStatus(ReservationStatus.EXPIRED);
            reservation.getSeats().forEach(seat -> seat.setStatus(SeatHoldStatus.RELEASED));
            holdMirrorService.remove(reservation.getId());

            String correlationId = UUID.randomUUID().toString();
            reservationService.appendEvent(reservation, "RESERVATION_EXPIRED", correlationId, null);
            reservationService.appendOutbox(reservation, "HoldExpired", correlationId);

            sagaInstanceRepository.findByReservationId(reservation.getId()).ifPresent(saga -> {
                if (!SagaState.COMPENSATED.name().equals(saga.getState())
                        && !SagaState.COMPLETED.name().equals(saga.getState())) {
                    saga.advance(SagaState.COMPENSATING, "HOLD_EXPIRED");
                    saga.advance(SagaState.COMPENSATED, "HOLD_EXPIRED_SEATS_FREED");
                    sagaInstanceRepository.save(saga);
                    log.info("Saga compensated via hold expiry for reservation={}", reservation.getId());
                }
            });
        }
        reservationRepository.saveAll(expired);
        log.info("Expired {} stale hold(s)", expired.size());
    }
}
