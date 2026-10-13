package com.stampedeio.booking.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import com.stampedeio.booking.domain.SagaInstance;
import com.stampedeio.booking.domain.SagaState;
import com.stampedeio.booking.repository.SagaInstanceRepository;
import com.stampedeio.booking.saga.BookingSagaOrchestrator;

@Component
public class SagaRecoverySweep {

    private static final Logger log = LoggerFactory.getLogger(SagaRecoverySweep.class);
    private static final List<String> TERMINAL_STATES = List.of(
            SagaState.COMPLETED.name(), SagaState.COMPENSATED.name());

    private final SagaInstanceRepository sagaInstanceRepository;
    private final BookingSagaOrchestrator sagaOrchestrator;
    private final Clock clock;
    private final Duration staleAfter;

    public SagaRecoverySweep(SagaInstanceRepository sagaInstanceRepository,
                             BookingSagaOrchestrator sagaOrchestrator,
                             Clock clock,
                             @Value("${saga.recovery.stale-after:PT10M}") Duration staleAfter,
                             MeterRegistry meterRegistry) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaOrchestrator = sagaOrchestrator;
        this.clock = clock;
        this.staleAfter = staleAfter;
        // STAM-66 / AC2 (SagaStuck): same TERMINAL_STATES/staleAfter this
        // sweep already recovers against -- the alert and the self-healing
        // sweep watch the identical condition, so SagaStuck should start
        // firing right around when recoverStaleSaga() would already be
        // acting on it, not some separately-tuned threshold.
        meterRegistry.gauge("saga_oldest_non_terminal_age_seconds", this, SagaRecoverySweep::oldestNonTerminalAgeSeconds);
    }

    private double oldestNonTerminalAgeSeconds() {
        return sagaInstanceRepository.findOldestNonTerminalUpdatedAt(TERMINAL_STATES)
                .map(oldest -> Duration.between(oldest, Instant.now(clock)).getSeconds())
                .map(Long::doubleValue)
                .orElse(0.0);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        sweep();
    }

    @Scheduled(fixedRate = 60_000)
    public void scheduledSweep() {
        sweep();
    }

    public void sweep() {
        Instant cutoff = Instant.now(clock).minus(staleAfter);
        List<SagaInstance> stale = sagaInstanceRepository
                .findByStateNotInAndUpdatedAtBefore(TERMINAL_STATES, cutoff);

        if (stale.isEmpty()) {
            log.debug("Recovery sweep: no stale sagas found");
            return;
        }

        log.info("Recovery sweep: found {} stale saga(s)", stale.size());
        for (SagaInstance saga : stale) {
            try {
                sagaOrchestrator.recoverStaleSaga(saga.getId());
            } catch (Exception e) {
                log.error("Recovery sweep: failed to recover saga={}", saga.getId(), e);
            }
        }
    }
}
