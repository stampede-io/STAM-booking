package com.stampedeio.booking.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import com.stampedeio.booking.exception.IllegalStateTransitionException;

public final class ReservationStateMachine {

    private static final Map<ReservationStatus, Set<ReservationStatus>> ALLOWED;

    static {
        EnumMap<ReservationStatus, Set<ReservationStatus>> m = new EnumMap<>(ReservationStatus.class);
        m.put(ReservationStatus.HELD, EnumSet.of(
                ReservationStatus.CONFIRMED,
                ReservationStatus.RELEASED,
                ReservationStatus.EXPIRED));
        m.put(ReservationStatus.EXPIRED, EnumSet.of(ReservationStatus.REFUNDED));
        ALLOWED = Map.copyOf(m);
    }

    private ReservationStateMachine() {
    }

    public static ReservationStatus transition(ReservationStatus current, ReservationStatus target) {
        Set<ReservationStatus> targets = ALLOWED.get(current);
        if (targets == null || !targets.contains(target)) {
            throw new IllegalStateTransitionException(current, target);
        }
        return target;
    }
}
