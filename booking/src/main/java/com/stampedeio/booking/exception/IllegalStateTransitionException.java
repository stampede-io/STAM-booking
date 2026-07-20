package com.stampedeio.booking.exception;

import com.stampedeio.booking.domain.ReservationStatus;

public class IllegalStateTransitionException extends ConflictException {

    public IllegalStateTransitionException(ReservationStatus from, ReservationStatus to) {
        super("Cannot transition reservation from " + from + " to " + to);
    }
}
