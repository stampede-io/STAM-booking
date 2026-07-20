package com.stampedeio.booking.domain;

public enum SagaState {
    STARTED,
    PAYMENT_REQUESTED,
    CONFIRMED,
    COMPENSATING,
    COMPENSATED,
    COMPLETED
}
