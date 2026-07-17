package com.stampedeio.booking.event;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String eventType,
        int version,
        Instant occurredAt,
        UUID correlationId,
        UUID aggregateId,
        Object payload
) {

    public static EventEnvelope create(String eventType, int version,
                                       UUID correlationId, UUID aggregateId,
                                       Object payload) {
        return new EventEnvelope(
                UUID.randomUUID(),
                eventType,
                version,
                Instant.now(),
                correlationId,
                aggregateId,
                payload
        );
    }
}
