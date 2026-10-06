package com.stampedeio.booking.config;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Single source of truth for reading the gateway-forwarded correlation ID
 * (STAM-443) out of MDC. {@link #currentOrNew()} always returns a valid UUID:
 * a missing or malformed value is logged and replaced, so every outbox row
 * still gets a non-null correlation_id.
 */
public final class CorrelationIds {

    private static final Logger log = LoggerFactory.getLogger(CorrelationIds.class);
    static final String MDC_KEY = "correlationId";

    private CorrelationIds() {
    }

    public static UUID currentOrNew() {
        String id = MDC.get(MDC_KEY);
        if (id == null || id.isBlank()) {
            return UUID.randomUUID();
        }
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            log.warn("Discarding malformed correlation id from MDC: {}", id);
            return UUID.randomUUID();
        }
    }
}
