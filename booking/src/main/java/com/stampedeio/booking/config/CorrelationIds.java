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
        return parseOrNew(MDC.get(MDC_KEY));
    }

    /**
     * Normalizes a raw, caller-supplied value (e.g. the inbound X-Correlation-Id
     * header) into a valid UUID. CorrelationIdFilter calls this once at the
     * request edge so MDC, the echoed response header and every outbox row
     * written downstream all agree on the same value, even when the inbound
     * header was blank or malformed.
     */
    public static UUID parseOrNew(String raw) {
        if (raw == null || raw.isBlank()) {
            return UUID.randomUUID();
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            log.warn("Discarding malformed correlation id: {}", raw);
            return UUID.randomUUID();
        }
    }
}
