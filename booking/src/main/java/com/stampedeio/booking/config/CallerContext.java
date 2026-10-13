package com.stampedeio.booking.config;

import java.util.UUID;

import org.springframework.security.oauth2.jwt.Jwt;

/** STAM-447: the caller's identity, as identity's JWT puts it in the user_id claim. */
public final class CallerContext {

    private CallerContext() {
    }

    public static UUID userId(Jwt jwt) {
        String raw = jwt.getClaimAsString("user_id");
        if (raw == null || raw.isBlank()) {
            throw new IllegalStateException("JWT is missing the user_id claim");
        }
        return UUID.fromString(raw);
    }
}
