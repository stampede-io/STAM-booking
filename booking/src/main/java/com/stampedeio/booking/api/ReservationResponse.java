package com.stampedeio.booking.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.domain.ReservationSeat;

public record ReservationResponse(
        UUID reservationId,
        UUID showId,
        UUID userId,
        String status,
        List<UUID> seatIds,
        Instant expiresAt,
        long ttlSeconds) {

    public static ReservationResponse from(Reservation reservation, Instant now) {
        Instant expiresAt = reservation.getExpiresAt();
        long ttl = Math.max(0, expiresAt.getEpochSecond() - now.getEpochSecond());
        List<UUID> seatIds = reservation.getSeats().stream()
                .map(ReservationSeat::getSeatId)
                .toList();
        return new ReservationResponse(
                reservation.getId(),
                reservation.getShowId(),
                reservation.getUserId(),
                reservation.getStatus().name(),
                seatIds,
                expiresAt,
                ttl);
    }
}
