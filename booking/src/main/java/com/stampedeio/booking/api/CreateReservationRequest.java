package com.stampedeio.booking.api;

import java.util.List;
import java.util.UUID;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public record CreateReservationRequest(
        @NotNull UUID showId,
        @NotNull UUID userId,
        @NotEmpty @Schema(description = "Seat IDs to reserve") List<UUID> seatIds) {
}
