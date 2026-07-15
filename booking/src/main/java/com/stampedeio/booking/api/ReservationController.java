package com.stampedeio.booking.api;

import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

@RestController
@RequestMapping("/api/v1/reservations")
@Tag(name = "Reservations", description = "Seat reservation lifecycle")
public class ReservationController {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(
            summary = "Create a reservation",
            description = "Reserves seats for a show. The Idempotency-Key header ensures safe retries: "
                    + "if the same key is replayed, the server returns the original response "
                    + "instead of creating a duplicate reservation.")
    @ApiResponse(responseCode = "201", description = "Reservation created")
    @ApiResponse(responseCode = "400", description = "Invalid request body",
            content = @Content(mediaType = "application/problem+json",
                    schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "409", description = "Seat already reserved or idempotent replay conflict",
            content = @Content(mediaType = "application/problem+json",
                    schema = @Schema(implementation = ProblemDetail.class)))
    public ReservationResponse createReservation(
            @Parameter(
                    description = "Client-generated UUID for idempotent retries. "
                            + "If a request with this key was already processed, "
                            + "the original response is returned without creating a duplicate.",
                    required = true,
                    example = "550e8400-e29b-41d4-a716-446655440000")
            @RequestHeader("Idempotency-Key") UUID idempotencyKey,
            @Valid @RequestBody CreateReservationRequest request) {

        // TODO: delegate to reservation service (STAM-booking saga story)
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public record CreateReservationRequest(
            @NotNull UUID showId,
            @NotNull @Schema(description = "Seat IDs to reserve") java.util.List<UUID> seatIds) {
    }

    public record ReservationResponse(
            UUID reservationId,
            String status) {
    }
}
