package com.stampedeio.booking.api;

import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.stampedeio.booking.service.ReservationService;
import com.stampedeio.booking.service.ReservationService.HoldResult;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/reservations")
@Tag(name = "Reservations", description = "Seat reservation lifecycle")
public class ReservationController {

    private final ReservationService reservationService;

    public ReservationController(ReservationService reservationService) {
        this.reservationService = reservationService;
    }

    @PostMapping
    @Operation(
            summary = "Create (hold) a reservation",
            description = "Holds seats for 7 minutes. If the Idempotency-Key was already used, "
                    + "returns 200 with the original response instead of creating a duplicate.")
    @ApiResponse(responseCode = "201", description = "Reservation created")
    @ApiResponse(responseCode = "200", description = "Idempotent replay: original response returned")
    @ApiResponse(responseCode = "400", description = "Invalid request body",
            content = @Content(mediaType = "application/problem+json",
                    schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "409", description = "One of the requested seats is already held",
            content = @Content(mediaType = "application/problem+json",
                    schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "422", description = "Seat IDs invalid for the given show",
            content = @Content(mediaType = "application/problem+json",
                    schema = @Schema(implementation = ProblemDetail.class)))
    public ResponseEntity<ReservationResponse> createReservation(
            @Parameter(
                    description = "Client-generated UUID for idempotent retries.",
                    required = true,
                    example = "550e8400-e29b-41d4-a716-446655440000")
            @RequestHeader("Idempotency-Key") UUID idempotencyKey,
            @Valid @RequestBody CreateReservationRequest request) {

        HoldResult result = reservationService.hold(idempotencyKey, request);
        HttpStatus status = result.idempotentReplay() ? HttpStatus.OK : HttpStatus.CREATED;
        return ResponseEntity.status(status).body(result.response());
    }

    @GetMapping("/{reservationId}")
    @Operation(summary = "Get reservation by id")
    @ApiResponse(responseCode = "200", description = "Reservation found")
    @ApiResponse(responseCode = "404", description = "No reservation with that id",
            content = @Content(mediaType = "application/problem+json",
                    schema = @Schema(implementation = ProblemDetail.class)))
    public ReservationResponse getReservation(@PathVariable UUID reservationId) {
        return reservationService.get(reservationId);
    }
}
