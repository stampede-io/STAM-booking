package com.stampedeio.booking.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stampedeio.booking.exception.ConflictException;
import com.stampedeio.booking.exception.GlobalExceptionHandler;
import com.stampedeio.booking.exception.ResourceNotFoundException;
import com.stampedeio.booking.exception.UnprocessableEntityException;
import com.stampedeio.booking.service.ReservationService;
import com.stampedeio.booking.service.ReservationService.HoldResult;

@WebMvcTest(ReservationController.class)
@Import(GlobalExceptionHandler.class)
class ReservationControllerTest {

    @Autowired
    private MockMvc mvc;

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @MockitoBean
    private ReservationService reservationService;

    @Test
    @DisplayName("AC1: POST /reservations returns 201 with HELD status and expiresAt")
    void post_returns201() throws Exception {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        Instant expiresAt = Instant.parse("2026-07-17T10:07:00Z");

        ReservationResponse resp = new ReservationResponse(
                reservationId, showId, userId, "HELD",
                List.of(seatId), expiresAt, 420);

        when(reservationService.hold(eq(key), any())).thenReturn(new HoldResult(resp, false));

        var body = objectMapper.writeValueAsString(
                new CreateReservationRequest(showId, userId, List.of(seatId)));

        mvc.perform(post("/api/v1/reservations")
                        .header("Idempotency-Key", key.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.reservationId").value(reservationId.toString()))
                .andExpect(jsonPath("$.status").value("HELD"))
                .andExpect(jsonPath("$.expiresAt").exists())
                .andExpect(jsonPath("$.ttlSeconds").value(420));
    }

    @Test
    @DisplayName("AC3: idempotent replay returns 200 with original body")
    void post_replay_returns200() throws Exception {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        ReservationResponse resp = new ReservationResponse(
                UUID.randomUUID(), showId, userId, "HELD",
                List.of(seatId), Instant.parse("2026-07-17T10:07:00Z"), 200);

        when(reservationService.hold(eq(key), any())).thenReturn(new HoldResult(resp, true));

        var body = objectMapper.writeValueAsString(
                new CreateReservationRequest(showId, userId, List.of(seatId)));

        mvc.perform(post("/api/v1/reservations")
                        .header("Idempotency-Key", key.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("HELD"));
    }

    @Test
    @DisplayName("AC2: ConflictException maps to 409 problem+json with seat id in detail")
    void post_conflict_returns409() throws Exception {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        when(reservationService.hold(eq(key), any()))
                .thenThrow(new ConflictException("Seat " + seatId + " is already held"));

        var body = objectMapper.writeValueAsString(
                new CreateReservationRequest(showId, UUID.randomUUID(), List.of(seatId)));

        mvc.perform(post("/api/v1/reservations")
                        .header("Idempotency-Key", key.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isConflict())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Conflict"))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.detail").value("Seat " + seatId + " is already held"));
    }

    @Test
    @DisplayName("AC4: UnprocessableEntityException maps to 422 problem+json")
    void post_invalidSeats_returns422() throws Exception {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        when(reservationService.hold(eq(key), any()))
                .thenThrow(new UnprocessableEntityException(
                        "One or more seat IDs are invalid or do not belong to show " + showId));

        var body = objectMapper.writeValueAsString(
                new CreateReservationRequest(showId, UUID.randomUUID(), List.of(seatId)));

        mvc.perform(post("/api/v1/reservations")
                        .header("Idempotency-Key", key.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Unprocessable Entity"))
                .andExpect(jsonPath("$.status").value(422))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("invalid")));
    }

    @Test
    @DisplayName("400 problem+json when required body fields missing")
    void post_missingFields_returns400() throws Exception {
        mvc.perform(post("/api/v1/reservations")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Bad Request"));
    }

    @Test
    @DisplayName("400 when Idempotency-Key header is missing")
    void post_missingHeader_returns400() throws Exception {
        var body = objectMapper.writeValueAsString(new CreateReservationRequest(
                UUID.randomUUID(), UUID.randomUUID(), List.of(UUID.randomUUID())));

        mvc.perform(post("/api/v1/reservations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("AC5: GET /reservations/{id} returns expiresAt and ttlSeconds")
    void get_returnsReservation() throws Exception {
        UUID reservationId = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        ReservationResponse resp = new ReservationResponse(
                reservationId, showId, userId, "HELD",
                List.of(seatId), Instant.parse("2026-07-17T10:07:00Z"), 300);

        when(reservationService.get(reservationId)).thenReturn(resp);

        mvc.perform(get("/api/v1/reservations/{id}", reservationId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.reservationId").value(reservationId.toString()))
                .andExpect(jsonPath("$.expiresAt").exists())
                .andExpect(jsonPath("$.ttlSeconds").value(300));
    }

    @Test
    @DisplayName("GET /reservations/{id} returns 404 problem+json when not found")
    void get_notFound_returns404() throws Exception {
        UUID reservationId = UUID.randomUUID();
        when(reservationService.get(reservationId))
                .thenThrow(new ResourceNotFoundException("Reservation", reservationId));

        mvc.perform(get("/api/v1/reservations/{id}", reservationId))
                .andExpect(status().isNotFound())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Not Found"));
    }

    @Test
    @DisplayName("Response is application/json (not problem+json) on success")
    void post_success_returnsJson() throws Exception {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        when(reservationService.hold(eq(key), any())).thenReturn(new HoldResult(
                new ReservationResponse(UUID.randomUUID(), showId, userId, "HELD",
                        List.of(seatId), Instant.now(), 420),
                false));

        var body = objectMapper.writeValueAsString(
                new CreateReservationRequest(showId, userId, List.of(seatId)));

        mvc.perform(post("/api/v1/reservations")
                        .header("Idempotency-Key", key.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(header().string("Content-Type",
                        org.hamcrest.Matchers.containsString(MediaType.APPLICATION_JSON_VALUE)));
    }
}
