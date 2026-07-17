package com.stampedeio.booking.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.exception.ConflictException;
import com.stampedeio.booking.exception.UnprocessableEntityException;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.ReservationSeatRepository;

class ReservationServiceTest {

    private ReservationRepository reservationRepository;
    private ReservationSeatRepository reservationSeatRepository;
    private CatalogClient catalogClient;
    private ReservationService service;

    @BeforeEach
    void setUp() {
        reservationRepository = mock(ReservationRepository.class);
        reservationSeatRepository = mock(ReservationSeatRepository.class);
        catalogClient = mock(CatalogClient.class);
        // Use system clock — Reservation.createdAt is set from Instant.now() at construction,
        // so the derived expiresAt has to be compared against wall-clock time too.
        service = new ReservationService(
                reservationRepository, reservationSeatRepository, catalogClient, Clock.systemUTC());
    }

    @Test
    @DisplayName("AC1: happy path returns HELD reservation with expiresAt = now + 7min")
    void hold_success() {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        CreateReservationRequest req = new CreateReservationRequest(showId, userId, List.of(seatId));

        when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
        doNothing().when(catalogClient).validateSeatsForShow(showId, List.of(seatId));
        when(reservationRepository.saveAndFlush(any(Reservation.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        ReservationService.HoldResult result = service.hold(key, req);

        assertThat(result.idempotentReplay()).isFalse();
        assertThat(result.response().status()).isEqualTo("HELD");
        assertThat(result.response().seatIds()).containsExactly(seatId);
        assertThat(result.response().expiresAt()).isAfter(Instant.now());
        // 7 min = 420s; allow a small jitter for cross-clock-read drift within the assertion.
        assertThat(result.response().ttlSeconds()).isBetween(415L, 421L);
    }

    @Test
    @DisplayName("AC2: unique-violation on seat maps to ConflictException with seat id in detail")
    void hold_conflict_mapsToConflictException() {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        CreateReservationRequest req = new CreateReservationRequest(
                showId, UUID.randomUUID(), List.of(seatId));

        when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
        when(reservationRepository.saveAndFlush(any(Reservation.class)))
                .thenThrow(new DataIntegrityViolationException("unique_violation"));
        when(reservationSeatRepository.findFirstConflictingSeatId(eq(showId), eq(List.of(seatId))))
                .thenReturn(Optional.of(seatId));

        assertThatThrownBy(() -> service.hold(key, req))
                .isInstanceOf(ConflictException.class)
                .hasMessageContaining(seatId.toString())
                .hasMessageContaining("is already held");
    }

    @Test
    @DisplayName("AC3: replay with same idempotency-key returns cached response, no new insert")
    void hold_idempotentReplay() {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        Reservation existing = new Reservation(showId, userId, key);
        existing.addSeat(seatId);

        when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.of(existing));

        ReservationService.HoldResult result =
                service.hold(key, new CreateReservationRequest(showId, userId, List.of(seatId)));

        assertThat(result.idempotentReplay()).isTrue();
        assertThat(result.response().seatIds()).containsExactly(seatId);
        verifyNoInteractions(catalogClient);
        verify(reservationRepository, org.mockito.Mockito.never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("AC4: catalog 404 propagates as UnprocessableEntityException; no DB insert")
    void hold_invalidSeats_bubblesUp422() {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        CreateReservationRequest req = new CreateReservationRequest(
                showId, UUID.randomUUID(), List.of(seatId));

        when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
        doThrow(new UnprocessableEntityException("invalid seats"))
                .when(catalogClient).validateSeatsForShow(showId, List.of(seatId));

        assertThatThrownBy(() -> service.hold(key, req))
                .isInstanceOf(UnprocessableEntityException.class);
        verify(reservationRepository, org.mockito.Mockito.never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Conflict race: unique-violation without a matching seat means idempotency-key collision under load")
    void hold_conflict_withNoSeatMatch_returnsIdempotentReplay() {
        UUID key = UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        CreateReservationRequest req = new CreateReservationRequest(showId, userId, List.of(seatId));

        Reservation winner = new Reservation(showId, userId, key);
        winner.addSeat(seatId);

        // First lookup: not there yet (racing). Insert throws unique-violation.
        // Seat conflict lookup: empty (the collision was on idempotency_key, not seat).
        // Second lookup: the winner is now visible.
        when(reservationRepository.findByIdempotencyKey(key))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(winner));
        when(reservationRepository.saveAndFlush(any(Reservation.class)))
                .thenThrow(new DataIntegrityViolationException("unique_violation on idempotency_key"));
        when(reservationSeatRepository.findFirstConflictingSeatId(showId, List.of(seatId)))
                .thenReturn(Optional.empty());

        ReservationService.HoldResult result = service.hold(key, req);

        assertThat(result.idempotentReplay()).isTrue();
    }
}
