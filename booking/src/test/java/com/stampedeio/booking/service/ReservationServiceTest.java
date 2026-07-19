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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.api.ReservationResponse;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.domain.ReservationEvent;
import com.stampedeio.booking.exception.ConflictException;
import com.stampedeio.booking.exception.IllegalStateTransitionException;
import com.stampedeio.booking.exception.ResourceNotFoundException;
import com.stampedeio.booking.exception.UnprocessableEntityException;
import org.springframework.transaction.support.TransactionTemplate;

import com.stampedeio.booking.repository.OutboxRepository;
import com.stampedeio.booking.repository.ReservationEventRepository;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.ReservationSeatRepository;

class ReservationServiceTest {

    private ReservationRepository reservationRepository;
    private ReservationSeatRepository reservationSeatRepository;
    private ReservationEventRepository reservationEventRepository;
    private OutboxRepository outboxRepository;
    private CatalogClient catalogClient;
    private HoldMirrorService holdMirrorService;
    private TransactionTemplate transactionTemplate;
    private ReservationService service;

    @BeforeEach
    void setUp() {
        reservationRepository = mock(ReservationRepository.class);
        reservationSeatRepository = mock(ReservationSeatRepository.class);
        reservationEventRepository = mock(ReservationEventRepository.class);
        outboxRepository = mock(OutboxRepository.class);
        catalogClient = mock(CatalogClient.class);
        holdMirrorService = mock(HoldMirrorService.class);
        transactionTemplate = mock(TransactionTemplate.class);
        when(transactionTemplate.execute(any())).thenAnswer(inv -> {
            org.springframework.transaction.support.TransactionCallback<?> callback = inv.getArgument(0);
            return callback.doInTransaction(null);
        });
        when(reservationEventRepository.findMaxSeqByAggregateId(any())).thenReturn(Optional.empty());
        when(reservationEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        service = new ReservationService(
                reservationRepository, reservationSeatRepository, reservationEventRepository,
                outboxRepository, catalogClient, holdMirrorService, transactionTemplate,
                Clock.systemUTC());
    }

    @Nested
    @DisplayName("hold()")
    class HoldTests {

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

    @Nested
    @DisplayName("confirm()")
    class ConfirmTests {

        @Test
        @DisplayName("AC2: confirm HELD reservation returns CONFIRMED with 200")
        void confirm_heldReservation_succeeds() {
            UUID reservationId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
            when(reservationEventRepository.findMaxSeqByAggregateId(any())).thenReturn(Optional.empty());
            when(reservationEventRepository.save(any(ReservationEvent.class)))
                    .thenAnswer(inv -> inv.getArgument(0));
            when(reservationRepository.save(any(Reservation.class)))
                    .thenAnswer(inv -> inv.getArgument(0));

            ReservationResponse response = service.confirm(reservationId, "PAY-123", "corr-1");

            assertThat(response.status()).isEqualTo("CONFIRMED");
            verify(holdMirrorService).remove(reservationId);
            verify(reservationEventRepository).save(any(ReservationEvent.class));
        }

        @Test
        @DisplayName("AC4: confirm non-HELD reservation throws 409")
        void confirm_nonHeldReservation_throwsConflict() {
            UUID reservationId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
            reservation.setStatus(com.stampedeio.booking.domain.ReservationStatus.EXPIRED);

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            assertThatThrownBy(() -> service.confirm(reservationId, "PAY-123", "corr-1"))
                    .isInstanceOf(IllegalStateTransitionException.class)
                    .hasMessageContaining("EXPIRED")
                    .hasMessageContaining("CONFIRMED");
        }

        @Test
        @DisplayName("confirm non-existent reservation throws 404")
        void confirm_notFound_throws404() {
            UUID reservationId = UUID.randomUUID();
            when(reservationRepository.findById(reservationId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.confirm(reservationId, "PAY-123", "corr-1"))
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("release()")
    class ReleaseTests {

        @Test
        @DisplayName("AC3: release HELD reservation returns RELEASED with 200")
        void release_heldReservation_succeeds() {
            UUID reservationId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
            when(reservationEventRepository.findMaxSeqByAggregateId(any())).thenReturn(Optional.of(1));
            when(reservationEventRepository.save(any(ReservationEvent.class)))
                    .thenAnswer(inv -> inv.getArgument(0));
            when(reservationRepository.save(any(Reservation.class)))
                    .thenAnswer(inv -> inv.getArgument(0));

            ReservationResponse response = service.release(reservationId, "corr-2");

            assertThat(response.status()).isEqualTo("RELEASED");
            verify(holdMirrorService).remove(reservationId);
            verify(reservationEventRepository).save(any(ReservationEvent.class));
        }

        @Test
        @DisplayName("AC4: release non-HELD reservation throws 409")
        void release_nonHeldReservation_throwsConflict() {
            UUID reservationId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
            reservation.setStatus(com.stampedeio.booking.domain.ReservationStatus.CONFIRMED);

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            assertThatThrownBy(() -> service.release(reservationId, "corr-2"))
                    .isInstanceOf(IllegalStateTransitionException.class)
                    .hasMessageContaining("CONFIRMED")
                    .hasMessageContaining("RELEASED");
        }

        @Test
        @DisplayName("release non-existent reservation throws 404")
        void release_notFound_throws404() {
            UUID reservationId = UUID.randomUUID();
            when(reservationRepository.findById(reservationId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.release(reservationId, "corr-2"))
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }
}
