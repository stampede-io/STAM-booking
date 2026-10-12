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

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

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
        meterRegistry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        service = new ReservationService(
                reservationRepository, reservationSeatRepository, reservationEventRepository,
                outboxRepository, catalogClient, holdMirrorService, transactionTemplate,
                Clock.systemUTC(), new ObjectMapper(), meterRegistry);
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
            CreateReservationRequest req = new CreateReservationRequest(showId, List.of(seatId));

            when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
            doNothing().when(catalogClient).validateSeatsForShow(showId, List.of(seatId));
            when(reservationRepository.saveAndFlush(any(Reservation.class)))
                    .thenAnswer(inv -> inv.getArgument(0));

            ReservationService.HoldResult result = service.hold(key, userId, req);

            assertThat(result.idempotentReplay()).isFalse();
            assertThat(result.response().status()).isEqualTo("HELD");
            assertThat(result.response().userId()).isEqualTo(userId);
            assertThat(result.response().seatIds()).containsExactly(seatId);
            assertThat(result.response().expiresAt()).isAfter(Instant.now());
            assertThat(result.response().ttlSeconds()).isBetween(415L, 421L);

            // STAM-398 / AC1: every genuine new hold is counted.
            assertThat(meterRegistry.get("holds_created_total").counter().count())
                    .isEqualTo(1.0);
        }

        @Test
        @DisplayName("AC2: unique-violation on seat maps to ConflictException with seat id in detail")
        void hold_conflict_mapsToConflictException() {
            UUID key = UUID.randomUUID();
            UUID showId = UUID.randomUUID();
            UUID seatId = UUID.randomUUID();
            CreateReservationRequest req = new CreateReservationRequest(showId, List.of(seatId));

            when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
            when(reservationRepository.saveAndFlush(any(Reservation.class)))
                    .thenThrow(new DataIntegrityViolationException("unique_violation"));
            when(reservationSeatRepository.findFirstConflictingSeatId(eq(showId), eq(List.of(seatId))))
                    .thenReturn(Optional.of(seatId));

            assertThatThrownBy(() -> service.hold(key, UUID.randomUUID(), req))
                    .isInstanceOf(ConflictException.class)
                    .hasMessageContaining(seatId.toString())
                    .hasMessageContaining("is already held");

            // STAM-398 / AC1: the real oversell-prevention event must be counted.
            assertThat(meterRegistry.get("oversell_attempts_blocked_total").counter().count())
                    .isEqualTo(1.0);
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
                    service.hold(key, userId, new CreateReservationRequest(showId, List.of(seatId)));

            assertThat(result.idempotentReplay()).isTrue();
            assertThat(result.response().seatIds()).containsExactly(seatId);
            verifyNoInteractions(catalogClient);
            verify(reservationRepository, org.mockito.Mockito.never()).saveAndFlush(any());
        }

        @Test
        @DisplayName("STAM-447: idempotency-key collision across different callers is treated as not found")
        void hold_idempotentReplay_differentCaller_throws404() {
            UUID key = UUID.randomUUID();
            UUID showId = UUID.randomUUID();
            UUID seatId = UUID.randomUUID();
            Reservation existing = new Reservation(showId, UUID.randomUUID(), key);
            existing.addSeat(seatId);

            when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.of(existing));

            assertThatThrownBy(() -> service.hold(
                    key, UUID.randomUUID(), new CreateReservationRequest(showId, List.of(seatId))))
                    .isInstanceOf(ResourceNotFoundException.class);
        }

        @Test
        @DisplayName("AC4: catalog 404 propagates as UnprocessableEntityException; no DB insert")
        void hold_invalidSeats_bubblesUp422() {
            UUID key = UUID.randomUUID();
            UUID showId = UUID.randomUUID();
            UUID seatId = UUID.randomUUID();
            CreateReservationRequest req = new CreateReservationRequest(showId, List.of(seatId));

            when(reservationRepository.findByIdempotencyKey(key)).thenReturn(Optional.empty());
            doThrow(new UnprocessableEntityException("invalid seats"))
                    .when(catalogClient).validateSeatsForShow(showId, List.of(seatId));

            assertThatThrownBy(() -> service.hold(key, UUID.randomUUID(), req))
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
            CreateReservationRequest req = new CreateReservationRequest(showId, List.of(seatId));

            Reservation winner = new Reservation(showId, userId, key);
            winner.addSeat(seatId);

            when(reservationRepository.findByIdempotencyKey(key))
                    .thenReturn(Optional.empty())
                    .thenReturn(Optional.of(winner));
            when(reservationRepository.saveAndFlush(any(Reservation.class)))
                    .thenThrow(new DataIntegrityViolationException("unique_violation on idempotency_key"));
            when(reservationSeatRepository.findFirstConflictingSeatId(showId, List.of(seatId)))
                    .thenReturn(Optional.empty());

            ReservationService.HoldResult result = service.hold(key, userId, req);

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
            UUID userId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), userId, UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
            when(reservationEventRepository.findMaxSeqByAggregateId(any())).thenReturn(Optional.empty());
            when(reservationEventRepository.save(any(ReservationEvent.class)))
                    .thenAnswer(inv -> inv.getArgument(0));
            when(reservationRepository.save(any(Reservation.class)))
                    .thenAnswer(inv -> inv.getArgument(0));

            ReservationResponse response = service.confirm(reservationId, userId, "PAY-123", "corr-1");

            assertThat(response.status()).isEqualTo("CONFIRMED");
            verify(holdMirrorService).remove(reservationId);
            verify(reservationEventRepository).save(any(ReservationEvent.class));
        }

        @Test
        @DisplayName("AC4: confirm non-HELD reservation throws 409")
        void confirm_nonHeldReservation_throwsConflict() {
            UUID reservationId = UUID.randomUUID();
            UUID userId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), userId, UUID.randomUUID());
            reservation.setStatus(com.stampedeio.booking.domain.ReservationStatus.EXPIRED);

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            assertThatThrownBy(() -> service.confirm(reservationId, userId, "PAY-123", "corr-1"))
                    .isInstanceOf(IllegalStateTransitionException.class)
                    .hasMessageContaining("EXPIRED")
                    .hasMessageContaining("CONFIRMED");
        }

        @Test
        @DisplayName("confirm non-existent reservation throws 404")
        void confirm_notFound_throws404() {
            UUID reservationId = UUID.randomUUID();
            when(reservationRepository.findById(reservationId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.confirm(reservationId, UUID.randomUUID(), "PAY-123", "corr-1"))
                    .isInstanceOf(ResourceNotFoundException.class);
        }

        @Test
        @DisplayName("STAM-447: confirm on another user's reservation throws 404, not 403")
        void confirm_notOwnedByCaller_throws404() {
            UUID reservationId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            assertThatThrownBy(() -> service.confirm(reservationId, UUID.randomUUID(), "PAY-123", "corr-1"))
                    .isInstanceOf(ResourceNotFoundException.class);
            verify(reservationRepository, org.mockito.Mockito.never()).save(any());
        }
    }

    @Nested
    @DisplayName("release()")
    class ReleaseTests {

        @Test
        @DisplayName("AC3: release HELD reservation returns RELEASED with 200")
        void release_heldReservation_succeeds() {
            UUID reservationId = UUID.randomUUID();
            UUID userId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), userId, UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
            when(reservationEventRepository.findMaxSeqByAggregateId(any())).thenReturn(Optional.of(1));
            when(reservationEventRepository.save(any(ReservationEvent.class)))
                    .thenAnswer(inv -> inv.getArgument(0));
            when(reservationRepository.save(any(Reservation.class)))
                    .thenAnswer(inv -> inv.getArgument(0));

            ReservationResponse response = service.release(reservationId, userId, "corr-2");

            assertThat(response.status()).isEqualTo("RELEASED");
            verify(holdMirrorService).remove(reservationId);
            verify(reservationEventRepository).save(any(ReservationEvent.class));
        }

        @Test
        @DisplayName("AC4: release non-HELD reservation throws 409")
        void release_nonHeldReservation_throwsConflict() {
            UUID reservationId = UUID.randomUUID();
            UUID userId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), userId, UUID.randomUUID());
            reservation.setStatus(com.stampedeio.booking.domain.ReservationStatus.CONFIRMED);

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            assertThatThrownBy(() -> service.release(reservationId, userId, "corr-2"))
                    .isInstanceOf(IllegalStateTransitionException.class)
                    .hasMessageContaining("CONFIRMED")
                    .hasMessageContaining("RELEASED");
        }

        @Test
        @DisplayName("release non-existent reservation throws 404")
        void release_notFound_throws404() {
            UUID reservationId = UUID.randomUUID();
            when(reservationRepository.findById(reservationId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.release(reservationId, UUID.randomUUID(), "corr-2"))
                    .isInstanceOf(ResourceNotFoundException.class);
        }

        @Test
        @DisplayName("STAM-447: release on another user's reservation throws 404, not 403")
        void release_notOwnedByCaller_throws404() {
            UUID reservationId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            assertThatThrownBy(() -> service.release(reservationId, UUID.randomUUID(), "corr-2"))
                    .isInstanceOf(ResourceNotFoundException.class);
            verify(reservationRepository, org.mockito.Mockito.never()).save(any());
        }
    }

    @Nested
    @DisplayName("setPaymentMethod()")
    class SetPaymentMethodTests {

        @Test
        @DisplayName("STAM-442: stores paymentMethodId on a HELD reservation")
        void setPaymentMethod_heldReservation_succeeds() {
            UUID reservationId = UUID.randomUUID();
            UUID userId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), userId, UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));
            when(reservationRepository.save(any(Reservation.class)))
                    .thenAnswer(inv -> inv.getArgument(0));

            service.setPaymentMethod(reservationId, userId, "pm_card_visa");

            assertThat(reservation.getPaymentMethodId()).isEqualTo("pm_card_visa");
            verify(reservationRepository).save(reservation);
        }

        @Test
        @DisplayName("STAM-442: setting payment method on a non-HELD reservation throws 409")
        void setPaymentMethod_nonHeldReservation_throwsConflict() {
            UUID reservationId = UUID.randomUUID();
            UUID userId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), userId, UUID.randomUUID());
            reservation.setStatus(com.stampedeio.booking.domain.ReservationStatus.CONFIRMED);

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            assertThatThrownBy(() -> service.setPaymentMethod(reservationId, userId, "pm_card_visa"))
                    .isInstanceOf(ConflictException.class);
            verify(reservationRepository, org.mockito.Mockito.never()).save(any());
        }

        @Test
        @DisplayName("setPaymentMethod on non-existent reservation throws 404")
        void setPaymentMethod_notFound_throws404() {
            UUID reservationId = UUID.randomUUID();
            when(reservationRepository.findById(reservationId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.setPaymentMethod(reservationId, UUID.randomUUID(), "pm_card_visa"))
                    .isInstanceOf(ResourceNotFoundException.class);
        }

        @Test
        @DisplayName("STAM-447: setPaymentMethod on another user's reservation throws 404, not 403")
        void setPaymentMethod_notOwnedByCaller_throws404() {
            UUID reservationId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            assertThatThrownBy(() -> service.setPaymentMethod(reservationId, UUID.randomUUID(), "pm_card_visa"))
                    .isInstanceOf(ResourceNotFoundException.class);
            verify(reservationRepository, org.mockito.Mockito.never()).save(any());
        }
    }

    @Nested
    @DisplayName("get()")
    class GetTests {

        @Test
        @DisplayName("STAM-447: get() returns the reservation to its owner")
        void get_ownedByCaller_succeeds() {
            UUID reservationId = UUID.randomUUID();
            UUID userId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), userId, UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            ReservationResponse response = service.get(reservationId, userId);

            assertThat(response.userId()).isEqualTo(userId);
        }

        @Test
        @DisplayName("STAM-447: get() on another user's reservation throws 404, not 403")
        void get_notOwnedByCaller_throws404() {
            UUID reservationId = UUID.randomUUID();
            Reservation reservation = new Reservation(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
            reservation.addSeat(UUID.randomUUID());

            when(reservationRepository.findById(reservationId)).thenReturn(Optional.of(reservation));

            assertThatThrownBy(() -> service.get(reservationId, UUID.randomUUID()))
                    .isInstanceOf(ResourceNotFoundException.class);
        }

        @Test
        @DisplayName("get() on non-existent reservation throws 404")
        void get_notFound_throws404() {
            UUID reservationId = UUID.randomUUID();
            when(reservationRepository.findById(reservationId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.get(reservationId, UUID.randomUUID()))
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }
}
