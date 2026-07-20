package com.stampedeio.booking.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.stampedeio.booking.domain.Reservation;

public interface ReservationRepository extends JpaRepository<Reservation, UUID> {

    Optional<Reservation> findByIdempotencyKey(UUID idempotencyKey);

    @Query("SELECT r FROM Reservation r LEFT JOIN FETCH r.seats WHERE r.status = com.stampedeio.booking.domain.ReservationStatus.HELD AND r.expiresAt < :now")
    List<Reservation> findExpiredHolds(@Param("now") Instant now);
}
