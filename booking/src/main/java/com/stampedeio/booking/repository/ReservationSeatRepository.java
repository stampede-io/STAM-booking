package com.stampedeio.booking.repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.stampedeio.booking.domain.ReservationSeat;

public interface ReservationSeatRepository extends JpaRepository<ReservationSeat, UUID> {

    @Query("""
            select rs.seatId
              from ReservationSeat rs
             where rs.showId = :showId
               and rs.seatId in :seatIds
               and rs.status in (com.stampedeio.booking.domain.SeatHoldStatus.HELD,
                                  com.stampedeio.booking.domain.SeatHoldStatus.CONFIRMED)
            """)
    List<UUID> findConflictingSeatIds(@Param("showId") UUID showId,
                                      @Param("seatIds") List<UUID> seatIds);

    default Optional<UUID> findFirstConflictingSeatId(UUID showId, List<UUID> seatIds) {
        return findConflictingSeatIds(showId, seatIds).stream().findFirst();
    }
}
