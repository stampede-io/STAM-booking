package com.stampedeio.booking.repository;

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.stampedeio.booking.domain.ReservationEvent;

public interface ReservationEventRepository extends JpaRepository<ReservationEvent, UUID> {

    @Query("SELECT MAX(e.seq) FROM ReservationEvent e WHERE e.aggregateId = :aggregateId")
    Optional<Integer> findMaxSeqByAggregateId(@Param("aggregateId") UUID aggregateId);
}
