package com.stampedeio.booking.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.stampedeio.booking.domain.SagaInstance;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findByReservationId(UUID reservationId);

    List<SagaInstance> findByStateNotInAndUpdatedAtBefore(List<String> terminalStates, Instant cutoff);
}
