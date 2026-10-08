package com.stampedeio.booking.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.stampedeio.booking.domain.SagaInstance;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findByReservationId(UUID reservationId);

    List<SagaInstance> findByStateNotInAndUpdatedAtBefore(List<String> terminalStates, Instant cutoff);

    // STAM-66 / AC2 (SagaStuck): oldest non-terminal saga's last update, for
    // a "how long has the longest-stuck saga been stuck" gauge.
    @Query("select min(s.updatedAt) from SagaInstance s where s.state not in :terminalStates")
    Optional<Instant> findOldestNonTerminalUpdatedAt(List<String> terminalStates);
}
