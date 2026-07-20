package com.stampedeio.booking.repository;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.stampedeio.booking.domain.OutboxMessage;

public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    @Query("SELECT o FROM OutboxMessage o WHERE o.publishedAt IS NULL ORDER BY o.createdAt ASC")
    List<OutboxMessage> findUnpublished();
}
