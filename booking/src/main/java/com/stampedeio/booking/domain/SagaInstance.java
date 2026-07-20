package com.stampedeio.booking.domain;

import java.time.Instant;
import java.util.UUID;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "saga_instances")
public class SagaInstance {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "saga_type", nullable = false, length = 80)
    private String sagaType;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "reservation_id", nullable = false)
    private Reservation reservation;

    @Column(nullable = false, length = 40)
    private String state = "STARTED";

    @Column(nullable = false, length = 60)
    private String step = "INIT";

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload = "{}";

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    protected SagaInstance() {
    }

    public SagaInstance(String sagaType, Reservation reservation) {
        this.sagaType = sagaType;
        this.reservation = reservation;
    }

    public UUID getId() {
        return id;
    }

    public String getSagaType() {
        return sagaType;
    }

    public Reservation getReservation() {
        return reservation;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
        this.updatedAt = Instant.now();
    }

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
        this.updatedAt = Instant.now();
    }

    public void advance(SagaState newState, String newStep) {
        this.state = newState.name();
        this.step = newStep;
        this.updatedAt = Instant.now();
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
        this.updatedAt = Instant.now();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
