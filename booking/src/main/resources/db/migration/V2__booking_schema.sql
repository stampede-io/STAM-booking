-- Reservation aggregate root
CREATE TABLE reservations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    show_id         UUID        NOT NULL,
    user_id         UUID        NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'HELD'
                        CHECK (status IN ('HELD', 'CONFIRMED', 'CANCELLED', 'EXPIRED')),
    idempotency_key UUID        NOT NULL UNIQUE,
    version         INTEGER     NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_reservations_show_id ON reservations (show_id);
CREATE INDEX idx_reservations_user_id ON reservations (user_id);

-- Junction table linking reservations to individual seats
CREATE TABLE reservation_seats (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id UUID        NOT NULL REFERENCES reservations (id),
    show_id        UUID        NOT NULL,
    seat_id        UUID        NOT NULL,
    status         VARCHAR(20) NOT NULL DEFAULT 'HELD'
                       CHECK (status IN ('HELD', 'CONFIRMED', 'RELEASED')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Partial unique index: prevents double-booking at the database level.
-- Only one HELD or CONFIRMED row per (show_id, seat_id) can exist.
CREATE UNIQUE INDEX idx_reservation_seats_oversell_guard
    ON reservation_seats (show_id, seat_id)
    WHERE status IN ('HELD', 'CONFIRMED');

CREATE INDEX idx_reservation_seats_reservation_id ON reservation_seats (reservation_id);

-- Event store for reservation aggregate (event sourcing)
CREATE TABLE reservation_events (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID        NOT NULL,
    seq          INTEGER     NOT NULL,
    event_type   VARCHAR(80) NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (aggregate_id, seq)
);

CREATE INDEX idx_reservation_events_aggregate_id ON reservation_events (aggregate_id);

-- Saga orchestrator state
CREATE TABLE saga_instances (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    saga_type      VARCHAR(80) NOT NULL,
    reservation_id UUID        NOT NULL REFERENCES reservations (id),
    state          VARCHAR(40) NOT NULL DEFAULT 'STARTED',
    payload        JSONB       NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_saga_instances_reservation_id ON saga_instances (reservation_id);

-- Transactional outbox for reliable Kafka publishing
CREATE TABLE outbox (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(80) NOT NULL,
    aggregate_id   UUID        NOT NULL,
    event_type     VARCHAR(80) NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
