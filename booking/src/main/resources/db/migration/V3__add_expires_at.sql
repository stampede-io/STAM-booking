ALTER TABLE reservations ADD COLUMN expires_at TIMESTAMPTZ;
UPDATE reservations SET expires_at = created_at + INTERVAL '7 minutes';
ALTER TABLE reservations ALTER COLUMN expires_at SET NOT NULL;

CREATE INDEX idx_reservations_expiry_sweep
    ON reservations (expires_at)
    WHERE status = 'HELD';
