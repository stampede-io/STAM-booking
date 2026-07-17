-- Replace CANCELLED with RELEASED and add REFUNDED to reservation status constraint
ALTER TABLE reservations DROP CONSTRAINT reservations_status_check;
ALTER TABLE reservations ADD CONSTRAINT reservations_status_check
    CHECK (status IN ('HELD', 'CONFIRMED', 'RELEASED', 'EXPIRED', 'REFUNDED'));

-- Migrate any existing CANCELLED rows to RELEASED
UPDATE reservations SET status = 'RELEASED' WHERE status = 'CANCELLED';
