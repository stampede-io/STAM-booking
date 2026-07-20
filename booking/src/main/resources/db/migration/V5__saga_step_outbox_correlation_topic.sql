ALTER TABLE saga_instances ADD COLUMN step VARCHAR(60) NOT NULL DEFAULT 'INIT';

ALTER TABLE outbox ADD COLUMN correlation_id UUID;
ALTER TABLE outbox ADD COLUMN topic VARCHAR(120) NOT NULL DEFAULT 'reservations.events';

-- Back-fill existing rows so the column can be queried uniformly
UPDATE outbox SET correlation_id = id WHERE correlation_id IS NULL;
