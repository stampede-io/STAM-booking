# Week 02 — Booking Schema & Oversell Guard

## Goal

Prove that the booking schema makes double-booking physically impossible at the database level, regardless of application-layer bugs.

## The Problem: Lost Updates

Without protection, two concurrent transactions can both read an available seat and both insert a reservation for it. Neither sees the other's uncommitted write, so both succeed — one customer's booking silently overwrites the other. This is the classic lost-update anomaly.

## Two-Session psql Reproduction

### Setup

```sql
-- Fresh schema (Flyway V2__booking_schema.sql already applied)
-- Insert a test reservation
INSERT INTO reservations (id, show_id, user_id, idempotency_key, version)
VALUES (
  '11111111-1111-1111-1111-111111111111',
  'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
  'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
  'cccccccc-cccc-cccc-cccc-cccccccccccc',
  0
);
```

### Step 1 — Without the partial unique index (hypothetical)

If we had no `idx_reservation_seats_oversell_guard`, the following race condition causes a double-book:

| Step | Session A                                                  | Session B                                                  |
|------|------------------------------------------------------------|------------------------------------------------------------|
| 1    | `BEGIN;`                                                   |                                                            |
| 2    |                                                            | `BEGIN;`                                                   |
| 3    | `INSERT INTO reservation_seats (reservation_id, show_id, seat_id, status) VALUES ('11111111-...', 'aaaaaaaa-...', 'dddddddd-dddd-dddd-dddd-dddddddddddd', 'HELD');` | |
| 4    |                                                            | `INSERT INTO reservation_seats (reservation_id, show_id, seat_id, status) VALUES ('11111111-...', 'aaaaaaaa-...', 'dddddddd-dddd-dddd-dddd-dddddddddddd', 'HELD');` |
| 5    | `COMMIT;` — succeeds                                      |                                                            |
| 6    |                                                            | `COMMIT;` — **also succeeds** — double-booked!             |

Both sessions commit without error. The same seat is now held twice — an oversell.

### Step 2 — With the partial unique index (our fix)

The index `CREATE UNIQUE INDEX idx_reservation_seats_oversell_guard ON reservation_seats (show_id, seat_id) WHERE status IN ('HELD', 'CONFIRMED')` catches it:

| Step | Session A                                                  | Session B                                                  |
|------|------------------------------------------------------------|------------------------------------------------------------|
| 1    | `BEGIN;`                                                   |                                                            |
| 2    |                                                            | `BEGIN;`                                                   |
| 3    | `INSERT INTO reservation_seats (reservation_id, show_id, seat_id, status) VALUES ('11111111-...', 'aaaaaaaa-...', 'dddddddd-dddd-dddd-dddd-dddddddddddd', 'HELD');` | |
| 4    | `COMMIT;` — succeeds                                      |                                                            |
| 5    |                                                            | `INSERT INTO reservation_seats (reservation_id, show_id, seat_id, status) VALUES ('11111111-...', 'aaaaaaaa-...', 'dddddddd-dddd-dddd-dddd-dddddddddddd', 'HELD');` |
| 6    |                                                            | **ERROR: duplicate key value violates unique constraint "idx_reservation_seats_oversell_guard"** |
|      |                                                            | `Detail: Key (show_id, seat_id)=(aaaaaaaa-..., dddddddd-...) already exists.` |
|      |                                                            | `ROLLBACK;`                                                |

Session B receives Postgres error code **23505** (`unique_violation`). The double-book is prevented.

### Step 3 — Released seats are not blocked

After a reservation is cancelled, its seats can be rebooked:

```sql
-- Release the seat from Session A's reservation
UPDATE reservation_seats
SET status = 'RELEASED'
WHERE show_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
  AND seat_id = 'dddddddd-dddd-dddd-dddd-dddddddddddd';

-- Now a new reservation for the same seat succeeds
INSERT INTO reservation_seats (reservation_id, show_id, seat_id, status)
VALUES ('22222222-2222-2222-2222-222222222222',
        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        'dddddddd-dddd-dddd-dddd-dddddddddddd',
        'HELD');
-- INSERT 0 1 — succeeds because the index only covers HELD/CONFIRMED rows
```

## Optimistic Locking on the Reservation Aggregate

The `reservations` table has a `version` column. JPA's `@Version` annotation on the `Reservation` entity increments it on every update. If two threads load the same reservation (both see version=0), modify it, and flush:

- Thread A commits first: `UPDATE reservations SET ... version = 1 WHERE id = ? AND version = 0` — 1 row updated, succeeds.
- Thread B attempts: `UPDATE reservations SET ... version = 1 WHERE id = ? AND version = 0` — 0 rows updated (version is already 1), Spring throws `ObjectOptimisticLockingFailureException`.

Our `GlobalExceptionHandler` catches this and returns HTTP **409 Conflict** with an RFC 7807 `application/problem+json` body.

## Schema Diff

```
booking_db=# \d reservation_seats
                          Table "public.reservation_seats"
     Column      |           Type           | Nullable |       Default
-----------------+--------------------------+----------+---------------------
 id              | uuid                     | not null | gen_random_uuid()
 reservation_id  | uuid                     | not null |
 show_id         | uuid                     | not null |
 seat_id         | uuid                     | not null |
 status          | character varying(20)    | not null | 'HELD'
 created_at      | timestamp with time zone | not null | now()
Indexes:
    "reservation_seats_pkey" PRIMARY KEY, btree (id)
    "idx_reservation_seats_oversell_guard" UNIQUE, btree (show_id, seat_id) WHERE status IN ('HELD', 'CONFIRMED')
    "idx_reservation_seats_reservation_id" btree (reservation_id)
Check constraints:
    "reservation_seats_status_check" CHECK (status IN ('HELD', 'CONFIRMED', 'RELEASED'))
Foreign-key constraints:
    "reservation_seats_reservation_id_fkey" FOREIGN KEY (reservation_id) REFERENCES reservations(id)
```

## Summary

Two layers of defense against overselling:

1. **Database layer** — Partial unique index on `reservation_seats(show_id, seat_id) WHERE status IN ('HELD', 'CONFIRMED')` makes double-booking physically impossible. Postgres enforces this even under concurrent transactions.
2. **Application layer** — `@Version` optimistic locking on the `Reservation` aggregate root catches concurrent modifications and surfaces them as 409 Conflict, allowing the client to retry.
