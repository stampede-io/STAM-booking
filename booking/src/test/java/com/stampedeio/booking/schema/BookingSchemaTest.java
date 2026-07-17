package com.stampedeio.booking.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.domain.ReservationSeat;
import com.stampedeio.booking.repository.ReservationRepository;

@SpringBootTest
@Testcontainers
class BookingSchemaTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private DataSource dataSource;

    @Test
    @DisplayName("AC1: reservations table has version column, reservation_seats exists with partial unique index")
    void schema_hasExpectedTablesAndConstraints() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        // reservations table exists and has version column
        Integer versionExists = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.columns " +
                "WHERE table_name = 'reservations' AND column_name = 'version'",
                Integer.class);
        assertThat(versionExists).isEqualTo(1);

        // reservation_seats table exists
        Integer seatsTableExists = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_name = 'reservation_seats'",
                Integer.class);
        assertThat(seatsTableExists).isEqualTo(1);

        // Partial unique index exists
        Integer indexExists = jdbc.queryForObject(
                "SELECT COUNT(*) FROM pg_indexes " +
                "WHERE tablename = 'reservation_seats' " +
                "AND indexname = 'idx_reservation_seats_oversell_guard'",
                Integer.class);
        assertThat(indexExists).isEqualTo(1);
    }

    @Test
    @DisplayName("AC2: partial unique index blocks duplicate (show_id, seat_id) when HELD")
    void partialUniqueIndex_blocksDuplicateHeldSeat() {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        // First reservation — should succeed
        Reservation r1 = new Reservation(showId, UUID.randomUUID(), UUID.randomUUID());
        r1.addSeat(seatId);
        reservationRepository.saveAndFlush(r1);

        // Second reservation for the same seat — should fail with unique_violation
        Reservation r2 = new Reservation(showId, UUID.randomUUID(), UUID.randomUUID());
        r2.addSeat(seatId);

        assertThatThrownBy(() -> reservationRepository.saveAndFlush(r2))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("AC2: RELEASED seats do not block new reservations")
    void releasedSeat_allowsNewReservation() {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        // First reservation — hold the seat, then release it
        Reservation r1 = new Reservation(showId, UUID.randomUUID(), UUID.randomUUID());
        r1.addSeat(seatId);
        reservationRepository.saveAndFlush(r1);

        // Release the seat
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.update("UPDATE reservation_seats SET status = 'RELEASED' WHERE show_id = ? AND seat_id = ?",
                showId, seatId);

        // Second reservation for the same seat — should now succeed
        Reservation r2 = new Reservation(showId, UUID.randomUUID(), UUID.randomUUID());
        r2.addSeat(seatId);
        reservationRepository.saveAndFlush(r2);

        assertThat(r2.getId()).isNotNull();
    }

    @Test
    @DisplayName("AC5: reservation_events table exists with UNIQUE(aggregate_id, seq)")
    void schema_hasReservationEventsTable() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        Integer tableExists = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_name = 'reservation_events'",
                Integer.class);
        assertThat(tableExists).isEqualTo(1);

        // Check unique constraint on (aggregate_id, seq)
        Integer constraintExists = jdbc.queryForObject(
                "SELECT COUNT(*) FROM pg_indexes " +
                "WHERE tablename = 'reservation_events' " +
                "AND indexdef LIKE '%aggregate_id%seq%' AND indexdef LIKE '%UNIQUE%'",
                Integer.class);
        assertThat(constraintExists).isEqualTo(1);
    }

    @Test
    @DisplayName("AC5: outbox table exists with nullable published_at")
    void schema_hasOutboxTable() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        Integer tableExists = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_name = 'outbox'",
                Integer.class);
        assertThat(tableExists).isEqualTo(1);

        // published_at is nullable
        String isNullable = jdbc.queryForObject(
                "SELECT is_nullable FROM information_schema.columns " +
                "WHERE table_name = 'outbox' AND column_name = 'published_at'",
                String.class);
        assertThat(isNullable).isEqualTo("YES");
    }

    @Test
    @DisplayName("AC5: saga_instances table exists")
    void schema_hasSagaInstancesTable() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        Integer tableExists = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_name = 'saga_instances'",
                Integer.class);
        assertThat(tableExists).isEqualTo(1);
    }
}
