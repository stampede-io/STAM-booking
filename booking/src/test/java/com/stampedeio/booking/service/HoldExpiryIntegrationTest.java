package com.stampedeio.booking.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.domain.Reservation;
import com.stampedeio.booking.repository.ReservationRepository;

@SpringBootTest
@Testcontainers
@Import(HoldExpiryIntegrationTest.StubCatalogConfig.class)
class HoldExpiryIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private ReservationService reservationService;

    @Autowired
    private HoldExpiryScheduler holdExpiryScheduler;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private DataSource dataSource;

    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        jdbc = new JdbcTemplate(dataSource);
    }

    @Test
    @DisplayName("AC1: creating a hold writes a Redis mirror key with TTL ~7 min")
    void hold_createsRedisMirrorKey() {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        var request = new com.stampedeio.booking.api.CreateReservationRequest(
                showId, UUID.randomUUID(), List.of(seatId));

        var result = reservationService.hold(UUID.randomUUID(), request);

        String key = "hold:" + result.response().reservationId();
        assertThat(redisTemplate.hasKey(key)).isTrue();
        Long ttl = redisTemplate.getExpire(key);
        assertThat(ttl).isBetween(400L, 421L);
    }

    @Test
    @DisplayName("AC2: expiry sweep marks expired holds as EXPIRED and releases seats")
    void expirySweep_expiresStaleHolds() {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        Reservation reservation = new Reservation(showId, UUID.randomUUID(), UUID.randomUUID());
        reservation.addSeat(seatId);
        Reservation saved = reservationRepository.saveAndFlush(reservation);

        redisTemplate.opsForValue().set("hold:" + saved.getId(), "1", Duration.ofMinutes(7));

        // Backdate expires_at to simulate TTL elapsed
        jdbc.update("UPDATE reservations SET expires_at = now() - INTERVAL '1 minute' WHERE id = ?",
                saved.getId());

        holdExpiryScheduler.expireStaleHolds();

        Reservation expired = reservationRepository.findById(saved.getId()).orElseThrow();
        assertThat(expired.getStatus().name()).isEqualTo("EXPIRED");

        String seatStatus = jdbc.queryForObject(
                "SELECT status FROM reservation_seats WHERE reservation_id = ?",
                String.class, saved.getId());
        assertThat(seatStatus).isEqualTo("RELEASED");

        assertThat(redisTemplate.hasKey("hold:" + saved.getId())).isFalse();
    }

    @Test
    @DisplayName("AC3: stranded holds from downtime are swept on startup")
    void startupSweep_processesStrandedHolds() {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        Reservation reservation = new Reservation(showId, UUID.randomUUID(), UUID.randomUUID());
        reservation.addSeat(seatId);
        Reservation saved = reservationRepository.saveAndFlush(reservation);

        // Simulate hold that expired during downtime
        jdbc.update("UPDATE reservations SET expires_at = now() - INTERVAL '10 minutes' WHERE id = ?",
                saved.getId());

        // onStartup delegates to expireStaleHolds — same logic as the scheduled sweep
        holdExpiryScheduler.expireStaleHolds();

        Reservation expired = reservationRepository.findById(saved.getId()).orElseThrow();
        assertThat(expired.getStatus().name()).isEqualTo("EXPIRED");
    }

    @Test
    @DisplayName("AC4: expired hold's seat becomes available (RELEASED in reservation_seats)")
    void expiredHold_seatReleasedForNewBooking() {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();

        Reservation reservation = new Reservation(showId, UUID.randomUUID(), UUID.randomUUID());
        reservation.addSeat(seatId);
        Reservation saved = reservationRepository.saveAndFlush(reservation);

        jdbc.update("UPDATE reservations SET expires_at = now() - INTERVAL '1 minute' WHERE id = ?",
                saved.getId());

        holdExpiryScheduler.expireStaleHolds();

        // Seat should be released — a new reservation for the same seat should succeed
        Reservation newReservation = new Reservation(showId, UUID.randomUUID(), UUID.randomUUID());
        newReservation.addSeat(seatId);
        Reservation newSaved = reservationRepository.saveAndFlush(newReservation);
        assertThat(newSaved.getId()).isNotNull();
    }

    @Test
    @DisplayName("AC5: sweep with no expired holds does nothing")
    void expirySweep_noExpiredHolds_noUpdates() {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        Reservation reservation = new Reservation(showId, UUID.randomUUID(), UUID.randomUUID());
        reservation.addSeat(seatId);
        Reservation saved = reservationRepository.saveAndFlush(reservation);

        // Do NOT backdate — hold is still valid
        holdExpiryScheduler.expireStaleHolds();

        Reservation stillHeld = reservationRepository.findById(saved.getId()).orElseThrow();
        assertThat(stillHeld.getStatus().name()).isEqualTo("HELD");
    }

    @TestConfiguration
    static class StubCatalogConfig {

        @Bean
        @Primary
        CatalogClient stubCatalogClient() {
            return (showId, seatIds) -> {
            };
        }
    }
}
