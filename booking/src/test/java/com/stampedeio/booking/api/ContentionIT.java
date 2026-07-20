package com.stampedeio.booking.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.stampedeio.booking.catalog.CatalogClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@Tag("integration")
@Import(ContentionIT.StubCatalogConfig.class)
class ContentionIT {

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

    @LocalServerPort
    private int port;

    @Autowired
    private DataSource dataSource;

    @Test
    @DisplayName("50 concurrent threads racing for the same seat -> exactly 1 CREATED, 49 CONFLICT, DB confirms 1 active row")
    void fifty_threads_race_one_seat_exactly_one_winner() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        int threads = 50;

        HttpClient httpClient = HttpClient.newBuilder().build();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finishGate = new CountDownLatch(threads);
        AtomicInteger created = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();

                    String body = """
                            {"showId":"%s","userId":"%s","seatIds":["%s"]}"""
                            .formatted(showId, UUID.randomUUID(), seatId);

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/api/v1/reservations"))
                            .header("Content-Type", "application/json")
                            .header("Idempotency-Key", UUID.randomUUID().toString())
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();

                    HttpResponse<String> response = httpClient.send(
                            request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() == 201) {
                        created.incrementAndGet();
                    } else if (response.statusCode() == 409) {
                        conflicts.incrementAndGet();
                    } else {
                        System.err.println("Unexpected status " + response.statusCode()
                                + ": " + response.body());
                        other.incrementAndGet();
                    }
                } catch (Exception e) {
                    System.err.println("Request failed: " + e.getMessage());
                    other.incrementAndGet();
                } finally {
                    finishGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(finishGate.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        assertThat(other)
                .as("no unexpected HTTP status codes (would map to 500 / network error)")
                .hasValue(0);
        assertThat(created)
                .as("exactly one thread receives HTTP 201")
                .hasValue(1);
        assertThat(conflicts)
                .as("remaining 49 threads receive HTTP 409")
                .hasValue(threads - 1);

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        Integer activeRows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM reservation_seats "
                        + "WHERE show_id = ? AND seat_id = ? AND status IN ('HELD', 'CONFIRMED')",
                Integer.class, showId, seatId);
        assertThat(activeRows)
                .as("DB confirms exactly 1 active reservation_seats row for the contested seat")
                .isEqualTo(1);
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
