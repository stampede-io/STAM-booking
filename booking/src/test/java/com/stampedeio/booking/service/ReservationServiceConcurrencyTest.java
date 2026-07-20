package com.stampedeio.booking.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisRepositoriesAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.stampedeio.booking.api.CreateReservationRequest;
import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.exception.ConflictException;

@SpringBootTest
@Testcontainers
@EnableAutoConfiguration(exclude = {
        DataRedisAutoConfiguration.class,
        DataRedisRepositoriesAutoConfiguration.class
})
@Import(ReservationServiceConcurrencyTest.StubCatalogConfig.class)
class ReservationServiceConcurrencyTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @MockitoBean
    HoldMirrorService holdMirrorService;

    @Autowired
    private ReservationService reservationService;

    @Test
    @DisplayName("AC2: 50 concurrent threads on the same seat → exactly one success, rest ConflictException, zero unexpected errors")
    void concurrent_holds_produce_exactly_one_winner() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        int threads = 50;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finishGate = new CountDownLatch(threads);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    CreateReservationRequest req = new CreateReservationRequest(
                            showId, UUID.randomUUID(), List.of(seatId));
                    reservationService.hold(UUID.randomUUID(), req);
                    successes.incrementAndGet();
                } catch (ConflictException ex) {
                    conflicts.incrementAndGet();
                } catch (Throwable t) {
                    unexpected.add(t);
                } finally {
                    finishGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(finishGate.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        assertThat(unexpected)
                .as("no unexpected errors (would map to HTTP 500)")
                .isEmpty();
        assertThat(successes).as("exactly one winner").hasValue(1);
        assertThat(conflicts).as("all other threads see conflict").hasValue(threads - 1);
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
