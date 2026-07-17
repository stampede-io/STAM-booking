package com.stampedeio.booking.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@Tag("integration")
class CircuitBreakerIT {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static WireMockServer catalogMock = new WireMockServer(
            WireMockConfiguration.wireMockConfig().dynamicPort());

    static {
        catalogMock.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("catalog.base-url", () -> catalogMock.baseUrl());
        registry.add("resilience4j.circuitbreaker.instances.catalogClient.sliding-window-size", () -> 3);
        registry.add("resilience4j.circuitbreaker.instances.catalogClient.minimum-number-of-calls", () -> 3);
        registry.add("resilience4j.circuitbreaker.instances.catalogClient.failure-rate-threshold", () -> 60);
        registry.add("resilience4j.circuitbreaker.instances.catalogClient.wait-duration-in-open-state", () -> "2s");
        registry.add("resilience4j.circuitbreaker.instances.catalogClient.permitted-number-of-calls-in-half-open-state", () -> 1);
    }

    @LocalServerPort
    private int port;

    private final HttpClient httpClient = HttpClient.newBuilder().build();

    @Test
    @DisplayName("AC2: catalog healthy -> booking proceeds normally (HTTP 201)")
    void catalog_healthy_booking_succeeds() throws Exception {
        catalogMock.stubFor(WireMock.get(WireMock.urlPathMatching("/api/v1/shows/.*/seats/validate"))
                .willReturn(WireMock.aResponse().withStatus(200)));

        HttpResponse<String> response = postReservation();
        assertThat(response.statusCode()).isEqualTo(201);
    }

    @Test
    @DisplayName("AC3: catalog down -> circuit opens after 3 failures -> booking returns 503 problem+json")
    void catalog_down_circuit_opens_returns_503() throws Exception {
        catalogMock.stubFor(WireMock.get(WireMock.urlPathMatching("/api/v1/shows/.*/seats/validate"))
                .willReturn(WireMock.aResponse().withStatus(500)));

        for (int i = 0; i < 3; i++) {
            HttpResponse<String> response = postReservation();
            assertThat(response.statusCode())
                    .as("failure call %d should return 503", i + 1)
                    .isEqualTo(503);
        }

        HttpResponse<String> openResponse = postReservation();
        assertThat(openResponse.statusCode()).isEqualTo(503);

        String body = openResponse.body();
        assertThat(body).contains("Seat validation unavailable");
        assertThat(openResponse.headers().firstValue("Content-Type").orElse(""))
                .contains("application/problem+json");
    }

    @Test
    @DisplayName("AC4: circuit transitions HALF_OPEN -> CLOSED when catalog recovers")
    void circuit_recovers_after_catalog_comes_back() throws Exception {
        catalogMock.stubFor(WireMock.get(WireMock.urlPathMatching("/api/v1/shows/.*/seats/validate"))
                .willReturn(WireMock.aResponse().withStatus(500)));

        for (int i = 0; i < 3; i++) {
            postReservation();
        }

        HttpResponse<String> openResponse = postReservation();
        assertThat(openResponse.statusCode()).isEqualTo(503);

        catalogMock.stubFor(WireMock.get(WireMock.urlPathMatching("/api/v1/shows/.*/seats/validate"))
                .willReturn(WireMock.aResponse().withStatus(200)));

        Thread.sleep(2500);

        HttpResponse<String> recoveredResponse = postReservation();
        assertThat(recoveredResponse.statusCode()).isEqualTo(201);
    }

    @Test
    @DisplayName("AC5: circuit breaker metrics visible at /actuator/prometheus")
    void circuit_breaker_metrics_exposed() throws Exception {
        catalogMock.stubFor(WireMock.get(WireMock.urlPathMatching("/api/v1/shows/.*/seats/validate"))
                .willReturn(WireMock.aResponse().withStatus(200)));

        postReservation();

        HttpRequest metricsReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/actuator/prometheus"))
                .GET()
                .build();
        HttpResponse<String> metricsResp = httpClient.send(metricsReq,
                HttpResponse.BodyHandlers.ofString());

        assertThat(metricsResp.statusCode()).isEqualTo(200);
        assertThat(metricsResp.body()).contains("resilience4j_circuitbreaker");
    }

    private HttpResponse<String> postReservation() throws Exception {
        UUID showId = UUID.randomUUID();
        UUID seatId = UUID.randomUUID();
        String body = """
                {"showId":"%s","userId":"%s","seatIds":["%s"]}"""
                .formatted(showId, UUID.randomUUID(), seatId);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/reservations"))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
