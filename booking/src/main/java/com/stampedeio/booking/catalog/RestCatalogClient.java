package com.stampedeio.booking.catalog;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import com.stampedeio.booking.exception.UnprocessableEntityException;

@Component
public class RestCatalogClient implements CatalogClient {

    private final RestClient restClient;

    public RestCatalogClient(@Value("${catalog.base-url:http://catalog:8081}") String baseUrl) {
        this.restClient = RestClient.builder().baseUrl(baseUrl).build();
    }

    @Override
    public void validateSeatsForShow(UUID showId, List<UUID> seatIds) {
        String ids = seatIds.stream().map(UUID::toString).collect(Collectors.joining(","));
        try {
            restClient.get()
                    .uri("/api/v1/shows/{showId}/seats/validate?ids={ids}", showId, ids)
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpClientErrorException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND) {
                throw new UnprocessableEntityException(
                        "One or more seat IDs are invalid or do not belong to show " + showId);
            }
            throw ex;
        }
    }
}
