package com.stampedeio.booking.catalog;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.stampedeio.booking.exception.ServiceUnavailableException;
import com.stampedeio.booking.exception.UnprocessableEntityException;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

@Component
public class RestCatalogClient implements CatalogClient {

    private final CatalogFeignClient feignClient;

    public RestCatalogClient(CatalogFeignClient feignClient) {
        this.feignClient = feignClient;
    }

    @Override
    @CircuitBreaker(name = "catalogClient", fallbackMethod = "validateFallback")
    public void validateSeatsForShow(UUID showId, List<UUID> seatIds) {
        try {
            feignClient.validateSeats(showId, seatIds);
        } catch (FeignException.NotFound ex) {
            throw new UnprocessableEntityException(
                    "One or more seat IDs are invalid or do not belong to show " + showId);
        }
    }

    @SuppressWarnings("unused")
    private void validateFallback(UUID showId, List<UUID> seatIds, Exception ex) {
        if (ex instanceof UnprocessableEntityException) {
            throw (UnprocessableEntityException) ex;
        }
        throw new ServiceUnavailableException(
                "Seat validation unavailable — retry shortly");
    }
}
