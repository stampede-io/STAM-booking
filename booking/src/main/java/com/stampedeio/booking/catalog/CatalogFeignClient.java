package com.stampedeio.booking.catalog;

import java.util.List;
import java.util.UUID;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "catalog", url = "${catalog.base-url:http://catalog:8081}")
public interface CatalogFeignClient {

    @GetMapping("/api/v1/shows/{showId}/seats/validate")
    void validateSeats(@PathVariable("showId") UUID showId,
                       @RequestParam("ids") List<UUID> seatIds);
}
