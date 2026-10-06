package com.stampedeio.booking.catalog;

import java.util.List;
import java.util.UUID;

public interface CatalogClient {

    /**
     * Validates that every seatId belongs to the given show. Throws
     * {@link com.stampedeio.booking.exception.UnprocessableEntityException} if catalog
     * returns 404 for the show or any seat is not part of it.
     */
    void validateSeatsForShow(UUID showId, List<UUID> seatIds);

    /**
     * Sums catalog's priceCents for the given seats of a show. The charge amount is
     * computed server-side from catalog rather than trusted from the client.
     */
    long totalPriceCentsForSeats(UUID showId, List<UUID> seatIds);
}
