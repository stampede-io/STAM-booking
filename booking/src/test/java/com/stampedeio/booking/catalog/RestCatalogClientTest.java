package com.stampedeio.booking.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.stampedeio.booking.exception.UnprocessableEntityException;

class RestCatalogClientTest {

    private final CatalogFeignClient feignClient = mock(CatalogFeignClient.class);
    private final RestCatalogClient client = new RestCatalogClient(feignClient);

    @Test
    @DisplayName("STAM-442: sums priceCents for the requested seats only")
    void totalPriceCentsForSeats_sumsOnlyRequestedSeats() {
        UUID showId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID seat2 = UUID.randomUUID();
        UUID otherSeat = UUID.randomUUID();
        when(feignClient.listSeats(showId)).thenReturn(List.of(
                new SeatPrice(seat1, 5000L),
                new SeatPrice(seat2, 7500L),
                new SeatPrice(otherSeat, 9999L)));

        long total = client.totalPriceCentsForSeats(showId, List.of(seat1, seat2));

        assertThat(total).isEqualTo(12500L);
    }

    @Test
    @DisplayName("STAM-442: throws rather than silently undercounting when a requested seat is missing from catalog")
    void totalPriceCentsForSeats_missingSeat_throwsInsteadOfUndercounting() {
        UUID showId = UUID.randomUUID();
        UUID seat1 = UUID.randomUUID();
        UUID missingSeat = UUID.randomUUID();
        when(feignClient.listSeats(showId)).thenReturn(List.of(new SeatPrice(seat1, 5000L)));

        assertThatThrownBy(() -> client.totalPriceCentsForSeats(showId, List.of(seat1, missingSeat)))
                .isInstanceOf(UnprocessableEntityException.class);
    }
}
