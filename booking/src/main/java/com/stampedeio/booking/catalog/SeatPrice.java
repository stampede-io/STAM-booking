package com.stampedeio.booking.catalog;

import java.util.UUID;

/** Mirrors the fields of catalog's SeatResponse that booking actually needs. */
public record SeatPrice(UUID id, long priceCents) {
}
