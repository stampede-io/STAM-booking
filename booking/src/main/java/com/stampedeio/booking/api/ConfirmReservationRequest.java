package com.stampedeio.booking.api;

import jakarta.validation.constraints.NotBlank;

public record ConfirmReservationRequest(@NotBlank String paymentReference) {
}
