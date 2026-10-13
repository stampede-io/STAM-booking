package com.stampedeio.booking.api;

import jakarta.validation.constraints.NotBlank;

public record SetPaymentMethodRequest(@NotBlank String paymentMethodId) {
}
