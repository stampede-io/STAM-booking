package com.stampedeio.booking.saga;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final BookingSagaOrchestrator sagaOrchestrator;

    public PaymentEventConsumer(BookingSagaOrchestrator sagaOrchestrator) {
        this.sagaOrchestrator = sagaOrchestrator;
    }

    @KafkaListener(topics = "payments.events", groupId = "booking-saga-consumer")
    public void consume(Map<String, Object> message) {
        String eventType = (String) message.get("eventType");
        UUID correlationId = UUID.fromString((String) message.get("correlationId"));
        UUID aggregateId = UUID.fromString((String) message.get("aggregateId"));

        log.info("Received {} for reservation={} correlationId={}", eventType, aggregateId, correlationId);

        switch (eventType) {
            case "PaymentAuthorized" -> sagaOrchestrator.handlePaymentAuthorized(aggregateId, correlationId);
            case "PaymentFailed" -> sagaOrchestrator.handlePaymentFailed(aggregateId, correlationId);
            case "RefundIssued" -> sagaOrchestrator.handleRefundIssued(aggregateId, correlationId);
            default -> log.warn("Unknown payment event type: {}", eventType);
        }
    }
}
