package com.stampedeio.booking.event;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.repository.OutboxRepository;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OutboxPublisher(OutboxRepository outboxRepository,
                           KafkaTemplate<String, Object> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRateString = "${outbox.poll.interval-ms:1000}")
    @Transactional
    public void poll() {
        List<OutboxMessage> pending = outboxRepository.findUnpublished();
        if (pending.isEmpty()) {
            return;
        }

        for (OutboxMessage msg : pending) {
            UUID correlationId = msg.getCorrelationId() != null ? msg.getCorrelationId() : msg.getId();
            EventEnvelope envelope = EventEnvelope.create(
                    msg.getEventType(),
                    1,
                    correlationId,
                    msg.getAggregateId(),
                    msg.getPayload()
            );

            kafkaTemplate.send(msg.getTopic(), msg.getAggregateId().toString(), envelope);
            msg.markPublished();
        }

        outboxRepository.saveAll(pending);
        log.info("Published {} outbox event(s)", pending.size());
    }
}
