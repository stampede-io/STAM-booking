package com.stampedeio.booking.event;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.stampedeio.booking.config.KafkaCorrelationIdInterceptor;
import com.stampedeio.booking.domain.OutboxMessage;
import com.stampedeio.booking.repository.OutboxRepository;

/**
 * Ships outbox rows to Kafka. A row is marked published only after the broker
 * acks it (STAM-451): {@code send()} is asynchronous, and marking on return
 * lost every buffered record when booking crashed during a broker outage.
 *
 * <p>Delivery is at-least-once. A row whose ack timed out is sent again on the
 * next poll, so every envelope carries the outbox row's id as {@code eventId}
 * and consumers dedupe on it (CLAUDE.md §4.5).
 *
 * <p>No transaction spans the poll: the wait for acks can take up to the
 * producer's {@code delivery.timeout.ms} and must not pin a DB connection.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    /** Safety net only: the producer fails every send within delivery.timeout.ms (KafkaProducerConfig). */
    private static final long ACK_TIMEOUT_MS = 20_000;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OutboxPublisher(OutboxRepository outboxRepository,
                           KafkaTemplate<String, Object> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRateString = "${outbox.poll.interval-ms:1000}")
    public void poll() {
        List<OutboxMessage> pending = outboxRepository.findUnpublished();
        if (pending.isEmpty()) {
            return;
        }

        List<CompletableFuture<?>> sends = new ArrayList<>(pending.size());
        for (OutboxMessage msg : pending) {
            CompletableFuture<?> sent = send(msg);
            sends.add(sent);
            // A synchronous failure means no broker metadata: every further
            // send() would block max.block.ms too, stalling the shared
            // scheduler thread for pending.size() x 5 s. Nothing after it
            // could be marked anyway.
            if (sent.isCompletedExceptionally()) {
                break;
            }
        }

        // Mark only the acked prefix, in outbox order. Stopping at the first
        // failure keeps a reservation's events in order: SeatsReleased must
        // not be marked while the SeatsHeld before it is still to be retried.
        List<OutboxMessage> acked = new ArrayList<>();
        for (int i = 0; i < sends.size(); i++) {
            OutboxMessage msg = pending.get(i);
            try {
                sends.get(i).get(ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException | TimeoutException e) {
                log.warn("Outbox event {} ({}) not acked by Kafka, {} row(s) left for the next poll: {}",
                        msg.getId(), msg.getEventType(), pending.size() - i, rootMessage(e));
                break;
            }
            msg.markPublished();
            acked.add(msg);
        }

        if (!acked.isEmpty()) {
            outboxRepository.saveAll(acked);
            log.info("Published {} outbox event(s)", acked.size());
        }
    }

    private CompletableFuture<?> send(OutboxMessage msg) {
        UUID correlationId = msg.getCorrelationId() != null ? msg.getCorrelationId() : msg.getId();
        // eventId and occurredAt come from the row, so a resend is the same event.
        EventEnvelope envelope = new EventEnvelope(
                msg.getId(),
                msg.getEventType(),
                1,
                msg.getCreatedAt(),
                correlationId,
                msg.getAggregateId(),
                msg.getPayload());
        ProducerRecord<String, Object> record = new ProducerRecord<>(
                msg.getTopic(), msg.getAggregateId().toString(), envelope);
        KafkaCorrelationIdInterceptor.addHeader(record, correlationId);
        try {
            return kafkaTemplate.send(record);
        } catch (RuntimeException e) {
            // send() itself throws when metadata can't be fetched within max.block.ms.
            return CompletableFuture.failedFuture(e);
        }
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
