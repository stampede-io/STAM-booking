package com.stampedeio.booking.config;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * STAM-406 / AC5: correlationId already rides in EventEnvelope.payload
 * (CLAUDE.md §4.7), but that means a consumer can only recover it after a
 * successful JSON parse -- useless for a malformed message, and it means
 * every listener has to pull it out of the map by hand. Carrying it as a
 * plain Kafka header too lets the MDC be populated (and logged) before
 * the listener method -- and any deserialization failure -- even runs.
 */
public final class KafkaCorrelationIdInterceptor implements RecordInterceptor<String, Map<String, Object>> {

    public static final String HEADER = "correlationId";
    private static final String SERVICE = "booking";

    private KafkaCorrelationIdInterceptor() {
    }

    /** Producer side: call right before kafkaTemplate.send(record). */
    public static void addHeader(ProducerRecord<?, ?> record, UUID correlationId) {
        record.headers().add(HEADER, correlationId.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static KafkaCorrelationIdInterceptor instance() {
        return new KafkaCorrelationIdInterceptor();
    }

    @Override
    public ConsumerRecord<String, Map<String, Object>> intercept(
            ConsumerRecord<String, Map<String, Object>> record, Consumer<String, Map<String, Object>> consumer) {
        Header header = record.headers().lastHeader(HEADER);
        if (header != null) {
            MDC.put(HEADER, new String(header.value(), StandardCharsets.UTF_8));
        }
        MDC.put("service", SERVICE);
        return record;
    }

    @Override
    public void success(ConsumerRecord<String, Map<String, Object>> record,
                         Consumer<String, Map<String, Object>> consumer) {
        clear();
    }

    @Override
    public void failure(ConsumerRecord<String, Map<String, Object>> record, Exception exception,
                         Consumer<String, Map<String, Object>> consumer) {
        clear();
    }

    private void clear() {
        MDC.remove(HEADER);
        MDC.remove("service");
    }
}
