package com.stampedeio.booking.service;

import java.time.Duration;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class HoldMirrorService {

    private final StringRedisTemplate redisTemplate;

    public HoldMirrorService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void mirror(UUID reservationId, long ttlSeconds) {
        String key = "hold:" + reservationId;
        redisTemplate.opsForValue().set(key, "1", Duration.ofSeconds(ttlSeconds));
    }

    public void remove(UUID reservationId) {
        redisTemplate.delete("hold:" + reservationId);
    }
}
