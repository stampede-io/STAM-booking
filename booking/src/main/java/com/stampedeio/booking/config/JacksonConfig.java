package com.stampedeio.booking.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@Configuration
public class JacksonConfig {

    // Spring Boot 4.1 auto-configures a Jackson 3 (tools.jackson.databind)
    // ObjectMapper as the default bean now that both major versions coexist
    // on the classpath — it no longer also registers a classic
    // com.fasterxml.jackson.databind.ObjectMapper bean. ReservationService
    // needs that classic type explicitly, so it's provided here.
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
}
