package com.stampedeio.booking;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisRepositoriesAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import org.springframework.transaction.support.TransactionTemplate;

import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.repository.OutboxRepository;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.ReservationEventRepository;
import com.stampedeio.booking.repository.ReservationSeatRepository;
import com.stampedeio.booking.service.HoldMirrorService;

@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,
        DataJpaRepositoriesAutoConfiguration.class,
        DataRedisAutoConfiguration.class,
        DataRedisRepositoriesAutoConfiguration.class
})
class BookingApplicationTests {

    @MockitoBean
    ReservationRepository reservationRepository;

    @MockitoBean
    ReservationSeatRepository reservationSeatRepository;

    @MockitoBean
    ReservationEventRepository reservationEventRepository;

    @MockitoBean
    OutboxRepository outboxRepository;

    @MockitoBean
    CatalogClient catalogClient;

    @MockitoBean
    HoldMirrorService holdMirrorService;

    @MockitoBean
    TransactionTemplate transactionTemplate;

    @Test
    void contextLoads() {
    }
}
