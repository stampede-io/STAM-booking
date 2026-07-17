package com.stampedeio.booking.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisRepositoriesAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.stampedeio.booking.catalog.CatalogClient;
import com.stampedeio.booking.repository.ReservationRepository;
import com.stampedeio.booking.repository.ReservationSeatRepository;
import com.stampedeio.booking.service.HoldMirrorService;

@SpringBootTest
@AutoConfigureMockMvc
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,
        DataJpaRepositoriesAutoConfiguration.class,
        DataRedisAutoConfiguration.class,
        DataRedisRepositoriesAutoConfiguration.class
})
class OpenApiSpecTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    ReservationRepository reservationRepository;

    @MockitoBean
    ReservationSeatRepository reservationSeatRepository;

    @MockitoBean
    CatalogClient catalogClient;

    @MockitoBean
    HoldMirrorService holdMirrorService;

    @Test
    void generateOpenApiSpec() throws Exception {
        MvcResult result = mvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andReturn();

        Path output = Path.of("target/openapi.json");
        Files.createDirectories(output.getParent());
        Files.writeString(output, result.getResponse().getContentAsString());
    }
}
