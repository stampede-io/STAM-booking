package com.stampedeio.booking.exception;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import com.stampedeio.booking.service.ReservationService;
import org.springframework.http.MediaType;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;

@WebMvcTest
@Import({GlobalExceptionHandlerTest.StubController.class, GlobalExceptionHandler.class})
class GlobalExceptionHandlerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    ReservationService reservationService;

    @Test
    @DisplayName("404 returns problem+json with correct fields")
    void notFound_returnsProblemJson() throws Exception {
        mvc.perform(get("/test/not-found/42"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Not Found"))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.detail").value("Show 42 not found"))
                .andExpect(jsonPath("$.instance").value("/test/not-found/42"));
    }

    @Test
    @DisplayName("409 returns problem+json with correct fields")
    void conflict_returnsProblemJson() throws Exception {
        mvc.perform(get("/test/conflict"))
                .andExpect(status().isConflict())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Conflict"))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.detail").value("Seat already reserved"))
                .andExpect(jsonPath("$.instance").value("/test/conflict"));
    }

    @Test
    @DisplayName("OptimisticLockException returns 409 problem+json")
    void optimisticLock_returns409() throws Exception {
        mvc.perform(get("/test/optimistic-lock"))
                .andExpect(status().isConflict())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Conflict"))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.detail").value(
                        "Concurrent modification detected; please retry the operation"))
                .andExpect(jsonPath("$.instance").value("/test/optimistic-lock"));
    }

    @Test
    @DisplayName("DataIntegrityViolationException returns 409 problem+json")
    void dataIntegrity_returns409() throws Exception {
        mvc.perform(get("/test/data-integrity"))
                .andExpect(status().isConflict())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Conflict"))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.detail").value("Seat is already reserved"))
                .andExpect(jsonPath("$.instance").value("/test/data-integrity"));
    }

    @Test
    @DisplayName("400 validation error returns problem+json listing violations")
    void validationError_returnsProblemJson() throws Exception {
        mvc.perform(post("/test/validate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\": \"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Bad Request"))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.detail").exists())
                .andExpect(jsonPath("$.instance").value("/test/validate"));
    }

    @RestController
    static class StubController {

        @GetMapping("/test/not-found/{id}")
        void notFound(@PathVariable String id) {
            throw new ResourceNotFoundException("Show", id);
        }

        @GetMapping("/test/conflict")
        void conflict() {
            throw new ConflictException("Seat already reserved");
        }

        @GetMapping("/test/optimistic-lock")
        void optimisticLock() {
            throw new ObjectOptimisticLockingFailureException("Reservation", null);
        }

        @GetMapping("/test/data-integrity")
        void dataIntegrity() {
            throw new DataIntegrityViolationException("unique_violation");
        }

        @PostMapping("/test/validate")
        void validate(@Valid @RequestBody StubRequest request) {
        }
    }

    record StubRequest(@NotBlank String name) {
    }
}
