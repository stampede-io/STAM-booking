package com.stampedeio.booking.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.stampedeio.booking.exception.IllegalStateTransitionException;

class ReservationStateMachineTest {

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("legalTransitions")
    @DisplayName("Legal transitions succeed and return the target status")
    void legalTransition_succeeds(ReservationStatus from, ReservationStatus to) {
        assertThat(ReservationStateMachine.transition(from, to)).isEqualTo(to);
    }

    static Stream<Arguments> legalTransitions() {
        return Stream.of(
                Arguments.of(ReservationStatus.HELD, ReservationStatus.CONFIRMED),
                Arguments.of(ReservationStatus.HELD, ReservationStatus.RELEASED),
                Arguments.of(ReservationStatus.HELD, ReservationStatus.EXPIRED),
                Arguments.of(ReservationStatus.EXPIRED, ReservationStatus.REFUNDED)
        );
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("illegalTransitions")
    @DisplayName("Illegal transitions throw IllegalStateTransitionException")
    void illegalTransition_throws(ReservationStatus from, ReservationStatus to) {
        assertThatThrownBy(() -> ReservationStateMachine.transition(from, to))
                .isInstanceOf(IllegalStateTransitionException.class)
                .hasMessageContaining(from.name())
                .hasMessageContaining(to.name());
    }

    static Stream<Arguments> illegalTransitions() {
        return Stream.of(
                // HELD cannot go to HELD or REFUNDED
                Arguments.of(ReservationStatus.HELD, ReservationStatus.HELD),
                Arguments.of(ReservationStatus.HELD, ReservationStatus.REFUNDED),
                // CONFIRMED is terminal (no outgoing transitions in map → null branch)
                Arguments.of(ReservationStatus.CONFIRMED, ReservationStatus.HELD),
                Arguments.of(ReservationStatus.CONFIRMED, ReservationStatus.RELEASED),
                Arguments.of(ReservationStatus.CONFIRMED, ReservationStatus.EXPIRED),
                Arguments.of(ReservationStatus.CONFIRMED, ReservationStatus.REFUNDED),
                // RELEASED is terminal
                Arguments.of(ReservationStatus.RELEASED, ReservationStatus.HELD),
                Arguments.of(ReservationStatus.RELEASED, ReservationStatus.CONFIRMED),
                Arguments.of(ReservationStatus.RELEASED, ReservationStatus.EXPIRED),
                // EXPIRED can only go to REFUNDED
                Arguments.of(ReservationStatus.EXPIRED, ReservationStatus.HELD),
                Arguments.of(ReservationStatus.EXPIRED, ReservationStatus.CONFIRMED),
                Arguments.of(ReservationStatus.EXPIRED, ReservationStatus.RELEASED),
                Arguments.of(ReservationStatus.EXPIRED, ReservationStatus.EXPIRED),
                // REFUNDED is terminal
                Arguments.of(ReservationStatus.REFUNDED, ReservationStatus.HELD),
                Arguments.of(ReservationStatus.REFUNDED, ReservationStatus.CONFIRMED)
        );
    }

    @Test
    @DisplayName("Exception message describes the illegal transition clearly")
    void illegalTransition_exceptionMessageIsDescriptive() {
        assertThatThrownBy(() ->
                ReservationStateMachine.transition(ReservationStatus.CONFIRMED, ReservationStatus.CONFIRMED))
                .hasMessage("Cannot transition reservation from CONFIRMED to CONFIRMED");
    }
}
