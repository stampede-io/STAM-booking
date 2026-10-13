package com.stampedeio.booking.config;

import java.io.IOException;

import org.slf4j.MDC;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * STAM-67 / AC1: logs the caller's identity as "userId" when authenticated.
 * Authentication.getName() rather than a Jwt-specific cast -- for a JWT
 * bearer token it already returns the subject claim, and the same filter
 * works unchanged for identity's session/form-login UserDetails principal.
 * Deliberately NOT ordered -- a plain, unordered @Component filter runs
 * after Spring Security's FilterChainProxy (registered much earlier by
 * SecurityFilterAutoConfiguration), so the SecurityContext is already
 * populated by the time this runs. CorrelationIdFilter stays at
 * HIGHEST_PRECEDENCE on purpose, so a 401 still logs a correlationId; this
 * one only has anything to add once auth has actually happened.
 */
@Component
public class UserIdMdcFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.isAuthenticated() && !(auth instanceof AnonymousAuthenticationToken)) {
            MDC.put("userId", auth.getName());
        }
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove("userId");
        }
    }
}
