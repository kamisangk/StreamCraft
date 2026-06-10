package com.streamcraft.service.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class InternalTokenFilter extends OncePerRequestFilter {

    private final InternalAccessProperties properties;
    private final RequestMatcher requestMatcher =
            new AntPathRequestMatcher("/api/pipelines/*/definition", "GET");

    public InternalTokenFilter(InternalAccessProperties properties) {
        this.properties = properties;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !requestMatcher.matches(request);
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {
        Authentication currentAuthentication = SecurityContextHolder.getContext().getAuthentication();
        if (isAuthenticated(currentAuthentication)) {
            filterChain.doFilter(request, response);
            return;
        }

        String token = request.getHeader(properties.headerName());
        if (!StringUtils.hasText(token)) {
            filterChain.doFilter(request, response);
            return;
        }
        if (!token.equals(properties.token())) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid internal token.");
            return;
        }

        UsernamePasswordAuthenticationToken authentication = UsernamePasswordAuthenticationToken.authenticated(
                "streamcraft-internal",
                token,
                List.of(new SimpleGrantedAuthority("ROLE_INTERNAL")));
        authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(authentication);
        SecurityContextHolder.setContext(context);
        filterChain.doFilter(request, response);
    }

    private boolean isAuthenticated(Authentication authentication) {
        return authentication != null
                && authentication.isAuthenticated()
                && !(authentication instanceof AnonymousAuthenticationToken);
    }
}
