package com.streamcraft.service.config;

import java.nio.charset.StandardCharsets;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriUtils;
import com.streamcraft.service.security.InternalTokenFilter;

@Configuration
public class SecurityConfig {

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http,
                                            UserDetailsService userDetailsService,
                                            StreamCraftAuthProperties properties,
                                            AuthenticationFailureHandler authenticationFailureHandler,
                                            InternalTokenFilter internalTokenFilter) throws Exception {
        http
                .addFilterBefore(internalTokenFilter, AnonymousAuthenticationFilter.class)
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers("/login", "/error", "/favicon.ico", "/brand-logo.png", "/css/**", "/js/**", "/images/**", "/webjars/**")
                        .permitAll()
                        .anyRequest()
                        .authenticated())
                .formLogin(form -> form
                        .loginPage("/login")
                        .loginProcessingUrl("/login")
                        .defaultSuccessUrl("/main", true)
                        .failureHandler(authenticationFailureHandler)
                        .permitAll())
                .rememberMe(rememberMe -> rememberMe
                        .rememberMeParameter("remember-me")
                        .key(properties.rememberMeKey())
                        .tokenValiditySeconds(properties.rememberMeValiditySeconds())
                        .userDetailsService(userDetailsService))
                .logout(logout -> logout
                        .logoutUrl("/logout")
                        .logoutSuccessUrl("/login?logout")
                        .invalidateHttpSession(true)
                        .deleteCookies("JSESSIONID", "remember-me"));

        return http.build();
    }

    @Bean
    AuthenticationFailureHandler authenticationFailureHandler() {
        return (request, response, exception) -> {
            String username = request.getParameter("username");
            String redirectUrl = "/login?error";
            if (StringUtils.hasText(username)) {
                redirectUrl += "&username=" + UriUtils.encode(username, StandardCharsets.UTF_8);
            }
            response.sendRedirect(redirectUrl);
        };
    }
}
