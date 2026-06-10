package com.streamcraft.service.auth.service;

import com.streamcraft.service.auth.web.UpdateAdminPasswordRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.JdbcUserDetailsManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AdminPasswordService {

    private static final String WRONG_CURRENT_PASSWORD_MESSAGE = "Current password is incorrect.";
    private static final String MISMATCHED_CONFIRMATION_MESSAGE = "New password confirmation does not match.";

    private final JdbcUserDetailsManager userDetailsManager;
    private final PasswordEncoder passwordEncoder;

    public AdminPasswordService(JdbcUserDetailsManager userDetailsManager, PasswordEncoder passwordEncoder) {
        this.userDetailsManager = userDetailsManager;
        this.passwordEncoder = passwordEncoder;
    }

    @Transactional
    public void updatePassword(UpdateAdminPasswordRequest request) {
        UserDetails user = userDetailsManager.loadUserByUsername(currentUsername());
        if (!passwordEncoder.matches(request.currentPassword(), user.getPassword())) {
            throw new IllegalArgumentException(WRONG_CURRENT_PASSWORD_MESSAGE);
        }
        if (!request.newPassword().equals(request.confirmPassword())) {
            throw new IllegalArgumentException(MISMATCHED_CONFIRMATION_MESSAGE);
        }

        userDetailsManager.updateUser(User.withUserDetails(user)
                .password(passwordEncoder.encode(request.newPassword()))
                .build());
    }

    private String currentUsername() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !authentication.isAuthenticated()) {
            throw new IllegalArgumentException("Authenticated user is required.");
        }
        return authentication.getName();
    }
}
