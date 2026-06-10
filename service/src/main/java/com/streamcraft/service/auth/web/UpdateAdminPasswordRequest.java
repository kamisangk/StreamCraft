package com.streamcraft.service.auth.web;

import jakarta.validation.constraints.NotBlank;

public record UpdateAdminPasswordRequest(
        @NotBlank String currentPassword,
        @NotBlank String newPassword,
        @NotBlank String confirmPassword) {
}
