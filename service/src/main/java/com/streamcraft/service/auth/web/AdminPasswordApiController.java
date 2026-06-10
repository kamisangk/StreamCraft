package com.streamcraft.service.auth.web;

import com.streamcraft.service.auth.service.AdminPasswordService;
import jakarta.validation.Valid;
import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AdminPasswordApiController {

    private final AdminPasswordService adminPasswordService;

    public AdminPasswordApiController(AdminPasswordService adminPasswordService) {
        this.adminPasswordService = adminPasswordService;
    }

    @PostMapping("/api/settings/password")
    public Map<String, String> updatePassword(@Valid @RequestBody UpdateAdminPasswordRequest request) {
        adminPasswordService.updatePassword(request);
        return Map.of("message", "Password updated.");
    }
}
