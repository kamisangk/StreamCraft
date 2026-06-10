package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class SettingsTemplateContractTest {

    @Test
    void settingsPageUsesSharedShellAndPasswordFormContract() throws Exception {
        String template = loadTemplate("templates/settings.html");

        assertThat(template)
                .contains("fragments/app-shell :: commonHead(${pageTitle})")
                .contains("fragments/sidebar :: sidebar(${activeNav})")
                .contains("fragments/topbar :: topbar(${pageTitle}, ${pageSubtitle})")
                .contains("data-settings-password-form")
                .contains("name=\"currentPassword\"")
                .contains("name=\"newPassword\"")
                .contains("name=\"confirmPassword\"")
                .contains("/api/settings/password")
                .contains("fetch(")
                .contains("${_csrf.headerName}")
                .contains("${_csrf.token}");
    }

    @Test
    void passwordFormRejectsRedirectsAndNonJsonResponsesBeforeSuccess() throws Exception {
        String template = loadTemplate("templates/settings.html");

        assertThat(template)
                .contains("response.redirected")
                .contains("response.headers.get('Content-Type')")
                .contains("includes('application/json')")
                .contains("throw new Error(settingsErrorFallback)");
    }

    @Test
    void passwordFormUsesLocalizedFeedbackForKnownApiMessages() throws Exception {
        String template = loadTemplate("templates/settings.html");

        assertThat(template)
                .contains("settingsCurrentPasswordFallback")
                .contains("settingsMismatchFallback")
                .contains("settingsBlankPasswordFallback")
                .contains("localizeSettingsPasswordError")
                .contains("showSettingsPasswordMessage(settingsSuccessFallback, 'success')")
                .doesNotContain("showSettingsPasswordMessage(result.message || settingsSuccessFallback, 'success')");
    }

    @Test
    void settingsPageDoesNotExposeUnreleasedGlobalOptions() throws Exception {
        String template = loadTemplate("templates/settings.html");

        assertThat(template)
                .doesNotContain("application.properties")
                .doesNotContain("theme")
                .doesNotContain("language");
    }

    @Test
    void settingsPageUsesCalStyleLocalizedHeroAndRoundedCard() throws Exception {
        String template = loadTemplate("templates/settings.html");

        assertThat(template)
                .contains("th:text=\"#{settings.hero.label}\"")
                .contains("settings-password-card")
                .doesNotContain("<p class=\"sc-subtle-label\">Admin</p>");
    }

    private String loadTemplate(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
