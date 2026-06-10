package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class LoginTemplateContractTest {

    @Test
    void templateBootstrapsClassBasedThemeWithoutDuplicateHeadTag() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("th:replace=\"~{fragments/app-shell :: commonHead(#{login.documentTitle})}\"")
                .contains("#{login.documentTitle}")
                .containsOnlyOnce("<head ")
                .doesNotContain("</title>\r\n<head>")
                .doesNotContain("tailwind.config = {");
    }

    @Test
    void loginPageUsesMessageKeysForVisibleCopy() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("#{login.hero.title.line1}")
                .contains("#{login.hero.title.line2}")
                .contains("#{login.hero.description}")
                .contains("#{login.title}")
                .contains("#{login.subtitle}")
                .contains("#{login.error.invalidCredentials}")
                .contains("#{login.logout.success}")
                .contains("#{login.username.label}")
                .contains("#{login.password.label}")
                .contains("#{login.password.placeholder}")
                .contains("#{login.rememberMe}")
                .contains("#{login.submit}")
                .doesNotContain("#{login.forgotPassword}")
                .doesNotContain("#{login.deployment.question}")
                .doesNotContain("#{login.deployment.link}");
    }

    @Test
    void sharedHeadExposesLocaleBootstrapForClientScripts() throws Exception {
        String template = loadTemplate("templates/fragments/app-shell.html");

        assertThat(template)
                .contains("window.STREAMCRAFT_LOCALE")
                .contains("window.STREAMCRAFT_MESSAGES")
                .contains("${currentLocaleTag}")
                .contains("${streamCraftMessages}");
    }

    @Test
    void sharedHeadBindsLanguageSelectsToCurrentUrlLangParameter() throws Exception {
        String template = loadTemplate("templates/fragments/app-shell.html");

        assertThat(template)
                .contains("querySelectorAll('[data-language-select]')")
                .contains("new URL(window.location.href)")
                .contains("url.searchParams.set('lang', selectedLocale)")
                .contains("window.location.assign(url.toString())");
    }

    @Test
    void sharedHeadDefinesBalancedThemeTokensAndUtilityClasses() throws Exception {
        String template = loadTemplate("templates/fragments/app-shell.html");

        assertThat(template)
                .contains("--sc-bg")
                .contains("--sc-surface")
                .contains("--sc-card")
                .contains("--sc-border")
                .contains("--sc-text")
                .contains("--sc-muted")
                .contains("--sc-primary")
                .contains("--sc-primary-foreground")
                .contains(".sc-shell-page")
                .contains(".sc-card")
                .contains(".sc-btn-primary")
                .contains(".sc-btn-secondary")
                .contains(".sc-input")
                .contains(".sc-pill");
    }

    @Test
    void loginPageUsesSharedShellCardAndButtonLanguage() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("sc-shell-page")
                .contains("sc-card-strong")
                .contains("sc-btn-primary")
                .contains("sc-input")
                .contains("StreamCraft");
    }

    @Test
    void loginPageUsesSharedBrandLogoAsset() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("th:src=\"@{/brand-logo.png}\"")
                .contains("alt=\"StreamCraft\"");
    }

    @Test
    void loginPagePlacesLanguageSelectInTopRightCorner() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-login-language-switcher")
                .contains("id=\"login-language-select\"")
                .contains("data-language-select")
                .contains("th:aria-label=\"#{language.switcher.label}\"")
                .contains("th:selected=\"${currentLocaleTag == 'zh-CN'}\"")
                .contains("th:selected=\"${currentLocaleTag == 'en-US'}\"")
                .contains("th:text=\"#{language.zhCN}\"")
                .contains("th:text=\"#{language.enUS}\"");
    }

    private String loadTemplate() throws IOException {
        return loadTemplate("templates/login.html");
    }

    private String loadTemplate(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
