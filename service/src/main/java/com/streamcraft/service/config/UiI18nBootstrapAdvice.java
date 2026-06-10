package com.streamcraft.service.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import org.springframework.context.MessageSource;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

@ControllerAdvice
public class UiI18nBootstrapAdvice {

    private static final List<String> MESSAGE_KEYS = List.of(
            "login.documentTitle",
            "login.hero.title.line1",
            "login.hero.title.line2",
            "login.hero.description",
            "login.title",
            "login.subtitle",
            "login.error.invalidCredentials",
            "login.logout.success",
            "login.username.label",
            "login.username.placeholder",
            "login.password.label",
            "login.password.placeholder",
            "login.rememberMe",
            "login.submit",
            "page.placeholder.title",
            "page.placeholder.subtitle",
            "page.main.title",
            "page.pipelines.title",
            "page.pipelines.subtitle",
            "page.runtimeTarget.title",
            "page.runtimeTarget.subtitle",
            "language.switcher.label",
            "language.zhCN",
            "language.enUS",
            "topbar.themeToggle",
            "topbar.logout",
            "nav.pipelineMonitor",
            "nav.pipelines",
            "nav.runtimeTarget"
    );

    private final MessageSource messageSource;

    public UiI18nBootstrapAdvice(MessageSource messageSource) {
        this.messageSource = messageSource;
    }

    @ModelAttribute("currentLocaleTag")
    public String currentLocaleTag(Locale locale) {
        return locale.toLanguageTag();
    }

    @ModelAttribute("streamCraftMessages")
    public Map<String, String> streamCraftMessages(Locale locale) {
        Map<String, String> messages = new LinkedHashMap<>();
        for (String key : MESSAGE_KEYS) {
            messages.put(key, messageSource.getMessage(key, null, key, locale));
        }
        try {
            ResourceBundle bundle = ResourceBundle.getBundle("messages", locale);
            for (String key : bundle.keySet()) {
                messages.put(key, messageSource.getMessage(key, null, key, locale));
            }
        } catch (MissingResourceException ignored) {
            // Tests may use a StaticMessageSource without a backing ResourceBundle.
        }
        return messages;
    }
}
