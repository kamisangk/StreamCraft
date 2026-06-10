package com.streamcraft.service.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.StaticMessageSource;

class UiI18nBootstrapAdviceTest {

    @Test
    void exposesCurrentLocaleTag() {
        UiI18nBootstrapAdvice advice = new UiI18nBootstrapAdvice(new StaticMessageSource());

        assertThat(advice.currentLocaleTag(Locale.US)).isEqualTo("en-US");
        assertThat(advice.currentLocaleTag(Locale.SIMPLIFIED_CHINESE)).isEqualTo("zh-CN");
    }

    @Test
    void exposesFlatMessageMapForBrowserBootstrap() {
        StaticMessageSource messageSource = new StaticMessageSource();
        messageSource.addMessage("login.title", Locale.US, "Welcome back");
        messageSource.addMessage("topbar.logout", Locale.US, "Sign out");
        UiI18nBootstrapAdvice advice = new UiI18nBootstrapAdvice(messageSource);

        Map<String, String> messages = advice.streamCraftMessages(Locale.US);

        assertThat(messages)
                .containsEntry("login.title", "Welcome back")
                .containsEntry("topbar.logout", "Sign out");
    }
}
