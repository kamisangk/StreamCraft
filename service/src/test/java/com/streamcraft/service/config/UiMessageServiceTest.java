package com.streamcraft.service.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.context.support.ResourceBundleMessageSource;

class UiMessageServiceTest {

    @AfterEach
    void resetLocale() {
        LocaleContextHolder.resetLocaleContext();
    }

    @Test
    void resolvesMessagesFromCurrentLocale() {
        UiMessageService messages = new UiMessageService(messageSource());

        LocaleContextHolder.setLocale(Locale.SIMPLIFIED_CHINESE);
        assertThat(messages.get("runtimeTarget.error.notConfigured"))
                .isEqualTo("\u5c1a\u672a\u914d\u7f6e Flink \u8fd0\u884c\u76ee\u6807\u3002");

        LocaleContextHolder.setLocale(Locale.US);
        assertThat(messages.get("runtimeTarget.error.notConfigured"))
                .isEqualTo("Flink runtime target has not been configured.");
    }

    @Test
    void englishFallbackUsesEnglishBundleWithoutRequestLocale() {
        assertThat(UiMessageService.englishFallback().get("pipeline.error.notFound"))
                .isEqualTo("Pipeline does not exist.");
    }

    private ResourceBundleMessageSource messageSource() {
        ResourceBundleMessageSource source = new ResourceBundleMessageSource();
        source.setBasename("messages");
        source.setDefaultEncoding("UTF-8");
        source.setFallbackToSystemLocale(false);
        return source;
    }
}
