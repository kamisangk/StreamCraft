package com.streamcraft.service.config;

import java.util.Locale;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UiMessageService {

    private final MessageSource messageSource;
    private final Locale fixedLocale;

    @Autowired
    public UiMessageService(MessageSource messageSource) {
        this(messageSource, null);
    }

    private UiMessageService(MessageSource messageSource, Locale fixedLocale) {
        this.messageSource = messageSource;
        this.fixedLocale = fixedLocale;
    }

    public static UiMessageService englishFallback() {
        ResourceBundleMessageSource source = new ResourceBundleMessageSource();
        source.setBasename("messages");
        source.setDefaultEncoding("UTF-8");
        source.setFallbackToSystemLocale(false);
        return new UiMessageService(source, Locale.US);
    }

    public String get(String key, Object... args) {
        Locale locale = fixedLocale == null ? LocaleContextHolder.getLocale() : fixedLocale;
        return messageSource.getMessage(key, args, key, locale);
    }
}
