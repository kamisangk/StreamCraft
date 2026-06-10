package com.streamcraft.service.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.servlet.LocaleResolver;
import org.springframework.web.servlet.i18n.CookieLocaleResolver;
import org.springframework.web.servlet.i18n.LocaleChangeInterceptor;

class WebLocaleConfigTest {

    @Test
    void localeResolverDefaultsToSimplifiedChineseAndWritesLocaleCookie() {
        WebLocaleConfig config = new WebLocaleConfig();

        LocaleResolver resolver = config.localeResolver();

        assertThat(resolver).isInstanceOf(CookieLocaleResolver.class);
        assertThat(resolver.resolveLocale(new MockHttpServletRequest()))
                .isEqualTo(Locale.SIMPLIFIED_CHINESE);

        MockHttpServletResponse response = new MockHttpServletResponse();
        resolver.setLocale(new MockHttpServletRequest(), response, Locale.US);

        assertThat(response.getCookie("streamcraft-locale")).isNotNull();
    }

    @Test
    void localeChangeInterceptorUsesLangRequestParameter() {
        WebLocaleConfig config = new WebLocaleConfig();

        LocaleChangeInterceptor interceptor = config.localeChangeInterceptor();

        assertThat(interceptor.getParamName()).isEqualTo("lang");
    }
}
