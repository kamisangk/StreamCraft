package com.streamcraft.service.web;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Locale;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class PageController {

    private final MessageSource messageSource;

    @Autowired
    public PageController(MessageSource messageSource) {
        this.messageSource = messageSource;
    }

    PageController() {
        this(defaultMessageSource());
    }

    @GetMapping("/")
    public String root(Authentication authentication) {
        return isLoggedIn(authentication) ? "redirect:/main" : "redirect:/login";
    }

    @GetMapping("/login")
    public String login(Authentication authentication, HttpServletRequest request) {
        if (isLoggedIn(authentication)) {
            return "redirect:/main";
        }

        Object csrfAttribute = request.getAttribute("_csrf");
        if (!(csrfAttribute instanceof CsrfToken)) {
            csrfAttribute = request.getAttribute(CsrfToken.class.getName());
        }
        if (csrfAttribute instanceof CsrfToken csrfToken) {
            csrfToken.getToken();
        }

        return "login";
    }

    @GetMapping("/main")
    public String main(Authentication authentication, Model model, Locale locale) {
        return renderStandardPage(
                "main",
                "pipeline-monitor",
                "page.main.title",
                null,
                authentication,
                model,
                locale
        );
    }

    public String main(Authentication authentication, Model model) {
        return main(authentication, model, Locale.SIMPLIFIED_CHINESE);
    }

    @GetMapping("/runtime-target")
    public String runtimeTarget(Authentication authentication, Model model, Locale locale) {
        return renderStandardPage(
                "runtime-target",
                "runtime-target",
                "page.runtimeTarget.title",
                "page.runtimeTarget.subtitle",
                authentication,
                model,
                locale
        );
    }

    public String runtimeTarget(Authentication authentication, Model model) {
        return runtimeTarget(authentication, model, Locale.SIMPLIFIED_CHINESE);
    }

    @GetMapping("/pipelines")
    public String pipelines(Authentication authentication, Model model, Locale locale) {
        return renderStandardPage(
                "pipeline-list",
                "pipelines",
                "page.pipelines.title",
                "page.pipelines.subtitle",
                authentication,
                model,
                locale
        );
    }

    public String pipelines(Authentication authentication, Model model) {
        return pipelines(authentication, model, Locale.SIMPLIFIED_CHINESE);
    }

    @GetMapping("/settings")
    public String settings(Authentication authentication, Model model, Locale locale) {
        return renderStandardPage(
                "settings",
                "settings",
                "page.settings.title",
                "page.settings.subtitle",
                authentication,
                model,
                locale
        );
    }

    public String settings(Authentication authentication, Model model) {
        return settings(authentication, model, Locale.SIMPLIFIED_CHINESE);
    }

    private String renderStandardPage(String viewName,
                                      String activeNav,
                                      String pageTitleKey,
                                      String pageSubtitleKey,
                                      Authentication authentication,
                                      Model model,
                                      Locale locale) {
        model.addAttribute("displayName", authentication.getName());
        model.addAttribute("activeNav", activeNav);
        model.addAttribute("pageTitle", message(pageTitleKey, locale));
        model.addAttribute("pageSubtitle", pageSubtitleKey == null ? null : message(pageSubtitleKey, locale));
        return viewName;
    }

    @GetMapping({"/studio", "/studio/{id}"})
    public String studio(@org.springframework.web.bind.annotation.PathVariable(required = false) Long id,
                         Authentication authentication,
                         Model model) {
        model.addAttribute("displayName", authentication.getName());
        model.addAttribute("pipelineId", id);
        return "studio";
    }

    private boolean isLoggedIn(Authentication authentication) {
        return authentication != null
                && authentication.isAuthenticated()
                && !(authentication instanceof AnonymousAuthenticationToken);
    }

    private String message(String key, Locale locale) {
        return messageSource.getMessage(key, null, key, locale);
    }

    private static MessageSource defaultMessageSource() {
        ResourceBundleMessageSource source = new ResourceBundleMessageSource();
        source.setBasename("messages");
        source.setDefaultEncoding("UTF-8");
        return source;
    }
}
