package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.context.support.StaticMessageSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.ui.Model;

class ReadableUiCopyContractTest {

    @Test
    void pageControllerResolvesTitlesAndSubtitlesThroughMessageSource() {
        StaticMessageSource messageSource = new StaticMessageSource();
        messageSource.addMessage("page.main.title", Locale.US, "Task Monitor");
        messageSource.addMessage("page.pipelines.title", Locale.US, "Pipelines");
        messageSource.addMessage("page.pipelines.subtitle", Locale.US, "Create, save, and run real-time pipelines.");
        messageSource.addMessage("page.runtimeTarget.title", Locale.US, "Flink");
        messageSource.addMessage("page.runtimeTarget.subtitle", Locale.US, "Choose one Flink runtime mode for this service.");
        PageController controller = new PageController(messageSource);
        UsernamePasswordAuthenticationToken auth =
                new UsernamePasswordAuthenticationToken("admin", "password");

        Model mainModel = new ExtendedModelMap();
        Model pipelinesModel = new ExtendedModelMap();
        Model runtimeTargetModel = new ExtendedModelMap();

        controller.main(auth, mainModel, Locale.US);
        controller.pipelines(auth, pipelinesModel, Locale.US);
        controller.runtimeTarget(auth, runtimeTargetModel, Locale.US);

        assertThat(mainModel.getAttribute("pageTitle")).isEqualTo("Task Monitor");
        assertThat(mainModel.getAttribute("pageSubtitle")).isNull();
        assertThat(pipelinesModel.getAttribute("pageTitle")).isEqualTo("Pipelines");
        assertThat(pipelinesModel.getAttribute("pageSubtitle")).isEqualTo("Create, save, and run real-time pipelines.");
        assertThat(runtimeTargetModel.getAttribute("pageTitle")).isEqualTo("Flink");
        assertThat(runtimeTargetModel.getAttribute("pageSubtitle")).isEqualTo("Choose one Flink runtime mode for this service.");
    }

    @Test
    void pageControllerUsesReadableChineseTitlesAndOptionalSubtitles() {
        PageController controller = new PageController();
        UsernamePasswordAuthenticationToken auth =
                new UsernamePasswordAuthenticationToken("admin", "password");

        Model mainModel = new ExtendedModelMap();
        Model pipelinesModel = new ExtendedModelMap();
        Model runtimeTargetModel = new ExtendedModelMap();

        controller.main(auth, mainModel);
        controller.pipelines(auth, pipelinesModel);
        controller.runtimeTarget(auth, runtimeTargetModel);

        assertThat(mainModel.getAttribute("pageTitle")).isEqualTo("\u4efb\u52a1\u76d1\u63a7");
        assertThat(mainModel.getAttribute("pageSubtitle")).isNull();
        assertThat(pipelinesModel.getAttribute("pageTitle")).isEqualTo("\u4efb\u52a1\u7ba1\u7406");
        assertThat(pipelinesModel.getAttribute("pageSubtitle"))
                .isEqualTo("\u521b\u5efa\u3001\u4fdd\u5b58\u5e76\u8fd0\u884c\u5b9e\u65f6\u4efb\u52a1\u3002");
        assertThat(runtimeTargetModel.getAttribute("pageTitle")).isEqualTo("Flink");
        assertThat(runtimeTargetModel.getAttribute("pageSubtitle"))
                .isEqualTo("\u9009\u62e9\u4e00\u79cd Flink \u8fd0\u884c\u6a21\u5f0f\u4f5c\u4e3a\u5f53\u524d service \u7684\u552f\u4e00\u8fd0\u884c\u76ee\u6807\u3002");
    }

    @Test
    void sharedShellFragmentsUseMessageKeysForNavigationAndTopbarCopy() throws Exception {
        String topbar = loadTemplate("templates/fragments/topbar.html");
        String sidebar = loadTemplate("templates/fragments/sidebar.html");

        assertThat(topbar)
                .contains("#{page.placeholder.title}")
                .contains("#{page.placeholder.subtitle}")
                .contains("#{language.switcher.label}")
                .contains("#{language.zhCN}")
                .contains("#{language.enUS}")
                .contains("#{topbar.themeToggle}")
                .contains("#{topbar.logout}");

        assertThat(sidebar)
                .contains("#{nav.pipelineMonitor}")
                .contains("#{nav.pipelines}")
                .contains("#{nav.runtimeTarget}");
    }

    @Test
    void studioOperatorLabelsUseReadableProductNames() {
        ResourceBundleMessageSource messageSource = messageSource();

        assertThat(messageSource.getMessage("studio.operator.eval", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5b57\u6bb5\u8ba1\u7b97");
        assertThat(messageSource.getMessage("studio.operator.eval", null, Locale.US))
                .isEqualTo("Field calculation");
        assertThat(messageSource.getMessage("studio.operator.lookupJoin", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u7ef4\u8868\u5173\u8054");
        assertThat(messageSource.getMessage("studio.operator.lookupJoin", null, Locale.US))
                .isEqualTo("Dimension join");
        assertThat(messageSource.getMessage("studio.operator.lookupEnrich", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u7ef4\u8868\u8865\u5168");
        assertThat(messageSource.getMessage("studio.operator.lookupEnrich", null, Locale.US))
                .isEqualTo("Dimension enrichment");
        assertThat(messageSource.getMessage("studio.operator.streamJoin", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u53cc\u6d41\u5173\u8054");
        assertThat(messageSource.getMessage("studio.operator.streamJoin", null, Locale.US))
                .isEqualTo("Two-stream join");
        assertThat(messageSource.getMessage("studio.operator.dataQuality", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u6570\u636e\u6821\u9a8c");
        assertThat(messageSource.getMessage("studio.operator.dataQuality", null, Locale.US))
                .isEqualTo("Data validation");
        assertThat(messageSource.getMessage("studio.operator.maskHash", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u654f\u611f\u5b57\u6bb5\u5904\u7406");
        assertThat(messageSource.getMessage("studio.operator.maskHash", null, Locale.US))
                .isEqualTo("Sensitive field handling");
    }

    @Test
    void loginTemplateUsesMessageKeysForReadableCopy() throws Exception {
        String loginTemplate = loadTemplate("templates/login.html");

        assertThat(loginTemplate)
                .contains("#{login.documentTitle}")
                .contains("#{login.title}")
                .contains("#{login.username.label}")
                .contains("#{login.rememberMe}");
    }

    @Test
    void standardPagesUseMessageKeysAndBrowserI18nForReadableCopy() throws Exception {
        String mainTemplate = loadTemplate("templates/main.html");
        String pipelineListTemplate = loadTemplate("templates/pipeline-list.html");
        String runtimeTargetTemplate = loadTemplate("templates/runtime-target.html");
        String monitorDetailTemplate = loadTemplate("templates/pipeline-monitor-detail.html");

        assertThat(mainTemplate)
                .contains("#{main.hero.label}")
                .contains("StreamCraftI18n?.t")
                .contains("t('main.slot.good'");

        assertThat(pipelineListTemplate)
                .contains("#{pipelines.hero.label}")
                .contains("#{pipelines.hero.title}")
                .contains("StreamCraftI18n?.t")
                .contains("pipelines.status.running");

        assertThat(runtimeTargetTemplate)
                .contains("#{runtimeTarget.hero.label}")
                .contains("#{runtimeTarget.target.title}")
                .contains("StreamCraftI18n?.t")
                .contains("runtimeTarget.status.connected");

        assertThat(monitorDetailTemplate)
                .contains("#{monitorDetail.title}")
                .contains("#{monitorDetail.refresh}")
                .contains("th:lang=\"${currentLocaleTag}\"")
                .contains("th:src=\"@{/js/studio-editor.js}\"");
    }

    @Test
    void studioStreamJoinConfigMessagesResolveToReadableCopy() {
        ResourceBundleMessageSource messageSource = messageSource();

        assertThat(messageSource.getMessage("studio.field.joinType", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5173\u8054\u7c7b\u578b");
        assertThat(messageSource.getMessage("studio.field.missingStrategy", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u672a\u547d\u4e2d\u7b56\u7565");
        assertThat(messageSource.getMessage("studio.field.overwriteTargetField", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u8986\u76d6\u76ee\u6807\u5b57\u6bb5");
        assertThat(messageSource.getMessage("studio.select.join.left", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5de6\u5173\u8054");
        assertThat(messageSource.getMessage("studio.select.join.inner", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5185\u5173\u8054");
        assertThat(messageSource.getMessage("studio.select.missing.keepOriginal", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4fdd\u7559\u539f\u8bb0\u5f55");
        assertThat(messageSource.getMessage("studio.select.missing.putNull", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5199\u5165 null");
        assertThat(messageSource.getMessage("studio.select.timeUnit.seconds", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u79d2");
        assertThat(messageSource.getMessage("studio.select.aggregateFunction.count", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u8ba1\u6570");
        assertThat(messageSource.getMessage("studio.select.aggregateFunction.countDistinct", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u53bb\u91cd\u8ba1\u6570");
        assertThat(messageSource.getMessage("studio.field.aggregationLimit", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("Top N \u6570\u91cf");
        assertThat(messageSource.getMessage("studio.field.eventTimeField", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e8b\u4ef6\u65f6\u95f4\u5b57\u6bb5");
        assertThat(messageSource.getMessage("studio.field.eventTimeUnit", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e8b\u4ef6\u65f6\u95f4\u5355\u4f4d");
        assertThat(messageSource.getMessage("studio.field.aggregateSortField", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u6392\u5e8f\u5b57\u6bb5");
        assertThat(messageSource.getMessage("studio.field.aggregateSortDirection", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u6392\u5e8f\u65b9\u5411");
        assertThat(messageSource.getMessage("studio.field.outputMode", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u8f93\u51fa\u5f62\u6001");
        assertThat(messageSource.getMessage("studio.select.outputMode.nested", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5d4c\u5957");
        assertThat(messageSource.getMessage("studio.select.outputMode.flat", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5e73\u94fa");
        assertThat(messageSource.getMessage("studio.select.timeDerive.year", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5e74");
        assertThat(messageSource.getMessage("studio.select.parseError.fail", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5931\u8d25\u5e76\u505c\u6b62");
        assertThat(messageSource.getMessage("studio.action.moveTimeDeriveUp", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e0a\u79fb\u6d3e\u751f\u5b57\u6bb5");
        assertThat(messageSource.getMessage("studio.action.moveTimeDeriveDown", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e0b\u79fb\u6d3e\u751f\u5b57\u6bb5");
        assertThat(messageSource.getMessage("studio.action.removeTimeDerive", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5220\u9664\u6d3e\u751f\u5b57\u6bb5");
        assertThat(messageSource.getMessage("studio.field.maskHashAction", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5904\u7406\u65b9\u5f0f");
        assertThat(messageSource.getMessage("studio.field.maskHashKeepFirst", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4fdd\u7559\u524d\u51e0\u4f4d");
        assertThat(messageSource.getMessage("studio.select.maskHash.hash", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u54c8\u5e0c\uff1a\u751f\u6210\u4e0d\u53ef\u8fd8\u539f\u5b57\u7b26\u4e32");
        assertThat(messageSource.getMessage("studio.action.moveMaskHashRuleUp", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e0a\u79fb\u8131\u654f/\u54c8\u5e0c\u89c4\u5219");
        assertThat(messageSource.getMessage("studio.action.moveMaskHashRuleDown", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e0b\u79fb\u8131\u654f/\u54c8\u5e0c\u89c4\u5219");
        assertThat(messageSource.getMessage("studio.action.removeMaskHashRule", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5220\u9664\u8131\u654f/\u54c8\u5e0c\u89c4\u5219");
        assertThat(messageSource.getMessage("studio.select.route.firstMatch", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u9996\u4e2a\u5339\u914d");
        assertThat(messageSource.getMessage("studio.select.route.allMatches", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5168\u90e8\u5339\u914d");
        assertThat(messageSource.getMessage("studio.field.routePortId", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u8f93\u51fa\u7aef\u53e3");
        assertThat(messageSource.getMessage("studio.field.routeCondition", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5206\u6d41\u6761\u4ef6");
        assertThat(messageSource.getMessage("studio.action.addRoute", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u65b0\u589e\u5206\u6d41");
        assertThat(messageSource.getMessage("studio.action.moveRouteUp", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e0a\u79fb\u5206\u6d41");
        assertThat(messageSource.getMessage("studio.action.moveRouteDown", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e0b\u79fb\u5206\u6d41");
        assertThat(messageSource.getMessage("studio.action.removeRoute", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5220\u9664\u5206\u6d41");
        assertThat(messageSource.getMessage("studio.field.caseWhenValueMode", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u8f93\u51fa\u7c7b\u578b");
        assertThat(messageSource.getMessage("studio.select.caseWhen.expression", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u8868\u8fbe\u5f0f");
        assertThat(messageSource.getMessage("studio.select.caseWhen.none", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u65e0\u9ed8\u8ba4\u503c");
        assertThat(messageSource.getMessage("studio.action.moveCaseUp", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e0a\u79fb\u6761\u4ef6");
        assertThat(messageSource.getMessage("studio.action.moveCaseDown", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u4e0b\u79fb\u6761\u4ef6");
        assertThat(messageSource.getMessage("studio.action.removeCase", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5220\u9664\u6761\u4ef6");
        assertThat(messageSource.getMessage("studio.select.dataQuality.fail", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u5931\u8d25\u5e76\u505c\u6b62");
        assertThat(messageSource.getMessage("studio.field.dataQualityRuleType", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u6821\u9a8c\u7c7b\u578b");
        assertThat(messageSource.getMessage("studio.field.dataQualityValueType", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u503c\u7c7b\u578b");
        assertThat(messageSource.getMessage("studio.field.dataQualityMinLength", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u6700\u5c0f\u957f\u5ea6");
        assertThat(messageSource.getMessage("studio.field.dataQualityMaxLength", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u6700\u5927\u957f\u5ea6");
        assertThat(messageSource.getMessage("studio.field.dataQualityCustomMessage", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u81ea\u5b9a\u4e49\u9519\u8bef\u6d88\u606f");
        assertThat(messageSource.getMessage("studio.select.dataQualityRule.length", null, Locale.SIMPLIFIED_CHINESE))
                .isEqualTo("\u957f\u5ea6");

        assertThat(messageSource.getMessage("studio.field.joinType", null, Locale.US))
                .isEqualTo("Join type");
        assertThat(messageSource.getMessage("studio.select.join.left", null, Locale.US))
                .isEqualTo("Left join");
        assertThat(messageSource.getMessage("studio.select.timeUnit.seconds", null, Locale.US))
                .isEqualTo("Seconds");
        assertThat(messageSource.getMessage("studio.select.aggregateFunction.count", null, Locale.US))
                .isEqualTo("Count");
        assertThat(messageSource.getMessage("studio.select.aggregateFunction.countDistinct", null, Locale.US))
                .isEqualTo("Count distinct");
        assertThat(messageSource.getMessage("studio.field.aggregationLimit", null, Locale.US))
                .isEqualTo("Top N count");
        assertThat(messageSource.getMessage("studio.field.eventTimeField", null, Locale.US))
                .isEqualTo("Event time field");
        assertThat(messageSource.getMessage("studio.field.eventTimeUnit", null, Locale.US))
                .isEqualTo("Event time unit");
        assertThat(messageSource.getMessage("studio.field.aggregateSortField", null, Locale.US))
                .isEqualTo("Sort field");
        assertThat(messageSource.getMessage("studio.field.aggregateSortDirection", null, Locale.US))
                .isEqualTo("Sort direction");
        assertThat(messageSource.getMessage("studio.field.outputMode", null, Locale.US))
                .isEqualTo("Output shape");
        assertThat(messageSource.getMessage("studio.select.outputMode.nested", null, Locale.US))
                .isEqualTo("Nested");
        assertThat(messageSource.getMessage("studio.select.outputMode.flat", null, Locale.US))
                .isEqualTo("Flat");
        assertThat(messageSource.getMessage("studio.select.timeDerive.year", null, Locale.US))
                .isEqualTo("Year");
        assertThat(messageSource.getMessage("studio.select.parseError.fail", null, Locale.US))
                .isEqualTo("Fail and stop");
        assertThat(messageSource.getMessage("studio.action.moveTimeDeriveUp", null, Locale.US))
                .isEqualTo("Move derived field up");
        assertThat(messageSource.getMessage("studio.action.moveTimeDeriveDown", null, Locale.US))
                .isEqualTo("Move derived field down");
        assertThat(messageSource.getMessage("studio.action.removeTimeDerive", null, Locale.US))
                .isEqualTo("Remove derived field");
        assertThat(messageSource.getMessage("studio.field.maskHashAction", null, Locale.US))
                .isEqualTo("Action");
        assertThat(messageSource.getMessage("studio.field.maskHashKeepFirst", null, Locale.US))
                .isEqualTo("Keep first");
        assertThat(messageSource.getMessage("studio.select.maskHash.hash", null, Locale.US))
                .isEqualTo("Hash: create a non-reversible string");
        assertThat(messageSource.getMessage("studio.action.moveMaskHashRuleUp", null, Locale.US))
                .isEqualTo("Move mask/hash rule up");
        assertThat(messageSource.getMessage("studio.action.moveMaskHashRuleDown", null, Locale.US))
                .isEqualTo("Move mask/hash rule down");
        assertThat(messageSource.getMessage("studio.action.removeMaskHashRule", null, Locale.US))
                .isEqualTo("Remove mask/hash rule");
        assertThat(messageSource.getMessage("studio.select.route.firstMatch", null, Locale.US))
                .isEqualTo("First match");
        assertThat(messageSource.getMessage("studio.select.route.allMatches", null, Locale.US))
                .isEqualTo("All matches");
        assertThat(messageSource.getMessage("studio.field.routePortId", null, Locale.US))
                .isEqualTo("Output port");
        assertThat(messageSource.getMessage("studio.field.routeCondition", null, Locale.US))
                .isEqualTo("Route condition");
        assertThat(messageSource.getMessage("studio.action.addRoute", null, Locale.US))
                .isEqualTo("Add route");
        assertThat(messageSource.getMessage("studio.action.moveRouteUp", null, Locale.US))
                .isEqualTo("Move route up");
        assertThat(messageSource.getMessage("studio.action.moveRouteDown", null, Locale.US))
                .isEqualTo("Move route down");
        assertThat(messageSource.getMessage("studio.action.removeRoute", null, Locale.US))
                .isEqualTo("Remove route");
        assertThat(messageSource.getMessage("studio.field.caseWhenValueMode", null, Locale.US))
                .isEqualTo("Output type");
        assertThat(messageSource.getMessage("studio.select.caseWhen.expression", null, Locale.US))
                .isEqualTo("Expression");
        assertThat(messageSource.getMessage("studio.select.caseWhen.none", null, Locale.US))
                .isEqualTo("No default");
        assertThat(messageSource.getMessage("studio.action.moveCaseUp", null, Locale.US))
                .isEqualTo("Move case up");
        assertThat(messageSource.getMessage("studio.action.moveCaseDown", null, Locale.US))
                .isEqualTo("Move case down");
        assertThat(messageSource.getMessage("studio.action.removeCase", null, Locale.US))
                .isEqualTo("Remove case");
        assertThat(messageSource.getMessage("studio.select.dataQuality.fail", null, Locale.US))
                .isEqualTo("Fail and stop");
        assertThat(messageSource.getMessage("studio.field.dataQualityRuleType", null, Locale.US))
                .isEqualTo("Rule type");
        assertThat(messageSource.getMessage("studio.field.dataQualityValueType", null, Locale.US))
                .isEqualTo("Value type");
        assertThat(messageSource.getMessage("studio.field.dataQualityMinLength", null, Locale.US))
                .isEqualTo("Min length");
        assertThat(messageSource.getMessage("studio.field.dataQualityMaxLength", null, Locale.US))
                .isEqualTo("Max length");
        assertThat(messageSource.getMessage("studio.field.dataQualityCustomMessage", null, Locale.US))
                .isEqualTo("Custom error message");
        assertThat(messageSource.getMessage("studio.select.dataQualityRule.length", null, Locale.US))
                .isEqualTo("Length");
    }

    private String loadTemplate(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    private ResourceBundleMessageSource messageSource() {
        ResourceBundleMessageSource source = new ResourceBundleMessageSource();
        source.setBasename("messages");
        source.setDefaultEncoding("UTF-8");
        source.setFallbackToSystemLocale(false);
        return source;
    }
}
