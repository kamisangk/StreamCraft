(function () {
    function interpolate(template, params) {
        if (!params || typeof template !== 'string') {
            return template;
        }
        return template.replace(/\{(\d+)}/g, (_, index) => {
            const value = params[Number(index)];
            return value === undefined || value === null ? '' : String(value);
        });
    }

    function translate(key, fallback, params) {
        const messages = window.STREAMCRAFT_MESSAGES || {};
        const template = Object.prototype.hasOwnProperty.call(messages, key)
            ? messages[key]
            : (fallback ?? key);
        return interpolate(template, params);
    }

    window.StreamCraftI18n = {
        locale: window.STREAMCRAFT_LOCALE || 'zh-CN',
        messages: window.STREAMCRAFT_MESSAGES || {},
        t: translate
    };
})();
