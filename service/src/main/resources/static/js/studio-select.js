(function () {
    const t = window.StreamCraftI18n?.t || ((key, fallback) => fallback ?? key);

    const SELECT_META = {
        "source-consume-mode": {
            accent: "blue",
            options: {
                earliest: { description: t("studio.select.consume.earliest", "Consume from the earliest available offset."), badge: "EARLY" },
                latest: { description: t("studio.select.consume.latest", "Consume only new messages after startup."), badge: "LIVE" },
                committed: { description: t("studio.select.consume.committed", "Require a committed offset; otherwise fail."), badge: "NONE" }
            }
        },
        "source-auth-type": {
            accent: "blue",
            options: {
                NONE: { description: t("studio.select.auth.none", "No credentials required."), badge: "OPEN" },
                SASL_PLAIN: { description: t("studio.select.auth.plain", "Authenticate with username and password through SASL/PLAIN."), badge: "PLAIN" },
                SASL_SCRAM: { description: t("studio.select.auth.scram", "Authenticate with username and password through SASL/SCRAM."), badge: "SCRAM" }
            }
        },
        "source-scram-mechanism": {
            accent: "blue",
            options: {
                "SCRAM-SHA-256": { description: t("studio.select.scram.sha256", "Balances compatibility and hashing strength."), badge: "256" },
                "SCRAM-SHA-512": { description: t("studio.select.scram.sha512", "Stronger SCRAM hashing mechanism."), badge: "512" }
            }
        },
        "source-format": {
            accent: "blue",
            options: {
                JSON: {}
            }
        },
        "sink-delivery-guarantee": {
            accent: "emerald",
            options: {
                NONE: { description: t("studio.select.delivery.none", "No extra delivery guarantee; prioritize throughput."), badge: "FAST" },
                AT_LEAST_ONCE: { description: t("studio.select.delivery.atLeastOnce", "At least once delivery; prioritize no message loss."), badge: "SAFE" },
                EXACTLY_ONCE: { description: t("studio.select.delivery.exactlyOnce", "Exactly once delivery; avoid duplicate writes."), badge: "STRICT" }
            }
        },
        "sink-auth-type": {
            accent: "emerald",
            options: {
                NONE: { description: t("studio.select.auth.none", "No credentials required."), badge: "OPEN" },
                SASL_PLAIN: { description: t("studio.select.auth.plain", "Authenticate with username and password through SASL/PLAIN."), badge: "PLAIN" },
                SASL_SCRAM: { description: t("studio.select.auth.scram", "Authenticate with username and password through SASL/SCRAM."), badge: "SCRAM" }
            }
        },
        "sink-scram-mechanism": {
            accent: "emerald",
            options: {
                "SCRAM-SHA-256": { description: t("studio.select.scram.sha256", "Balances compatibility and hashing strength."), badge: "256" },
                "SCRAM-SHA-512": { description: t("studio.select.scram.sha512", "Stronger SCRAM hashing mechanism."), badge: "512" }
            }
        },
        "sink-format": {
            accent: "emerald",
            options: {
                JSON: {}
            }
        }
    };

    const controllers = new Map();

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function selectMeta(select) {
        return SELECT_META[select.id] || {};
    }

    function parseSelectOptions(select) {
        const meta = selectMeta(select);
        return Array.from(select.options).map((option, index) => {
            const optionMeta = meta.options?.[option.value] || {};
            return {
                value: option.value,
                label: option.textContent || option.label || option.value,
                description: option.dataset.description || optionMeta.description || "",
                badge: option.dataset.badge || optionMeta.badge || "",
                disabled: option.disabled,
                index
            };
        });
    }

    class StudioSelect {
        constructor(select) {
            this.select = select;
            this.meta = selectMeta(select);
            this.options = [];
            this.filtered = [];
            this.activeIndex = 0;
            this.searchable = select.dataset.selectSearch === "true" || Boolean(this.meta.search);

            this.render();
            this.bind();
            this.refreshOptionsFromDom();
        }

        render() {
            this.select.classList.add("studio-select-native");

            this.shell = document.createElement("div");
            this.shell.className = "studio-select-shell";
            this.shell.dataset.accent = this.select.dataset.selectAccent || this.meta.accent || "blue";

            this.trigger = document.createElement("button");
            this.trigger.type = "button";
            this.trigger.className = "studio-select-trigger";
            this.trigger.setAttribute("aria-haspopup", "listbox");
            this.trigger.setAttribute("aria-expanded", "false");
            this.trigger.innerHTML = `
                <div class="studio-select-trigger-copy">
                    <span class="studio-select-trigger-label"></span>
                    <span class="studio-select-trigger-description"></span>
                </div>
                <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path>
                </svg>
            `;

            this.menu = document.createElement("div");
            this.menu.className = "studio-select-menu";
            this.menu.hidden = true;

            if (this.searchable) {
                this.searchInput = document.createElement("input");
                this.searchInput.type = "text";
                this.searchInput.className = "studio-select-search";
                this.searchInput.placeholder = this.select.dataset.selectPlaceholder || this.meta.placeholder || t("studio.select.search.placeholder", "Search options");
                this.menu.appendChild(this.searchInput);
            }

            this.optionsHost = document.createElement("div");
            this.optionsHost.className = "studio-select-options";
            this.menu.appendChild(this.optionsHost);

            this.shell.appendChild(this.trigger);
            this.shell.appendChild(this.menu);
            this.select.insertAdjacentElement("afterend", this.shell);
        }

        bind() {
            this.trigger.addEventListener("click", () => this.toggle());
            this.trigger.addEventListener("keydown", event => this.onTriggerKeydown(event));

            if (this.searchInput) {
                this.searchInput.addEventListener("input", () => {
                    const keyword = this.searchInput.value.trim().toLowerCase();
                    this.filtered = this.options.filter(item =>
                        item.label.toLowerCase().includes(keyword) || item.description.toLowerCase().includes(keyword)
                    );
                    this.activeIndex = 0;
                    this.renderOptions();
                });
                this.searchInput.addEventListener("keydown", event => this.onListKeydown(event));
            }

            this.select.addEventListener("change", () => this.syncTrigger());

            document.addEventListener("click", event => {
                if (!this.shell.contains(event.target)) {
                    this.close();
                }
            });

            this.observer = new MutationObserver(() => this.refreshOptionsFromDom());
            this.observer.observe(this.select, {
                childList: true,
                subtree: true,
                attributes: true,
                attributeFilter: ["disabled", "label", "value", "selected", "data-description", "data-badge"]
            });
        }

        onTriggerKeydown(event) {
            if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                this.toggle();
                return;
            }
            if (event.key === "ArrowDown") {
                event.preventDefault();
                this.open();
                this.moveActive(1);
                return;
            }
            if (event.key === "ArrowUp") {
                event.preventDefault();
                this.open();
                this.moveActive(-1);
            }
        }

        onListKeydown(event) {
            if (event.key === "ArrowDown") {
                event.preventDefault();
                this.moveActive(1);
                return;
            }
            if (event.key === "ArrowUp") {
                event.preventDefault();
                this.moveActive(-1);
                return;
            }
            if (event.key === "Enter") {
                event.preventDefault();
                const item = this.filtered[this.activeIndex];
                if (item) {
                    this.commit(item.value);
                }
                return;
            }
            if (event.key === "Escape") {
                event.preventDefault();
                this.close();
                this.trigger.focus();
            }
        }

        refreshOptionsFromDom() {
            this.options = parseSelectOptions(this.select);
            this.filtered = [...this.options];

            if (!this.options.find(option => option.value === this.select.value) && this.options.length > 0) {
                this.select.value = this.options[0].value;
            }

            this.activeIndex = Math.max(0, this.options.findIndex(option => option.value === this.select.value));
            this.syncTrigger();
            this.renderOptions();
        }

        syncTrigger() {
            const selected = this.options.find(option => option.value === this.select.value) || this.options[0];
            const labelElement = this.trigger.querySelector(".studio-select-trigger-label");
            const descriptionElement = this.trigger.querySelector(".studio-select-trigger-description");

            if (!selected) {
                labelElement.textContent = "";
                descriptionElement.textContent = "";
                return;
            }

            labelElement.textContent = selected.label;
            descriptionElement.textContent = selected.description;
            this.renderOptions();
        }

        renderOptions() {
            this.optionsHost.innerHTML = "";

            if (!this.filtered.length) {
                const empty = document.createElement("div");
                empty.className = "studio-select-empty";
                empty.textContent = t("studio.select.empty", "No matching options");
                this.optionsHost.appendChild(empty);
                return;
            }

            const currentValue = this.select.value;
            this.filtered.forEach((item, index) => {
                const button = document.createElement("button");
                button.type = "button";
                button.className = "studio-select-option";
                if (index === this.activeIndex) {
                    button.classList.add("is-active");
                }
                button.disabled = item.disabled;
                button.innerHTML = `
                    <div class="studio-select-option-row">
                        <div class="studio-select-option-copy">
                            <span class="studio-select-option-label">${escapeHtml(item.label)}</span>
                            <span class="studio-select-option-description">${escapeHtml(item.description)}</span>
                        </div>
                        <span class="studio-select-option-badge">${escapeHtml(item.badge)}</span>
                    </div>
                `;
                if (item.value === currentValue) {
                    button.querySelector(".studio-select-option-label").style.color = "var(--studio-select-accent)";
                }
                button.addEventListener("click", () => this.commit(item.value));
                this.optionsHost.appendChild(button);
            });
        }

        moveActive(step) {
            if (!this.filtered.length) {
                return;
            }
            this.activeIndex = (this.activeIndex + step + this.filtered.length) % this.filtered.length;
            this.renderOptions();
        }

        commit(value) {
            const changed = this.select.value !== value;
            this.select.value = value;
            this.syncTrigger();
            this.close();
            if (changed) {
                this.select.dispatchEvent(new Event("input", { bubbles: true }));
                this.select.dispatchEvent(new Event("change", { bubbles: true }));
            }
        }

        toggle() {
            if (this.menu.hidden) {
                this.open();
            } else {
                this.close();
            }
        }

        open() {
            controllers.forEach(controller => {
                if (controller !== this) {
                    controller.close();
                }
            });

            this.menu.hidden = false;
            this.shell.classList.add("is-open");
            this.trigger.setAttribute("aria-expanded", "true");
            this.filtered = [...this.options];
            this.activeIndex = Math.max(0, this.filtered.findIndex(option => option.value === this.select.value));
            if (this.searchInput) {
                this.searchInput.value = "";
                this.searchInput.focus();
            }
            this.renderOptions();
        }

        close() {
            this.menu.hidden = true;
            this.shell.classList.remove("is-open");
            this.trigger.setAttribute("aria-expanded", "false");
        }
    }

    function prepareSelect(select) {
        const meta = selectMeta(select);
        if (!meta.accent && select.dataset.studioSelect !== "true") {
            return false;
        }
        select.dataset.studioSelect = "true";
        if (!select.dataset.selectAccent && meta.accent) {
            select.dataset.selectAccent = meta.accent;
        }
        if (!select.dataset.selectPlaceholder && meta.placeholder) {
            select.dataset.selectPlaceholder = meta.placeholder;
        }
        if (!select.dataset.selectSearch && meta.search) {
            select.dataset.selectSearch = "true";
        }
        return true;
    }

    function init(root = document) {
        root.querySelectorAll("select").forEach(select => {
            if (!prepareSelect(select)) {
                return;
            }

            const existing = controllers.get(select.id);
            if (existing) {
                existing.refreshOptionsFromDom();
                return;
            }

            controllers.set(select.id, new StudioSelect(select));
        });
    }

    window.StudioSelectEnhancer = {
        init,
        refreshValue(id) {
            controllers.get(id)?.syncTrigger();
        },
        refreshOptions(id) {
            controllers.get(id)?.refreshOptionsFromDom();
        }
    };
})();
