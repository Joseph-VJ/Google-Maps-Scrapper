(function () {
    const THEME_STORAGE_KEY = 'gm-scraper-theme';
    const body = document.body;
    const toggle = document.querySelector('[data-theme-toggle]');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)');

    const storedTheme = localStorage.getItem(THEME_STORAGE_KEY);
    const hasStoredPreference = storedTheme === 'light' || storedTheme === 'dark';

    function applyTheme(theme) {
        const nextTheme = theme === 'dark' ? 'dark' : 'light';
        body.dataset.theme = nextTheme;
        updateToggleUI(nextTheme);
    }

    function updateToggleUI(theme) {
        if (!toggle) {
            return;
        }
        const icon = toggle.querySelector('[data-theme-toggle-icon]');
        const label = toggle.querySelector('[data-theme-toggle-label]');
        const isDark = theme === 'dark';

        toggle.setAttribute('aria-pressed', String(isDark));
        toggle.setAttribute('aria-label', isDark ? 'Switch to light mode' : 'Switch to dark mode');

        if (icon) {
            icon.className = isDark ? 'fa-solid fa-sun' : 'fa-solid fa-moon';
        }

        if (label) {
            label.textContent = isDark ? 'Light mode' : 'Dark mode';
        }
    }

    function getSystemPreference() {
        return prefersDark.matches ? 'dark' : 'light';
    }

    function setTheme(theme, persist = false) {
        applyTheme(theme);
        if (persist) {
            localStorage.setItem(THEME_STORAGE_KEY, theme);
        }
    }

    // Initialise theme
    if (hasStoredPreference) {
        applyTheme(storedTheme);
    } else {
        applyTheme(getSystemPreference());
    }

    if (!hasStoredPreference) {
        const handleSystemChange = (event) => {
            applyTheme(event.matches ? 'dark' : 'light');
        };

        if (typeof prefersDark.addEventListener === 'function') {
            prefersDark.addEventListener('change', handleSystemChange);
        } else if (typeof prefersDark.addListener === 'function') {
            prefersDark.addListener(handleSystemChange);
        }
    }

    if (toggle) {
        toggle.addEventListener('click', () => {
            const currentTheme = body.dataset.theme === 'dark' ? 'dark' : 'light';
            const nextTheme = currentTheme === 'dark' ? 'light' : 'dark';
            setTheme(nextTheme, true);
        });
    }

    // Enable Bootstrap tooltips and popovers
    document.addEventListener('DOMContentLoaded', () => {
        if (window.bootstrap && typeof window.bootstrap.Tooltip === 'function') {
            document.querySelectorAll('[data-bs-toggle="tooltip"]')
                .forEach((el) => new window.bootstrap.Tooltip(el));
        }
    });

    // Smooth scroll for guided buttons
    document.addEventListener('click', (event) => {
        const trigger = event.target.closest('[data-scroll-target]');
        if (!trigger) {
            return;
        }

        const selector = trigger.getAttribute('data-scroll-target');
        if (!selector) {
            return;
        }

        const target = document.querySelector(selector);
        if (!target) {
            return;
        }

        event.preventDefault();
        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
        if (typeof target.focus === 'function') {
            target.focus({ preventScroll: true });
        }
    });

    function formatTitle(type) {
        switch (type) {
            case 'success':
                return 'Success';
            case 'error':
                return 'Something went wrong';
            case 'warning':
                return 'Warning';
            default:
                return 'Notice';
        }
    }

    function ensureContainer() {
        let container = document.getElementById('notificationContainer');
        if (!container) {
            container = document.createElement('div');
            container.id = 'notificationContainer';
            container.className = 'notification-container';
            body.appendChild(container);
        }
        return container;
    }

    function removeNotification(notification) {
        if (!notification) {
            return;
        }
        notification.classList.remove('show');
        setTimeout(() => {
            notification.remove();
        }, 250);
    }

    window.showNotification = function showNotification(message, type = 'info', options = {}) {
        const { title = formatTitle(type), duration = 5000 } = options;
        const container = ensureContainer();
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;

        const closeBtn = document.createElement('button');
        closeBtn.type = 'button';
        closeBtn.className = 'btn-close';
        closeBtn.setAttribute('aria-label', 'Dismiss notification');
        closeBtn.addEventListener('click', () => removeNotification(notification));

        const header = document.createElement('div');
        header.className = 'notification__header';
        header.innerHTML = `<span>${title}</span>`;
        header.appendChild(closeBtn);

        const bodyEl = document.createElement('div');
        bodyEl.className = 'notification__body';
        bodyEl.innerHTML = message;

        notification.appendChild(header);
        notification.appendChild(bodyEl);

        container.appendChild(notification);

        requestAnimationFrame(() => {
            notification.classList.add('show');
        });

        if (duration > 0) {
            setTimeout(() => removeNotification(notification), duration);
        }

        return notification;
    };
})();
