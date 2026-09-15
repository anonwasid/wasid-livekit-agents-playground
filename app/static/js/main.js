// LiveKit Dashboard Client-Side JavaScript

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    console.log('LiveKit Dashboard initialized');
    
    // Fix text colors for dark theme
    fixTextColors();

    // Initialize auto-refresh controls
    if (window.DashboardAutoRefresh) {
        window.DashboardAutoRefresh.init();
    }
    
    // Auto-dismiss alerts after 5 seconds
    const alerts = document.querySelectorAll('.alert:not(.alert-permanent)');
    alerts.forEach(alert => {
        setTimeout(() => {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        }, 5000);
    });

    // Initialize custom audio players on page load
    initCustomAudioPlayers(document);
});

/**
 * Fix text color issues in dark theme
 */
function fixTextColors() {
    // Find all elements with black or dark text
    const allElements = document.querySelectorAll('*');
    
    allElements.forEach(element => {
        const computedStyle = window.getComputedStyle(element);
        const color = computedStyle.color;
        
        // Check if text is black or very dark
        if (color === 'rgb(0, 0, 0)' || color === '#000000' || color === 'black' ||
            color === 'rgb(33, 37, 41)' || color === '#212529') {
            
            // Apply proper text color based on element type
            if (element.closest('.alert-success')) {
                element.style.color = 'var(--success)';
            } else if (element.closest('.alert-danger, .alert-error')) {
                element.style.color = 'var(--danger)';
            } else if (element.closest('.alert-warning')) {
                element.style.color = 'var(--warning)';
            } else if (element.closest('.alert-info')) {
                element.style.color = 'var(--info)';
            } else if (element.closest('.text-muted')) {
                element.style.color = 'var(--text-muted)';
            } else {
                element.style.color = 'var(--text-primary)';
            }
        }
    });
    
    // Fix specific recommendation/failure message elements
    const messageBoxes = document.querySelectorAll('.recommendation-box, .message-box, .failure-message, .error-message');
    messageBoxes.forEach(box => {
        box.style.color = 'var(--text-primary)';
        const allChildren = box.querySelectorAll('*');
        allChildren.forEach(child => {
            if (!child.style.color || child.style.color === 'black' || child.style.color === 'rgb(0, 0, 0)') {
                child.style.color = 'inherit';
            }
        });
    });
}

// HTMX event handlers
document.body.addEventListener('htmx:beforeRequest', function(event) {
    // If any audio in the recordings table is currently playing, pause background refresh
    const activeAudios = document.querySelectorAll('#recordings-table-body audio');
    for (const audio of activeAudios) {
        if (!audio.paused && !audio.ended && audio.currentTime > 0) {
            console.log('HTMX refresh paused: call recording audio is currently playing');
            event.preventDefault();
            return;
        }
    }
    if (document.querySelector('.custom-audio-player.is-playing')) {
        console.log('HTMX refresh paused: player has is-playing state');
        event.preventDefault();
        return;
    }
    console.log('HTMX request starting:', event.detail.path);
});

document.body.addEventListener('htmx:afterRequest', function(event) {
    console.log('HTMX request completed:', event.detail.path);
});

document.body.addEventListener('htmx:afterSwap', function(event) {
    initCustomAudioPlayers(document);
});

document.body.addEventListener('htmx:responseError', function(event) {
    console.error('HTMX error:', event.detail);
    showNotification('Error loading data. Please refresh the page.', 'danger');
});

/**
 * Format seconds to MM:SS string
 */
function formatAudioTime(seconds) {
    if (isNaN(seconds) || seconds < 0) return "0:00";
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs < 10 ? '0' : ''}${secs}`;
}

/**
 * Attach audio element event listeners for timeupdate, play, pause, ended, loadedmetadata
 */
function hookAudioEvents(player, audio, playBtn) {
    if (!player || !audio || !playBtn || audio._eventsHooked) return;
    audio._eventsHooked = true;

    const scrubber = player.querySelector('.audio-scrubber');
    const currentTimeSpan = player.querySelector('.audio-current-time');
    const totalTimeSpan = player.querySelector('.audio-total-time');

    audio.addEventListener('play', function() {
        playBtn.innerHTML = '<i class="bi bi-pause-fill" style="font-size: 1.1rem;"></i>';
        playBtn.classList.remove('btn-primary');
        playBtn.classList.add('btn-warning');
        player.classList.add('is-playing');
    });

    audio.addEventListener('pause', function() {
        playBtn.innerHTML = '<i class="bi bi-play-fill" style="font-size: 1.1rem; margin-left: 2px;"></i>';
        playBtn.classList.remove('btn-warning');
        playBtn.classList.add('btn-primary');
        player.classList.remove('is-playing');
    });

    audio.addEventListener('ended', function() {
        playBtn.innerHTML = '<i class="bi bi-play-fill" style="font-size: 1.1rem; margin-left: 2px;"></i>';
        playBtn.classList.remove('btn-warning');
        playBtn.classList.add('btn-primary');
        player.classList.remove('is-playing');
        if (scrubber) scrubber.value = 0;
        if (currentTimeSpan) currentTimeSpan.textContent = "0:00";
    });

    audio.addEventListener('timeupdate', function() {
        const cur = audio.currentTime;
        const dur = (audio.duration && !isNaN(audio.duration) && audio.duration > 0)
            ? audio.duration
            : parseFloat(player.dataset.duration || 0);

        if (dur > 0) {
            if (scrubber) scrubber.value = (cur / dur) * 100;
            if (currentTimeSpan) currentTimeSpan.textContent = formatAudioTime(cur);
            if (totalTimeSpan) totalTimeSpan.textContent = formatAudioTime(dur);
        } else {
            if (currentTimeSpan) currentTimeSpan.textContent = formatAudioTime(cur);
        }
    });

    audio.addEventListener('loadedmetadata', function() {
        if (audio.duration && !isNaN(audio.duration) && totalTimeSpan) {
            totalTimeSpan.textContent = formatAudioTime(audio.duration);
        }
    });
}

/**
 * Initialize all Custom Audio Players on the page
 */
function initCustomAudioPlayers(root = document) {
    const doc = (root && root.querySelectorAll) ? root : document;
    const players = doc.querySelectorAll('.custom-audio-player');
    players.forEach(player => {
        const audio = player.querySelector('audio');
        const playBtn = player.querySelector('.audio-play-btn');
        if (audio && playBtn) {
            hookAudioEvents(player, audio, playBtn);
        }
    });
}

// Global Delegated Play / Pause Click Handler
document.addEventListener('click', function(e) {
    const playBtn = e.target.closest('.audio-play-btn');
    if (!playBtn) return;
    e.preventDefault();
    e.stopPropagation();

    const player = playBtn.closest('.custom-audio-player');
    if (!player) return;
    const audio = player.querySelector('audio');
    if (!audio) return;

    hookAudioEvents(player, audio, playBtn);

    if (audio.paused) {
        // Pause any other active audio player on the page
        document.querySelectorAll('.custom-audio-player audio').forEach(otherAudio => {
            if (otherAudio !== audio && !otherAudio.paused) {
                otherAudio.pause();
            }
        });
        audio.play().catch(err => console.error('Audio playback failed:', err));
    } else {
        audio.pause();
    }
});

// Global Delegated Scrubber Handler
function handleScrubberInput(e) {
    const scrubber = e.target.closest('.audio-scrubber');
    if (!scrubber) return;
    const player = scrubber.closest('.custom-audio-player');
    if (!player) return;
    const audio = player.querySelector('audio');
    if (!audio) return;
    const currentTimeSpan = player.querySelector('.audio-current-time');

    let dur = (audio.duration && !isNaN(audio.duration) && audio.duration > 0)
        ? audio.duration
        : parseFloat(player.dataset.duration || 0);

    if (dur > 0) {
        const targetTime = (parseFloat(scrubber.value) / 100) * dur;
        audio.currentTime = targetTime;
        if (currentTimeSpan) currentTimeSpan.textContent = formatAudioTime(targetTime);
    }
}
document.addEventListener('input', handleScrubberInput);
document.addEventListener('change', handleScrubberInput);

// Run initialization immediately if DOM is already ready
if (document.readyState === 'interactive' || document.readyState === 'complete') {
    initCustomAudioPlayers(document);
}

// Utility Functions

/**
 * Show a temporary notification
 */
function showNotification(message, type = 'info') {
    const container = document.querySelector('.container-fluid') || document.querySelector('.container');
    if (!container) return;
    
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    container.insertBefore(alertDiv, container.firstChild);
    
    setTimeout(() => {
        const bsAlert = new bootstrap.Alert(alertDiv);
        bsAlert.close();
    }, 5000);
}

/**
 * Copy text to clipboard
 */
function copyToClipboard(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text)
            .then(() => {
                showNotification('Copied to clipboard!', 'success');
            })
            .catch(err => {
                console.error('Failed to copy:', err);
                fallbackCopyToClipboard(text);
            });
    } else {
        fallbackCopyToClipboard(text);
    }
}

/**
 * Fallback copy method for older browsers
 */
function fallbackCopyToClipboard(text) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.top = '0';
    textArea.style.left = '0';
    textArea.style.opacity = '0';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        const successful = document.execCommand('copy');
        if (successful) {
            showNotification('Copied to clipboard!', 'success');
        } else {
            showNotification('Failed to copy to clipboard', 'danger');
        }
    } catch (err) {
        console.error('Fallback copy failed:', err);
        showNotification('Failed to copy to clipboard', 'danger');
    }
    
    document.body.removeChild(textArea);
}

/**
 * Format duration in seconds to human readable
 */
function formatDuration(seconds) {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    const parts = [];
    if (hours > 0) parts.push(`${hours}h`);
    if (minutes > 0) parts.push(`${minutes}m`);
    if (secs > 0 || parts.length === 0) parts.push(`${secs}s`);
    
    return parts.join(' ');
}

/**
 * Format timestamp to relative time
 */
function formatRelativeTime(timestamp) {
    const now = Date.now();
    const diff = now - timestamp;
    const seconds = Math.floor(diff / 1000);
    
    if (seconds < 60) return 'just now';
    if (seconds < 3600) return `${Math.floor(seconds / 60)} minutes ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)} hours ago`;
    return `${Math.floor(seconds / 86400)} days ago`;
}

/**
 * Confirm action with custom message
 */
function confirmAction(message, callback) {
    if (confirm(message)) {
        callback();
    }
}

/**
 * Handle form submission with loading state
 */
function handleFormSubmit(formElement, onSuccess) {
    const submitBtn = formElement.querySelector('[type="submit"]');
    const originalText = submitBtn.innerHTML;
    
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Processing...';
    
    const formData = new FormData(formElement);
    
    fetch(formElement.action, {
        method: formElement.method || 'POST',
        body: formData
    })
    .then(response => {
        if (!response.ok) throw new Error('Request failed');
        return response.json();
    })
    .then(data => {
        submitBtn.disabled = false;
        submitBtn.innerHTML = originalText;
        if (onSuccess) onSuccess(data);
    })
    .catch(error => {
        console.error('Form submission error:', error);
        submitBtn.disabled = false;
        submitBtn.innerHTML = originalText;
        showNotification('An error occurred. Please try again.', 'danger');
    });
}

// Ctrl/Cmd+K global search shortcut
document.addEventListener('keydown', function(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault();
        const input = document.getElementById('global-search-input');
        if (input) {
            input.focus();
            input.select();
        }
    }
});

// Copy-to-clipboard buttons
document.addEventListener('click', function(e) {
    const button = e.target.closest('.copy-btn');
    if (!button) return;

    const value = button.dataset.copy || '';
    if (!value) return;

    navigator.clipboard.writeText(value).then(() => {
        const icon = button.querySelector('i');
        if (icon) {
            const original = icon.className;
            icon.className = 'bi bi-clipboard-check';
            setTimeout(() => { icon.className = original; }, 1200);
        }
        const message = button.dataset.copySuccess || 'Copied to clipboard!';
        if (window.showNotification) {
            showNotification(message, 'success');
        }
    }).catch(() => {
        if (window.showNotification) {
            showNotification('Failed to copy to clipboard', 'danger');
        }
    });
});

/**
 * Dashboard auto-refresh controller shared by overview / rooms pages.
 */
const DashboardAutoRefresh = (() => {
    const STORAGE_ENABLED = 'dashboard_auto_refresh_enabled';
    const STORAGE_INTERVAL = 'dashboard_auto_refresh_interval';
    let timerId = null;

    function getToggle() {
        return document.getElementById('auto-refresh-toggle');
    }

    function getIntervalSelect() {
        return document.getElementById('refresh-interval-select');
    }

    function getIntervalSeconds() {
        const select = getIntervalSelect();
        const raw = select ? parseInt(select.value, 10) : parseInt(localStorage.getItem(STORAGE_INTERVAL) || '30', 10);
        return Number.isFinite(raw) && raw > 0 ? raw : 30;
    }

    function hasPoller() {
        return Boolean(document.getElementById('rooms-auto-poller'));
    }

    function syncPoller() {
        const poller = document.getElementById('rooms-auto-poller');
        if (!poller || !window.htmx) return;

        if (isEnabled()) {
            const interval = getIntervalSeconds();
            const url = new URL(window.location.href);
            url.searchParams.set('partial', '1');
            poller.setAttribute('hx-get', url.pathname + url.search);
            poller.setAttribute('hx-target', '#rooms-list');
            poller.setAttribute('hx-swap', 'outerHTML');
            poller.setAttribute('hx-trigger', `every ${interval}s`);
        } else {
            poller.removeAttribute('hx-get');
            poller.removeAttribute('hx-target');
            poller.removeAttribute('hx-swap');
            poller.removeAttribute('hx-trigger');
        }
        htmx.process(poller);
    }

    function stopTimer() {
        if (timerId) {
            clearInterval(timerId);
            timerId = null;
        }
    }

    function startTimer() {
        stopTimer();
        if (!isEnabled() || hasPoller()) {
            syncPoller();
            return;
        }

        const interval = getIntervalSeconds();
        timerId = setInterval(() => {
            window.location.reload();
        }, interval * 1000);
    }

    function isEnabled() {
        return localStorage.getItem(STORAGE_ENABLED) === '1';
    }

    function setEnabled(enabled) {
        localStorage.setItem(STORAGE_ENABLED, enabled ? '1' : '0');
        const toggle = getToggle();
        if (toggle) toggle.checked = enabled;
        if (enabled) {
            startTimer();
        } else {
            stopTimer();
            syncPoller();
        }
    }

    function setIntervalSeconds(seconds) {
        const safe = Number.isFinite(seconds) && seconds > 0 ? seconds : 30;
        localStorage.setItem(STORAGE_INTERVAL, String(safe));
        const select = getIntervalSelect();
        if (select) select.value = String(safe);
        if (isEnabled()) {
            startTimer();
        }
    }

    function init() {
        const toggle = getToggle();
        const select = getIntervalSelect();
        const enabled = isEnabled();
        const interval = getIntervalSeconds();

        if (toggle) toggle.checked = enabled;
        if (select) select.value = String(interval);

        if (enabled) {
            startTimer();
        } else {
            syncPoller();
        }
    }

    return {
        init,
        toggle: setEnabled,
        setInterval: setIntervalSeconds,
        syncPoller,
    };
})();

window.DashboardAutoRefresh = DashboardAutoRefresh;

/**
 * Theme controller — cycles dark → light → system, persists to localStorage.
 */
const DashboardTheme = (() => {
    const KEY = 'dashboard_theme';
    const ICONS = { dark: 'bi-moon-stars', light: 'bi-sun', system: 'bi-circle-half' };
    const CYCLE = { dark: 'light', light: 'system', system: 'dark' };

    function current() {
        return localStorage.getItem(KEY) || 'dark';
    }

    function apply(theme) {
        let resolved = theme;
        if (theme === 'system') {
            resolved = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
        }
        document.documentElement.setAttribute('data-bs-theme', resolved);
        const icon = document.getElementById('theme-icon');
        if (icon) {
            icon.className = 'bi ' + (ICONS[theme] || ICONS.dark);
        }
    }

    function cycle() {
        const next = CYCLE[current()] || 'dark';
        localStorage.setItem(KEY, next);
        apply(next);
    }

    function init() {
        apply(current());
    }

    return { cycle, init };
})();

window.DashboardTheme = DashboardTheme;

/**
 * Mobile Navigation Drawer controller
 */
const DashboardNavigation = (() => {
    function init() {
        const sidebar = document.getElementById('sidebar');
        const toggleBtn = document.getElementById('sidebar-toggle');
        const closeBtn = document.getElementById('sidebar-close');
        const backdrop = document.getElementById('sidebar-backdrop');

        if (!sidebar) return;

        function openDrawer() {
            sidebar.classList.add('show');
            if (backdrop) backdrop.classList.add('show');
            document.body.classList.add('sidebar-open');
            if (toggleBtn) toggleBtn.setAttribute('aria-expanded', 'true');
        }

        function closeDrawer() {
            sidebar.classList.remove('show');
            if (backdrop) backdrop.classList.remove('show');
            document.body.classList.remove('sidebar-open');
            if (toggleBtn) toggleBtn.setAttribute('aria-expanded', 'false');
        }

        if (toggleBtn) {
            toggleBtn.addEventListener('click', function(e) {
                e.stopPropagation();
                if (sidebar.classList.contains('show')) {
                    closeDrawer();
                } else {
                    openDrawer();
                }
            });
        }

        if (closeBtn) {
            closeBtn.addEventListener('click', function(e) {
                e.stopPropagation();
                closeDrawer();
            });
        }

        if (backdrop) {
            backdrop.addEventListener('click', function(e) {
                e.stopPropagation();
                closeDrawer();
            });
        }

        // Close on Escape key
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && sidebar.classList.contains('show')) {
                closeDrawer();
            }
        });

        // Close drawer when clicking a nav link on mobile
        const navLinks = sidebar.querySelectorAll('.nav-link');
        navLinks.forEach(link => {
            link.addEventListener('click', function() {
                if (window.innerWidth < 992) {
                    closeDrawer();
                }
            });
        });

        // Auto-close drawer if window resized to desktop size
        window.addEventListener('resize', function() {
            if (window.innerWidth >= 992 && sidebar.classList.contains('show')) {
                closeDrawer();
            }
        });
    }

    return { init };
})();

window.DashboardNavigation = DashboardNavigation;

document.addEventListener('DOMContentLoaded', function () {
    if (window.DashboardTheme) DashboardTheme.init();
    if (window.DashboardNavigation) DashboardNavigation.init();
    initCustomAudioPlayers(document);
});


