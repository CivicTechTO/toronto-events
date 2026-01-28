/**
 * Domain Classification Validator
 * Browser-based UI for validating domain classifications
 */

class DomainValidator {
    constructor() {
        this.domains = [];
        this.validations = {};
        this.currentTab = 'confirmed';
        this.selectedIndex = 0;
        this.filteredDomains = [];

        this.init();
    }

    async init() {
        this.loadValidations();
        await this.loadDomains();
        this.setupEventListeners();
        this.render();
    }

    async loadDomains() {
        try {
            const response = await fetch('domains.json');
            if (!response.ok) {
                throw new Error('domains.json not found. Run prepare_validation_data.py first.');
            }
            this.domains = await response.json();
        } catch (error) {
            console.error('Failed to load domains:', error);
            this.domains = [];
            this.showError(error.message);
        }
    }

    loadValidations() {
        const saved = localStorage.getItem('domain-validations');
        if (saved) {
            this.validations = JSON.parse(saved);
            this.updateSaveStatus(true);
        }
    }

    saveValidations() {
        localStorage.setItem('domain-validations', JSON.stringify(this.validations));
        this.updateSaveStatus(true);
    }

    updateSaveStatus(saved) {
        const status = document.getElementById('saveStatus');
        if (saved) {
            status.textContent = '✓ Saved';
            status.style.color = 'var(--accent-green)';
        } else {
            status.textContent = '● Unsaved';
            status.style.color = 'var(--accent-yellow)';
        }
    }

    setupEventListeners() {
        // Tab navigation
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                this.currentTab = btn.dataset.tab;
                this.selectedIndex = 0;
                this.updateTabs();
                this.render();
            });
        });

        // Export button
        document.getElementById('exportBtn').addEventListener('click', () => {
            this.exportValidations();
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

            const domain = this.filteredDomains[this.selectedIndex];
            if (!domain && ['a', 'r', 'u'].includes(e.key.toLowerCase())) return;

            switch (e.key.toLowerCase()) {
                case 'a':
                    this.setValidation(domain.domain, 'accept');
                    break;
                case 'r':
                    this.setValidation(domain.domain, 'reject');
                    break;
                case 'u':
                    this.setValidation(domain.domain, 'uncertain');
                    break;
                case 'n':
                    this.selectNext();
                    break;
                case 'p':
                    this.selectPrevious();
                    break;
            }
        });
    }

    setValidation(domain, status) {
        this.validations[domain] = {
            status: status,
            originalClassification: this.domains.find(d => d.domain === domain)?.classification,
            timestamp: new Date().toISOString()
        };
        this.saveValidations();
        this.render();
    }

    selectNext() {
        if (this.selectedIndex < this.filteredDomains.length - 1) {
            this.selectedIndex++;
            this.render();
            this.scrollToSelected();
        }
    }

    selectPrevious() {
        if (this.selectedIndex > 0) {
            this.selectedIndex--;
            this.render();
            this.scrollToSelected();
        }
    }

    scrollToSelected() {
        const selected = document.querySelector('.domain-card.selected');
        if (selected) {
            selected.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }

    getFilteredDomains() {
        return this.domains.filter(d => d.classification === this.currentTab);
    }

    updateTabs() {
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.tab === this.currentTab);
        });
    }

    updateBadges() {
        const classifications = ['confirmed', 'likely', 'possible', 'review'];
        classifications.forEach(cls => {
            const count = this.domains.filter(d => d.classification === cls).length;
            const validated = this.domains.filter(d =>
                d.classification === cls && this.validations[d.domain]
            ).length;

            const badge = document.getElementById(`badge-${cls}`);
            badge.textContent = `${validated}/${count}`;
        });
    }

    updateProgress() {
        const total = this.filteredDomains.length;
        const validated = this.filteredDomains.filter(d => this.validations[d.domain]).length;
        const percentage = total > 0 ? (validated / total) * 100 : 0;

        document.getElementById('progressFill').style.width = `${percentage}%`;
        document.getElementById('progressText').textContent = `${validated} / ${total} validated`;
    }

    getConfidenceClass(score) {
        if (score >= 0.6) return 'confidence-high';
        if (score >= 0.3) return 'confidence-medium';
        return 'confidence-low';
    }

    renderEventsList(events) {
        if (!events || events.length === 0) {
            return '<p class="no-events">No events data available</p>';
        }

        return events.map(event => {
            const dateStr = event.start_date ?
                `<span class="event-date">${event.start_date}</span>` : '';
            const locationStr = event.location ?
                `<span class="event-location">📍 ${event.location}</span>` : '';

            return `
                <div class="event-item">
                    <a href="${event.url}" target="_blank" rel="noopener noreferrer" class="event-name">
                        ${event.name || 'Untitled Event'}
                    </a>
                    <div class="event-meta">
                        ${dateStr}${locationStr}
                    </div>
                </div>
            `;
        }).join('');
    }

    renderDomainCard(domain, index) {
        const validation = this.validations[domain.domain];
        const validationClass = validation ? `validated-${validation.status}` : '';
        const selectedClass = index === this.selectedIndex ? 'selected' : '';

        const matchReasons = (domain.match_reasons || '').split('|').filter(r => r.trim());
        const reasonTags = matchReasons.map(r =>
            `<span class="reason-tag">${r.trim()}</span>`
        ).join('');

        const eventCount = domain.events?.length || 0;
        const eventsHtml = this.renderEventsList(domain.events);

        return `
            <article class="domain-card ${validationClass} ${selectedClass}" 
                     data-domain="${domain.domain}">
                <div class="domain-header" onclick="validator.selectDomain(${index})">
                    <a class="domain-name" href="https://${domain.domain}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation();">${domain.domain}</a>
                    <span class="confidence-score ${this.getConfidenceClass(domain.confidence_score)}">
                        ${(domain.confidence_score * 100).toFixed(0)}%
                    </span>
                </div>
                
                <div class="domain-stats">
                    <span class="stat">Events: <span class="stat-value">${domain.gta_events || 0}/${domain.total_events || 0}</span></span>
                    <span class="stat">Postal: <span class="stat-value">${domain.postal_matches || 0}</span></span>
                    <span class="stat">Coord: <span class="stat-value">${domain.coord_matches || 0}</span></span>
                    <span class="stat">Locality: <span class="stat-value">${domain.locality_matches || 0}</span></span>
                </div>
                
                ${matchReasons.length > 0 ? `<div class="match-reasons">${reasonTags}</div>` : ''}
                
                <button class="expand-events-btn" onclick="event.stopPropagation(); validator.toggleEvents('${domain.domain}')">
                    <span class="expand-icon" id="expand-icon-${domain.domain}">▶</span>
                    Show ${eventCount} events
                </button>
                
                <div class="events-panel" id="events-${domain.domain}" style="display: none;">
                    ${eventsHtml}
                </div>
                
                <div class="validation-actions">
                    <button class="action-btn accept ${validation?.status === 'accept' ? 'active' : ''}"
                            onclick="event.stopPropagation(); validator.setValidation('${domain.domain}', 'accept')">
                        ✓ Accept
                    </button>
                    <button class="action-btn reject ${validation?.status === 'reject' ? 'active' : ''}"
                            onclick="event.stopPropagation(); validator.setValidation('${domain.domain}', 'reject')">
                        ✗ Reject
                    </button>
                    <button class="action-btn uncertain ${validation?.status === 'uncertain' ? 'active' : ''}"
                            onclick="event.stopPropagation(); validator.setValidation('${domain.domain}', 'uncertain')">
                        ? Uncertain
                    </button>
                </div>
            </article>
        `;
    }

    toggleEvents(domain) {
        const panel = document.getElementById(`events-${domain}`);
        const icon = document.getElementById(`expand-icon-${domain}`);
        if (panel.style.display === 'none') {
            panel.style.display = 'block';
            icon.textContent = '▼';
        } else {
            panel.style.display = 'none';
            icon.textContent = '▶';
        }
    }

    selectDomain(index) {
        this.selectedIndex = index;
        this.render();
    }

    showError(message) {
        const list = document.getElementById('domainList');
        list.innerHTML = `
            <div class="empty-state">
                <h2>Error Loading Data</h2>
                <p>${message}</p>
            </div>
        `;
    }

    render() {
        this.filteredDomains = this.getFilteredDomains();
        this.updateBadges();
        this.updateProgress();

        const list = document.getElementById('domainList');

        if (this.filteredDomains.length === 0) {
            list.innerHTML = `
                <div class="empty-state">
                    <h2>No domains in this category</h2>
                    <p>Try another tab or run the scoring pipeline.</p>
                </div>
            `;
            return;
        }

        list.innerHTML = this.filteredDomains
            .map((domain, index) => this.renderDomainCard(domain, index))
            .join('');
    }

    exportValidations() {
        const exportData = {
            exported_at: new Date().toISOString(),
            total_validations: Object.keys(this.validations).length,
            validations: this.validations
        };

        const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);

        const a = document.createElement('a');
        a.href = url;
        a.download = 'validations.json';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }
}

// Initialize on page load
const validator = new DomainValidator();
