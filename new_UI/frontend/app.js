/**
 * ShortList - Minimal Frontend Application
 * Clean, vanilla JavaScript with no framework dependencies.
 */

const API_BASE = 'http://localhost:5002/api';
const INTERVIEW_WS_BASE = 'ws://localhost:8001';

// State
const state = {
    user: null,
    token: localStorage.getItem('shortlist_token'),
    currentPage: 'loading',
    previousPage: null,   // Track where we came from for back navigation
    roles: [],
    recommendations: [],  // Semantic-matched job recommendations
    recommendationsLoaded: false,
    forYouJobs: [],       // Personalized jobs for "For You" page
    forYouLoaded: false,
    myApplications: [],
    selectedRole: null,
    filters: {
        search: '',
        role_type: '',
        experience_level: '',
        location: '',
        work_arrangement: ''
    },
    // Employer state
    employerRoles: [],
    selectedEmployerRole: null,
    applicants: [],
    hiddenApplicantsCount: 0,
    totalApplicantsCount: 0,
    showHiddenApplicants: false,
    selectedApplicantDetail: null,
    drawerOpen: false,
    employerFilters: {
        seniority: [],
        minScore: 70
    },
    // Signup state
    signupAsEmployer: false,
    // Profile state
    profilePreferences: null
};

// API Helper
async function api(endpoint, options = {}) {
    const headers = {
        'Content-Type': 'application/json',
        ...(state.token ? { 'Authorization': `Bearer ${state.token}` } : {})
    };

    const url = `${API_BASE}${endpoint}`;
    console.log('API Request:', url, options.method || 'GET');

    try {
        const response = await fetch(url, {
            ...options,
            headers: { ...headers, ...options.headers },
            mode: 'cors'
        });

        console.log('API Response status:', response.status);
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Request failed');
        }

        return data;
    } catch (err) {
        console.error('API Error for', url, ':', err);
        throw err;
    }
}

// Auth Functions
async function checkAuth() {
    // Always start at home page
    if (!state.token) {
        navigate('home');
        return;
    }

    // Validate token silently and load user data
    try {
        const data = await api('/auth/me');
        state.user = data.user;
        // Stay on home page - user can navigate from there
        navigate('home');
    } catch (err) {
        // Invalid token - clear it and show home page
        localStorage.removeItem('shortlist_token');
        state.token = null;
        state.user = null;
        navigate('home');
    }
}

async function signup(fullName, email, password, userType = 'seeker', company = null) {
    const payload = { full_name: fullName, email, password, user_type: userType };
    if (company) {
        payload.company = company;
    }

    const data = await api('/auth/signup', {
        method: 'POST',
        body: JSON.stringify(payload)
    });

    state.token = data.token;
    state.user = data.user;
    localStorage.setItem('shortlist_token', data.token);

    // Route based on user type
    if (userType === 'employer') {
        navigate('employer');
    } else {
        navigate('setup');
    }
}

async function login(email, password) {
    const data = await api('/auth/login', {
        method: 'POST',
        body: JSON.stringify({ email, password })
    });

    state.token = data.token;
    state.user = data.user;
    localStorage.setItem('shortlist_token', data.token);

    // Check profile status
    const me = await api('/auth/me');
    state.user = me.user;

    if (!state.user.profile_complete) {
        navigate('setup');
    } else if (state.user.has_resume) {
        // User has resume - go to For You page
        navigate('for-you');
    } else {
        // No resume - go to Explore page
        navigate('explore');
    }
}

function logout() {
    localStorage.removeItem('shortlist_token');
    state.token = null;
    state.user = null;
    navigate('home');
}

async function savePreferences(preferences) {
    await api('/profile/preferences', {
        method: 'PUT',
        body: JSON.stringify(preferences)
    });

    state.user.preferences = preferences;
    // Go to resume upload step (profile_complete will be set after resume or skip)
    navigate('resume-upload');
}

async function loadPreferences() {
    try {
        const data = await api('/profile/preferences');
        return data.preferences || {};
    } catch (err) {
        console.error('Failed to load preferences:', err);
        return {};
    }
}

// Roles Functions
async function loadRoles() {
    try {
        const params = new URLSearchParams();
        if (state.filters.search) params.append('search', state.filters.search);
        if (state.filters.role_type) params.append('role_type', state.filters.role_type);
        if (state.filters.experience_level) params.append('experience_level', state.filters.experience_level);
        if (state.filters.location) params.append('location', state.filters.location);
        if (state.filters.work_arrangement) params.append('work_arrangement', state.filters.work_arrangement);

        const data = await api(`/roles?${params}`);
        state.roles = data.roles || [];
        renderRolesList();
    } catch (err) {
        console.error('Failed to load roles:', err);
        state.roles = [];
        renderRolesList();
    }
}

async function loadRecommendations() {
    // Only load if user is logged in and hasn't loaded recommendations yet
    if (!state.token || state.recommendationsLoaded) {
        return;
    }

    try {
        const data = await api('/recommendations?limit=6');
        state.recommendations = data.recommendations || [];
        state.recommendationsLoaded = true;
        renderRecommendations();
    } catch (err) {
        // User likely doesn't have a resume processed - that's OK
        console.log('No recommendations available:', err.message);
        state.recommendations = [];
        state.recommendationsLoaded = true;
        renderRecommendations();
    }
}

async function loadForYouJobs() {
    // Load personalized "For You" jobs with 75% resume / 25% preferences weighting
    if (!state.token) {
        return;
    }

    try {
        const data = await api('/for-you?limit=100&min_score=60');
        state.forYouJobs = data.jobs || [];
        state.forYouLoaded = true;
        renderForYouList();
    } catch (err) {
        console.log('Could not load For You jobs:', err.message);
        state.forYouJobs = [];
        state.forYouLoaded = true;
        // If user needs resume, show message
        if (err.message.includes('resume')) {
            renderForYouNeedsResume();
        } else {
            renderForYouList();
        }
    }
}

function renderRecommendations() {
    const container = document.getElementById('recommendations-section');
    if (!container) return;

    // Don't show if no recommendations
    if (!state.recommendations.length) {
        container.innerHTML = '';
        return;
    }

    container.innerHTML = `
        <div class="recommendations-header">
            <h2>Recommended for you</h2>
            <p>Based on your resume and profile</p>
        </div>
        <div class="recommendations-grid">
            ${state.recommendations.map(role => `
                <div class="recommendation-card" data-role-id="${role.id}">
                    <div class="recommendation-match">
                        <span class="match-percent">${Math.round(role.match_score)}%</span>
                        <span class="match-label">match</span>
                    </div>
                    <div class="recommendation-content">
                        <h3 class="recommendation-title">${escapeHtml(role.title)}</h3>
                        <div class="recommendation-company">${escapeHtml(role.company_name)}</div>
                        ${role.match_reason ? `<div class="recommendation-reason">${escapeHtml(role.match_reason)}</div>` : ''}
                        <div class="recommendation-meta">
                            <span>${escapeHtml(role.location || 'Boston Area')}</span>
                            ${role.salary_range ? `<span class="salary">${escapeHtml(role.salary_range)}</span>` : ''}
                        </div>
                    </div>
                </div>
            `).join('')}
        </div>
    `;

    // Add click listeners
    container.querySelectorAll('.recommendation-card').forEach(card => {
        card.addEventListener('click', () => {
            const roleId = card.dataset.roleId;
            loadRole(roleId);
            state.currentPage = 'role';
        });
    });
}

async function loadRole(roleId, matchScoreOverride = null) {
    // Load role data and user's applications in parallel
    const [roleData, appsData] = await Promise.all([
        api(`/roles/${roleId}`),
        state.token ? api('/shortlist/my-applications').catch(() => ({ applications: [] })) : Promise.resolve({ applications: [] })
    ]);

    state.selectedRole = roleData.role;
    state.myApplications = appsData.applications || [];
    // Override match score if provided (from For You page), or null to hide (from Explore)
    state.selectedRole.match_score = matchScoreOverride;
    renderRoleDetail();
}

// Shortlist Functions
async function applyToShortlist(roleId) {
    try {
        // First, just get the questions and role info without creating an application
        const data = await api(`/shortlist/prepare/${roleId}`);

        // Show the comprehensive 3-step application flow (application created on submit)
        showApplicationFlow(roleId, data.questions || [], data.has_resume, data.role_type || 'other');
    } catch (err) {
        if (err.message.includes('INCOMPLETE_PROFILE')) {
            navigate('setup');
        } else if (err.message.includes('Already on this shortlist')) {
            alert('You have already applied to this role.');
        } else {
            alert(err.message);
        }
    }
}

// 4-Step Application Flow Modal
function showApplicationFlow(roleId, fitQuestions, hasResume, roleType) {
    const modal = document.createElement('div');
    modal.className = 'modal-overlay application-flow-overlay';

    // Application ID will be set when we actually submit
    let applicationId = null;

    // State management - start at step 2 if user already has resume
    let currentStep = hasResume ? 2 : 1;
    let resumeFile = null;
    let resumeUploaded = hasResume;
    const eligibilityData = {
        authorized_us: null,
        needs_sponsorship: null,
        hybrid_onsite: null,
        start_date: '',
        seniority_band: '',
        must_have_skills: [
            { checked: false, evidence: '' },
            { checked: false, evidence: '' }
        ],
        portfolio_link: ''
    };
    const fitResponses = {};

    // Interview permissions state
    let permissionsState = {
        camera: null,  // null = unchecked, true = granted, false = denied
        microphone: null
    };

    // Only show portfolio link for tech roles
    const isTechRole = ['software_engineer', 'data_scientist', 'data_analyst', 'engineering_manager', 'designer'].includes(roleType);

    // Determine context-aware link label (only for tech roles)
    const linkLabel = roleType === 'software_engineer' ? 'GitHub' :
                      roleType === 'data_scientist' ? 'GitHub / Kaggle' :
                      roleType === 'designer' ? 'Portfolio / Dribbble' : 'GitHub / Portfolio';

    const linkPlaceholder = roleType === 'software_engineer' ? 'https://github.com/username' :
                            roleType === 'data_scientist' ? 'https://github.com/username or kaggle.com/username' :
                            roleType === 'designer' ? 'https://dribbble.com/username' : 'https://github.com/username';

    // Must-have skills based on role type
    const mustHaveSkills = roleType === 'software_engineer' ?
        ['Proficiency in a core language (Python, JS, Java, etc.)', 'Experience with production systems'] :
        roleType === 'data_scientist' ?
        ['Statistical modeling / ML experience', 'SQL and data pipeline experience'] :
        roleType === 'sales' ?
        ['Quota-carrying experience', 'Full-cycle sales experience'] :
        ['Relevant domain expertise', 'Proven track record in similar role'];

    function render() {
        modal.innerHTML = `
            <div class="application-flow-modal">
                <!-- Pinned Progress Header -->
                <div class="flow-header">
                    <div class="flow-progress-bar">
                        <div class="flow-step ${currentStep >= 1 ? 'active' : ''} ${currentStep > 1 ? 'complete' : ''}" data-step="1">
                            <div class="flow-step-indicator">
                                ${currentStep > 1 ? '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="20 6 9 17 4 12"/></svg>' : '1'}
                            </div>
                            <span class="flow-step-label">Resume</span>
                        </div>
                        <div class="flow-step-connector ${currentStep > 1 ? 'complete' : ''}"></div>
                        <div class="flow-step ${currentStep >= 2 ? 'active' : ''} ${currentStep > 2 ? 'complete' : ''}" data-step="2">
                            <div class="flow-step-indicator">
                                ${currentStep > 2 ? '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="20 6 9 17 4 12"/></svg>' : '2'}
                            </div>
                            <span class="flow-step-label">Eligibility</span>
                        </div>
                        <div class="flow-step-connector ${currentStep > 2 ? 'complete' : ''}"></div>
                        <div class="flow-step ${currentStep >= 3 ? 'active' : ''} ${currentStep > 3 ? 'complete' : ''}" data-step="3">
                            <div class="flow-step-indicator">
                                ${currentStep > 3 ? '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="20 6 9 17 4 12"/></svg>' : '3'}
                            </div>
                            <span class="flow-step-label">Fit</span>
                        </div>
                        <div class="flow-step-connector ${currentStep > 3 ? 'complete' : ''}"></div>
                        <div class="flow-step ${currentStep >= 4 ? 'active' : ''}" data-step="4">
                            <div class="flow-step-indicator">4</div>
                            <span class="flow-step-label">Interview</span>
                        </div>
                    </div>
                    <button class="flow-close-btn" id="close-flow">
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="18" y1="6" x2="6" y2="18"></line>
                            <line x1="6" y1="6" x2="18" y2="18"></line>
                        </svg>
                    </button>
                </div>

                <!-- Step Content -->
                <div class="flow-content">
                    ${currentStep === 1 ? renderResumeStep() : ''}
                    ${currentStep === 2 ? renderEligibilityStep() : ''}
                    ${currentStep === 3 ? renderFitStep() : ''}
                    ${currentStep === 4 ? renderInterviewStep() : ''}
                </div>

                <!-- Footer -->
                <div class="flow-footer">
                    ${currentStep > 1 ? '<button class="btn btn-secondary" id="flow-back">Back</button>' : '<div></div>'}
                    ${renderFooterButton()}
                </div>
            </div>
        `;

        attachListeners();
    }

    function renderResumeStep() {
        return `
            <div class="flow-step-content step-resume">
                <div class="flow-section-header">
                    <h2>Upload your resume</h2>
                    <p>Your resume helps us match you with the right opportunities</p>
                </div>

                <div class="resume-upload-card">
                    ${resumeUploaded ? `
                        <div class="resume-uploaded-state">
                            <div class="resume-success-icon">
                                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
                                    <polyline points="22 4 12 14.01 9 11.01"></polyline>
                                </svg>
                            </div>
                            <h3>Resume on file</h3>
                            <p class="resume-filename">Your resume is already uploaded</p>
                            <button class="btn btn-secondary btn-sm" id="replace-resume">Upload a different resume</button>
                        </div>
                    ` : `
                        <div class="resume-dropzone" id="resume-dropzone">
                            <input type="file" id="resume-file-input" accept=".pdf,.doc,.docx" style="display: none;">
                            <div class="dropzone-content">
                                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                                    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                                    <polyline points="14 2 14 8 20 8"></polyline>
                                    <line x1="12" y1="18" x2="12" y2="12"></line>
                                    <line x1="9" y1="15" x2="15" y2="15"></line>
                                </svg>
                                <p class="dropzone-text">Drag and drop your resume here</p>
                                <p class="dropzone-subtext">or <span class="dropzone-browse">browse files</span></p>
                                <p class="dropzone-formats">PDF, DOC, DOCX up to 5MB</p>
                            </div>
                        </div>
                        ${resumeFile ? `
                            <div class="resume-selected">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                                    <polyline points="14 2 14 8 20 8"></polyline>
                                </svg>
                                <span class="resume-name">${resumeFile.name}</span>
                                <button class="resume-remove" id="remove-resume">
                                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                        <line x1="18" y1="6" x2="6" y2="18"></line>
                                        <line x1="6" y1="6" x2="18" y2="18"></line>
                                    </svg>
                                </button>
                            </div>
                        ` : ''}
                    `}
                </div>
            </div>
        `;
    }

    function renderEligibilityStep() {
        return `
            <div class="flow-step-content step-eligibility">
                <div class="flow-section-header">
                    <h2>Quick eligibility check</h2>
                    <p>Just a few questions to make sure this role is a good fit</p>
                </div>

                <!-- Authorization Card -->
                <div class="eligibility-card">
                    <div class="eligibility-question">
                        <span class="eligibility-label">Authorized to work in the US?</span>
                        <div class="pill-toggle" data-field="authorized_us">
                            <button class="pill-option ${eligibilityData.authorized_us === true ? 'selected' : ''}" data-value="true">Yes</button>
                            <button class="pill-option ${eligibilityData.authorized_us === false ? 'selected' : ''}" data-value="false">No</button>
                        </div>
                    </div>
                </div>

                <!-- Sponsorship Card -->
                <div class="eligibility-card">
                    <div class="eligibility-question">
                        <span class="eligibility-label">Need visa sponsorship now or in the future?</span>
                        <div class="pill-toggle" data-field="needs_sponsorship">
                            <button class="pill-option ${eligibilityData.needs_sponsorship === true ? 'selected' : ''}" data-value="true">Yes</button>
                            <button class="pill-option ${eligibilityData.needs_sponsorship === false ? 'selected' : ''}" data-value="false">No</button>
                        </div>
                    </div>
                </div>

                <!-- Hybrid/On-site Card -->
                <div class="eligibility-card">
                    <div class="eligibility-question">
                        <span class="eligibility-label">Open to hybrid or on-site in Boston?</span>
                        <div class="pill-toggle" data-field="hybrid_onsite">
                            <button class="pill-option ${eligibilityData.hybrid_onsite === true ? 'selected' : ''}" data-value="true">Yes</button>
                            <button class="pill-option ${eligibilityData.hybrid_onsite === false ? 'selected' : ''}" data-value="false">No</button>
                        </div>
                    </div>
                </div>

                <!-- Start Date Card -->
                <div class="eligibility-card">
                    <div class="eligibility-question vertical">
                        <span class="eligibility-label">When can you start?</span>
                        <select class="flow-select" data-field="start_date">
                            <option value="">Select start window...</option>
                            <option value="immediately" ${eligibilityData.start_date === 'immediately' ? 'selected' : ''}>Immediately</option>
                            <option value="2_weeks" ${eligibilityData.start_date === '2_weeks' ? 'selected' : ''}>2 weeks notice</option>
                            <option value="1_month" ${eligibilityData.start_date === '1_month' ? 'selected' : ''}>1 month</option>
                            <option value="2_months" ${eligibilityData.start_date === '2_months' ? 'selected' : ''}>2 months</option>
                            <option value="3_months" ${eligibilityData.start_date === '3_months' ? 'selected' : ''}>3+ months</option>
                        </select>
                    </div>
                </div>

                <!-- Seniority Card -->
                <div class="eligibility-card">
                    <div class="eligibility-question vertical">
                        <span class="eligibility-label">Years of relevant experience</span>
                        <div class="seniority-pills">
                            ${['0-1', '1-3', '3-5', '5+'].map(band => `
                                <button class="seniority-pill ${eligibilityData.seniority_band === band ? 'selected' : ''}" data-band="${band}">
                                    ${band} years
                                </button>
                            `).join('')}
                        </div>
                    </div>
                </div>

                <!-- Must-Have Skills Card -->
                <div class="eligibility-card skills-card">
                    <div class="eligibility-question vertical">
                        <span class="eligibility-label">Confirm must-have skills</span>
                        <p class="eligibility-helper">Check each skill you have and provide brief evidence</p>
                    </div>
                    <div class="must-have-skills">
                        ${mustHaveSkills.map((skill, i) => `
                            <div class="must-have-item ${eligibilityData.must_have_skills[i].checked ? 'checked' : ''}">
                                <label class="must-have-checkbox">
                                    <input type="checkbox" data-skill-index="${i}" ${eligibilityData.must_have_skills[i].checked ? 'checked' : ''}>
                                    <span class="checkbox-custom"></span>
                                    <span class="must-have-label">${skill}</span>
                                </label>
                                <div class="evidence-input ${eligibilityData.must_have_skills[i].checked ? 'visible' : ''}">
                                    <input type="text"
                                        placeholder="Project/company — what you did"
                                        data-evidence-index="${i}"
                                        value="${escapeHtml(eligibilityData.must_have_skills[i].evidence)}"
                                        maxlength="100">
                                    <span class="char-count">${eligibilityData.must_have_skills[i].evidence.length}/100</span>
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>

                ${isTechRole ? `
                <!-- Portfolio Link Card (Tech roles only) -->
                <div class="eligibility-card">
                    <div class="eligibility-question vertical">
                        <span class="eligibility-label">${linkLabel} <span class="optional-badge">Optional</span></span>
                        <input type="url"
                            class="flow-input"
                            placeholder="${linkPlaceholder}"
                            data-field="portfolio_link"
                            value="${escapeHtml(eligibilityData.portfolio_link)}">
                        <p class="eligibility-helper">Add a link to showcase your work</p>
                    </div>
                </div>
                ` : ''}
            </div>
        `;
    }

    function renderFitStep() {
        // Separate MC and free response questions
        const mcQuestions = fitQuestions.filter(q => q.question_type === 'multiple_choice');
        const frQuestions = fitQuestions.filter(q => q.question_type === 'free_response');

        return `
            <div class="flow-step-content step-fit">
                <div class="flow-section-header">
                    <h2>Culture & fit</h2>
                    <p>Help us match you with teams that share your working style</p>
                </div>

                ${mcQuestions.length > 0 ? `
                    <div class="fit-questions-grid">
                        ${mcQuestions.map((q, index) => `
                            <div class="fit-card" data-question-id="${q.id}">
                                <div class="fit-card-header">
                                    <span class="fit-card-number">${index + 1}</span>
                                    <p class="fit-card-question">${escapeHtml(q.question_text)}</p>
                                </div>
                                <div class="fit-card-options">
                                    ${q.options.map(opt => `
                                        <label class="fit-option-card ${fitResponses[q.id]?.response_value === opt.value ? 'selected' : ''}">
                                            <input type="radio" name="fit_${q.id}" value="${opt.value}" ${fitResponses[q.id]?.response_value === opt.value ? 'checked' : ''}>
                                            <span class="fit-option-letter">${opt.value}</span>
                                            <span class="fit-option-text">${escapeHtml(opt.label)}</span>
                                        </label>
                                    `).join('')}
                                </div>
                            </div>
                        `).join('')}
                    </div>
                ` : ''}

                ${frQuestions.length > 0 ? `
                    <div class="fit-freeform-section">
                        <div class="fit-section-divider">
                            <span>Short answers</span>
                        </div>
                        ${frQuestions.map((q, index) => `
                            <div class="fit-freeform-card" data-question-id="${q.id}">
                                <p class="fit-freeform-question">${escapeHtml(q.question_text)}</p>
                                <textarea
                                    class="fit-freeform-input"
                                    placeholder="Your answer..."
                                    rows="2"
                                    maxlength="300"
                                    data-question-id="${q.id}"
                                >${fitResponses[q.id]?.response_text || ''}</textarea>
                                <div class="fit-freeform-footer">
                                    <span class="char-count">${(fitResponses[q.id]?.response_text || '').length}/300</span>
                                </div>
                            </div>
                        `).join('')}
                    </div>
                ` : ''}

                ${fitQuestions.length === 0 ? `
                    <div class="no-fit-questions">
                        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                            <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
                            <polyline points="22 4 12 14.01 9 11.01"></polyline>
                        </svg>
                        <p>No additional questions for this role</p>
                    </div>
                ` : ''}
            </div>
        `;
    }

    function renderInterviewStep() {
        const cameraStatus = permissionsState.camera === null ? 'pending' :
                             permissionsState.camera === true ? 'ready' : 'denied';
        const micStatus = permissionsState.microphone === null ? 'pending' :
                          permissionsState.microphone === true ? 'ready' : 'denied';

        const getStatusText = (status) => {
            if (status === 'ready') return 'Ready';
            if (status === 'denied') return 'Denied';
            return 'Needs permission';
        };

        const allReady = permissionsState.camera === true && permissionsState.microphone === true;

        return `
            <div class="flow-step-content step-interview">
                <div class="flow-section-header">
                    <h2>AI Interview</h2>
                    <p>A short conversation to assess communication and role fit</p>
                </div>

                <!-- Interview Preview Card -->
                <div class="interview-preview-card">
                    <div class="interview-expectations">
                        <div class="expectation-item">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="12" cy="12" r="10"></circle>
                                <polyline points="12 6 12 12 16 14"></polyline>
                            </svg>
                            <span>10-15 min</span>
                        </div>
                        <div class="expectation-item">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
                            </svg>
                            <span>6-10 questions</span>
                        </div>
                        <div class="expectation-item">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3z"></path>
                                <path d="M19 10v2a7 7 0 0 1-14 0v-2"></path>
                                <line x1="12" y1="19" x2="12" y2="22"></line>
                            </svg>
                            <span>Voice responses</span>
                        </div>
                    </div>

                    <!-- Readiness Checklist -->
                    <div class="readiness-checklist">
                        <div class="readiness-item ${cameraStatus}">
                            <div class="readiness-icon">
                                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <polygon points="23 7 16 12 23 17 23 7"></polygon>
                                    <rect x="1" y="5" width="15" height="14" rx="2" ry="2"></rect>
                                </svg>
                            </div>
                            <span class="readiness-label">Camera</span>
                            <span class="readiness-status ${cameraStatus}" data-permission="camera">
                                <span class="status-dot"></span>
                                ${getStatusText(cameraStatus)}
                            </span>
                        </div>
                        <div class="readiness-item ${micStatus}">
                            <div class="readiness-icon">
                                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3z"></path>
                                    <path d="M19 10v2a7 7 0 0 1-14 0v-2"></path>
                                    <line x1="12" y1="19" x2="12" y2="22"></line>
                                </svg>
                            </div>
                            <span class="readiness-label">Microphone</span>
                            <span class="readiness-status ${micStatus}" data-permission="microphone">
                                <span class="status-dot"></span>
                                ${getStatusText(micStatus)}
                            </span>
                        </div>
                        <div class="readiness-item ready">
                            <div class="readiness-icon">
                                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M3 18v-6a9 9 0 0 1 18 0v6"></path>
                                    <path d="M21 19a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3zM3 19a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2H3z"></path>
                                </svg>
                            </div>
                            <span class="readiness-label">Quiet environment</span>
                            <span class="readiness-status ready">
                                <span class="status-dot"></span>
                                Ready
                            </span>
                        </div>
                    </div>

                    ${!allReady ? `
                        <button class="btn btn-secondary btn-full" id="check-permissions">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polygon points="23 7 16 12 23 17 23 7"></polygon>
                                <rect x="1" y="5" width="15" height="14" rx="2" ry="2"></rect>
                            </svg>
                            Enable Camera & Microphone
                        </button>
                    ` : ''}

                    <!-- Interview Timeline Preview -->
                    <div class="interview-timeline">
                        <div class="timeline-label">Interview outline</div>
                        <div class="timeline-chips">
                            <span class="timeline-chip">Rapport</span>
                            <span class="timeline-chip">Experience questions</span>
                            <span class="timeline-chip">${isTechRole ? 'Technical deep-dive' : 'Role scenarios'}</span>
                            <span class="timeline-chip">Wrap-up</span>
                        </div>
                    </div>

                    <p class="interview-note">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="12" cy="12" r="10"></circle>
                            <line x1="12" y1="16" x2="12" y2="12"></line>
                            <line x1="12" y1="8" x2="12.01" y2="8"></line>
                        </svg>
                        Your video is displayed for verification but not recorded or saved.
                    </p>
                </div>

                <!-- Defer Option -->
                <div class="defer-option">
                    <p class="defer-text">Not ready right now? You can still join the shortlist and complete the interview later.</p>
                    <button class="btn btn-link" id="defer-interview">Do this later</button>
                    <p class="defer-note">Candidates who complete the interview may be ranked higher</p>
                </div>
            </div>
        `;
    }

    function renderFooterButton() {
        if (currentStep === 1) {
            // Resume step
            const isResumeValid = validateResumeStep();
            return `<button class="btn btn-primary" id="flow-next" ${!isResumeValid ? 'disabled' : ''}>Next</button>`;
        } else if (currentStep === 2) {
            // Eligibility step
            const isEligibilityValid = validateEligibilityStep();
            return `<button class="btn btn-primary" id="flow-next" ${!isEligibilityValid ? 'disabled' : ''}>
                ${fitQuestions.length > 0 ? 'Next' : 'Continue'}
            </button>`;
        } else if (currentStep === 3) {
            // Fit step
            const isFitValid = validateFitStep();
            return `<button class="btn btn-primary" id="flow-next" ${!isFitValid ? 'disabled' : ''}>Continue</button>`;
        } else {
            // Interview step
            const allReady = permissionsState.camera === true && permissionsState.microphone === true;
            return `<button class="btn btn-primary" id="start-interview" ${!allReady ? 'disabled' : ''}>
                ${allReady ? 'Start Interview' : 'Enable permissions to start'}
            </button>`;
        }
    }

    function validateResumeStep() {
        return resumeUploaded || resumeFile !== null;
    }

    function validateEligibilityStep() {
        return eligibilityData.authorized_us !== null &&
               eligibilityData.needs_sponsorship !== null &&
               eligibilityData.hybrid_onsite !== null &&
               eligibilityData.start_date !== '' &&
               eligibilityData.seniority_band !== '';
    }

    function validateFitStep() {
        if (fitQuestions.length === 0) return true;
        return Object.keys(fitResponses).length === fitQuestions.length;
    }

    function updateNextButtonState() {
        const nextBtn = modal.querySelector('#flow-next');
        if (nextBtn) {
            if (currentStep === 1) {
                nextBtn.disabled = !validateResumeStep();
            } else if (currentStep === 2) {
                nextBtn.disabled = !validateEligibilityStep();
            } else if (currentStep === 3) {
                nextBtn.disabled = !validateFitStep();
            }
        }
    }

    function attachListeners() {
        // Close button - works immediately without confirm on click
        modal.querySelector('#close-flow')?.addEventListener('click', () => {
            modal.remove();
        });

        // Also close on overlay click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });

        // Close on Escape key
        const escapeHandler = (e) => {
            if (e.key === 'Escape') {
                modal.remove();
                document.removeEventListener('keydown', escapeHandler);
            }
        };
        document.addEventListener('keydown', escapeHandler);

        // Back button
        modal.querySelector('#flow-back')?.addEventListener('click', () => {
            currentStep--;
            render();
        });

        // Next button
        const nextBtn = modal.querySelector('#flow-next');
        console.log('Setting up next button listener, found:', !!nextBtn, 'currentStep:', currentStep);
        if (nextBtn) {
            nextBtn.addEventListener('click', async (e) => {
                e.preventDefault();
                console.log('Next button clicked! currentStep:', currentStep);
                if (currentStep === 1) {
                    // Resume step - upload if needed then go to step 2
                    if (resumeFile && !resumeUploaded) {
                        nextBtn.disabled = true;
                        nextBtn.textContent = 'Uploading...';
                        try {
                            await uploadProfileResume(resumeFile);
                            resumeUploaded = true;
                            currentStep = 2;
                            render();
                        } catch (err) {
                            console.error('Failed to upload resume:', err);
                            alert('Failed to upload resume: ' + err.message);
                            nextBtn.disabled = false;
                            nextBtn.textContent = 'Next';
                        }
                    } else {
                        currentStep = 2;
                        render();
                    }
                } else if (currentStep === 2) {
                    // Eligibility step - go to step 3
                    currentStep = 3;
                    render();
                } else if (currentStep === 3) {
                    // Fit step - submit and go to step 4
                    console.log('Step 3 (Fit) - submitting...');
                    nextBtn.disabled = true;
                    nextBtn.textContent = 'Submitting...';
                    try {
                        await submitApplicationData();
                        console.log('Submit successful, moving to step 4');
                        currentStep = 4;
                        render();
                    } catch (err) {
                        console.error('Failed to submit application:', err);
                        alert('Failed to submit: ' + err.message);
                        nextBtn.disabled = false;
                        nextBtn.textContent = 'Continue';
                    }
                }
            });
        }

        // Resume step listeners
        if (currentStep === 1) {
            const dropzone = modal.querySelector('#resume-dropzone');
            const fileInput = modal.querySelector('#resume-file-input');

            if (dropzone && fileInput) {
                // Click to browse
                dropzone.addEventListener('click', () => fileInput.click());

                // File selected
                fileInput.addEventListener('change', (e) => {
                    if (e.target.files.length > 0) {
                        resumeFile = e.target.files[0];
                        render();
                    }
                });

                // Drag and drop
                dropzone.addEventListener('dragover', (e) => {
                    e.preventDefault();
                    dropzone.classList.add('dragover');
                });
                dropzone.addEventListener('dragleave', () => {
                    dropzone.classList.remove('dragover');
                });
                dropzone.addEventListener('drop', (e) => {
                    e.preventDefault();
                    dropzone.classList.remove('dragover');
                    if (e.dataTransfer.files.length > 0) {
                        resumeFile = e.dataTransfer.files[0];
                        render();
                    }
                });
            }

            // Remove resume button
            modal.querySelector('#remove-resume')?.addEventListener('click', (e) => {
                e.stopPropagation();
                resumeFile = null;
                render();
            });

            // Replace resume button (when already uploaded)
            modal.querySelector('#replace-resume')?.addEventListener('click', () => {
                resumeUploaded = false;
                resumeFile = null;
                render();
            });
        }

        // Check permissions button (Step 3)
        modal.querySelector('#check-permissions')?.addEventListener('click', async () => {
            const btn = modal.querySelector('#check-permissions');
            if (btn) {
                btn.disabled = true;
                btn.innerHTML = `
                    <svg class="spinner" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <circle cx="12" cy="12" r="10" stroke-dasharray="32" stroke-dashoffset="32"></circle>
                    </svg>
                    Requesting permissions...
                `;
            }

            const permissions = await checkMediaPermissions();
            permissionsState.camera = permissions.camera;
            permissionsState.microphone = permissions.microphone;

            // Re-render Step 3 to update status
            render();
        });

        // Start Interview button
        modal.querySelector('#start-interview')?.addEventListener('click', async () => {
            const allReady = permissionsState.camera === true && permissionsState.microphone === true;
            if (!allReady) {
                alert('Please enable camera and microphone permissions first.');
                return;
            }

            // Start the AI interview
            modal.remove();

            await startAIInterview(
                applicationId,
                // On complete
                (metadata) => {
                    showSuccessModal(
                        'Interview Complete!',
                        `Great job! Your ${metadata?.questions_asked || ''} question interview has been submitted. The hiring team will review your responses.`
                    );
                    refreshCurrentRole();
                },
                // On close/cancel
                () => {
                    showSuccessModal(
                        'Interview Saved',
                        'Your progress has been saved. You can complete the interview later from your shortlist.'
                    );
                    refreshCurrentRole();
                }
            );
        });

        // Defer interview
        modal.querySelector('#defer-interview')?.addEventListener('click', async () => {
            // Make sure application data is submitted if not already
            if (!applicationId) {
                await submitApplicationData();
            }
            modal.remove();
            showSuccessModal('Added to Shortlist!', 'You can complete the AI interview later from your dashboard to improve your ranking.');
            refreshCurrentRole();
        });

        // Step 2 (Eligibility) listeners - update UI locally without re-render
        if (currentStep === 2) {
            // Pill toggles
            modal.querySelectorAll('.pill-toggle').forEach(toggle => {
                toggle.querySelectorAll('.pill-option').forEach(btn => {
                    btn.addEventListener('click', () => {
                        const field = toggle.dataset.field;
                        const value = btn.dataset.value === 'true';
                        eligibilityData[field] = value;

                        // Update UI locally
                        toggle.querySelectorAll('.pill-option').forEach(b => b.classList.remove('selected'));
                        btn.classList.add('selected');

                        // Handle conditional commute field
                        if (field === 'hybrid_onsite') {
                            const conditionalField = toggle.closest('.eligibility-card').querySelector('.conditional-field');
                            if (conditionalField) {
                                if (value) {
                                    conditionalField.classList.add('expanded');
                                } else {
                                    conditionalField.classList.remove('expanded');
                                }
                            }
                        }

                        updateNextButtonState();
                    });
                });
            });

            // Selects
            modal.querySelectorAll('.flow-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const field = e.target.dataset.field;
                    eligibilityData[field] = e.target.value;
                    updateNextButtonState();
                });
            });

            // Seniority pills
            modal.querySelectorAll('.seniority-pill').forEach(pill => {
                pill.addEventListener('click', () => {
                    eligibilityData.seniority_band = pill.dataset.band;

                    // Update UI locally
                    modal.querySelectorAll('.seniority-pill').forEach(p => p.classList.remove('selected'));
                    pill.classList.add('selected');

                    updateNextButtonState();
                });
            });

            // Must-have skill checkboxes
            modal.querySelectorAll('.must-have-checkbox input').forEach(checkbox => {
                checkbox.addEventListener('change', (e) => {
                    const index = parseInt(e.target.dataset.skillIndex);
                    const isChecked = e.target.checked;
                    eligibilityData.must_have_skills[index].checked = isChecked;

                    // Update UI locally
                    const item = e.target.closest('.must-have-item');
                    const evidenceInput = item.querySelector('.evidence-input');

                    if (isChecked) {
                        item.classList.add('checked');
                        evidenceInput.classList.add('visible');
                    } else {
                        item.classList.remove('checked');
                        evidenceInput.classList.remove('visible');
                        eligibilityData.must_have_skills[index].evidence = '';
                        evidenceInput.querySelector('input').value = '';
                    }
                });
            });

            // Evidence inputs
            modal.querySelectorAll('.evidence-input input').forEach(input => {
                input.addEventListener('input', (e) => {
                    const index = parseInt(e.target.dataset.evidenceIndex);
                    eligibilityData.must_have_skills[index].evidence = e.target.value;
                    // Update char count without full re-render
                    const charCount = input.parentElement.querySelector('.char-count');
                    if (charCount) charCount.textContent = `${e.target.value.length}/100`;
                });
            });

            // Portfolio link
            modal.querySelector('[data-field="portfolio_link"]')?.addEventListener('input', (e) => {
                eligibilityData.portfolio_link = e.target.value;
            });
        }

        // Step 3 (Fit) listeners - update UI locally without re-render
        if (currentStep === 3) {
            // MC options
            modal.querySelectorAll('.fit-option-card input').forEach(radio => {
                radio.addEventListener('change', (e) => {
                    const questionId = e.target.closest('.fit-card').dataset.questionId;
                    fitResponses[questionId] = { question_id: questionId, response_value: e.target.value };

                    // Update UI locally
                    const card = e.target.closest('.fit-card');
                    card.querySelectorAll('.fit-option-card').forEach(opt => opt.classList.remove('selected'));
                    e.target.closest('.fit-option-card').classList.add('selected');

                    updateNextButtonState();
                });
            });

            // Free response
            modal.querySelectorAll('.fit-freeform-input').forEach(textarea => {
                textarea.addEventListener('input', (e) => {
                    const questionId = e.target.dataset.questionId;
                    const value = e.target.value.trim();
                    if (value) {
                        fitResponses[questionId] = { question_id: questionId, response_text: value };
                    } else {
                        delete fitResponses[questionId];
                    }
                    // Update char count
                    const footer = e.target.parentElement.querySelector('.char-count');
                    if (footer) footer.textContent = `${e.target.value.length}/300`;

                    updateNextButtonState();
                });
            });
        }
    }

    async function submitApplicationData() {
        try {
            // Create the application now (deferred from initial click)
            if (!applicationId) {
                const result = await api('/shortlist/apply', {
                    method: 'POST',
                    body: JSON.stringify({ role_id: roleId })
                });
                applicationId = result.application_id;
            }

            // Submit eligibility data
            await api(`/shortlist/submit-eligibility/${applicationId}`, {
                method: 'POST',
                body: JSON.stringify(eligibilityData)
            });

            // Submit fit responses if any
            if (Object.keys(fitResponses).length > 0) {
                await api(`/shortlist/submit-fit-responses/${applicationId}`, {
                    method: 'POST',
                    body: JSON.stringify({ responses: Object.values(fitResponses) })
                });
            }
        } catch (err) {
            console.error('Failed to submit application data:', err);
            throw err; // Re-throw so we can handle it in the caller
        }
    }

    function refreshCurrentRole() {
        // Refresh the current role view if we're on the role detail page
        if (state.currentRole) {
            renderRoleDetail();
        }
        // Also refresh applications list
        loadMyApplications();
    }

    render();
    document.body.appendChild(modal);
}

async function uploadResume(applicationId, file) {
    const formData = new FormData();
    formData.append('file', file);

    const response = await fetch(`${API_BASE}/shortlist/upload-resume/${applicationId}`, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${state.token}`
        },
        body: formData
    });

    const data = await response.json();
    if (!response.ok) {
        throw new Error(data.error || 'Upload failed');
    }

    return data;
}

async function uploadProfileResume(file) {
    const formData = new FormData();
    formData.append('file', file);

    const response = await fetch(`${API_BASE}/profile/upload-resume`, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${state.token}`
        },
        body: formData
    });

    const data = await response.json();
    if (!response.ok) {
        throw new Error(data.error || 'Upload failed');
    }

    // Update user state to reflect resume is uploaded
    if (state.user) {
        state.user.has_resume = true;
    }

    return data;
}

// =============================================================================
// AI INTERVIEW MODULE
// =============================================================================

/**
 * Start the AI Interview experience
 * @param {number} applicationId - The application ID
 * @param {function} onComplete - Callback when interview completes
 * @param {function} onClose - Callback when user closes
 */
async function startAIInterview(applicationId, onComplete, onClose) {
    // Create full-screen interview overlay
    const overlay = document.createElement('div');
    overlay.className = 'interview-overlay';

    // Interview state
    let ws = null;
    let mediaStream = null;
    let mediaRecorder = null;
    let audioChunks = [];
    let isRecording = false;
    let currentPhase = 'connecting'; // connecting, rapport, question, listening, processing, complete
    let questionNumber = 0;
    let totalQuestions = 0;
    let audioQueue = [];
    let isPlayingAudio = false;
    let conversationMessages = []; // Store messages to persist across renders
    let liveTranscript = ''; // Current streaming transcript
    let isUserScrolledUp = false; // Track if user scrolled up
    let startTime = null; // Interview start time

    function getElapsedTime() {
        if (!startTime) return '0:00';
        const elapsed = Math.floor((Date.now() - startTime) / 1000);
        const mins = Math.floor(elapsed / 60);
        const secs = elapsed % 60;
        return `${mins}:${secs.toString().padStart(2, '0')}`;
    }

    // Confetti celebration effect
    function launchConfetti() {
        const colors = ['#4F46E5', '#06B6D4', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6'];
        const confettiCount = 150;
        const container = overlay;

        for (let i = 0; i < confettiCount; i++) {
            const confetti = document.createElement('div');
            confetti.className = 'confetti-piece';
            confetti.style.cssText = `
                position: fixed;
                width: ${Math.random() * 10 + 5}px;
                height: ${Math.random() * 10 + 5}px;
                background: ${colors[Math.floor(Math.random() * colors.length)]};
                left: ${Math.random() * 100}vw;
                top: -20px;
                opacity: 1;
                border-radius: ${Math.random() > 0.5 ? '50%' : '0'};
                transform: rotate(${Math.random() * 360}deg);
                pointer-events: none;
                z-index: 10000;
            `;
            container.appendChild(confetti);

            // Animate each piece
            const duration = Math.random() * 2000 + 2000;
            const horizontalDrift = (Math.random() - 0.5) * 200;
            const rotation = Math.random() * 720 - 360;

            confetti.animate([
                {
                    transform: `translateY(0) translateX(0) rotate(0deg)`,
                    opacity: 1
                },
                {
                    transform: `translateY(100vh) translateX(${horizontalDrift}px) rotate(${rotation}deg)`,
                    opacity: 0
                }
            ], {
                duration: duration,
                easing: 'cubic-bezier(0.25, 0.46, 0.45, 0.94)'
            }).onfinish = () => confetti.remove();
        }
    }

    // Track previous scroll position and message count for render
    let lastScrollTop = 0;
    let lastMessageCount = 0;

    function render() {
        if (!startTime && currentPhase !== 'connecting') {
            startTime = Date.now();
        }

        // Capture scroll position before re-render
        const oldContainer = document.getElementById('transcript-container');
        if (oldContainer) {
            lastScrollTop = oldContainer.scrollTop;
        }
        const currentMessageCount = conversationMessages.length;

        overlay.innerHTML = `
            <div class="studio-interview">
                <!-- Minimal Header Rail -->
                <div class="studio-header">
                    <div class="studio-brand">
                        <span class="brand-mark">S</span>
                        <span class="brand-status ${currentPhase === 'connecting' ? 'connecting' : 'live'}">
                            ${currentPhase === 'connecting' ? 'Connecting' : 'Live'}
                        </span>
                    </div>
                    <div class="studio-progress">
                        ${totalQuestions > 0 ? `
                            <span class="progress-label">Question ${questionNumber || 1} of ${totalQuestions}</span>
                            <div class="progress-track">
                                <div class="progress-fill" style="width: ${Math.max(((questionNumber || 1) / totalQuestions) * 100, 10)}%"></div>
                            </div>
                        ` : '<span class="progress-label">Preparing...</span>'}
                    </div>
                    <div class="studio-meta">
                        <span class="elapsed-time" id="elapsed-time">${getElapsedTime()}</span>
                        <button class="exit-btn" id="close-interview" title="End interview">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <line x1="18" y1="6" x2="6" y2="18"></line>
                                <line x1="6" y1="6" x2="18" y2="18"></line>
                            </svg>
                        </button>
                    </div>
                </div>

                <!-- Main Two-Zone Layout -->
                <div class="studio-main">
                    <!-- Central Conversation Zone -->
                    <div class="conversation-zone">
                        <div class="transcript-container" id="transcript-container">
                            ${currentPhase === 'connecting' ? `
                                <div class="connecting-state">
                                    <div class="pulse-ring"></div>
                                    <p>Setting up your interview...</p>
                                </div>
                            ` : `
                                <div class="transcript-thread" id="transcript-thread">
                                    ${conversationMessages.map((msg, idx) => `
                                        <div class="transcript-entry ${msg.type} ${msg.isLive ? 'live' : 'final'}">
                                            <div class="entry-indicator ${msg.type}">
                                                ${msg.type === 'interviewer' ? 'AI' : 'You'}
                                            </div>
                                            <div class="entry-content">
                                                <p class="${msg.isLive ? 'streaming' : ''}">${escapeHtml(msg.text)}${msg.isLive ? '<span class="typing-caret"></span>' : ''}</p>
                                            </div>
                                        </div>
                                    `).join('')}
                                    ${liveTranscript ? `
                                        <div class="transcript-entry candidate live">
                                            <div class="entry-indicator candidate">You</div>
                                            <div class="entry-content">
                                                <p class="streaming">${escapeHtml(liveTranscript)}<span class="typing-caret"></span></p>
                                            </div>
                                        </div>
                                    ` : ''}
                                </div>
                            `}
                        </div>
                        ${isUserScrolledUp ? `
                            <button class="jump-to-live" id="jump-to-live">
                                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M12 19V5M5 12l7-7 7 7"/>
                                </svg>
                                Jump to live
                            </button>
                        ` : ''}
                    </div>

                    <!-- Control Rail -->
                    <div class="control-rail">
                        <!-- Self Video -->
                        <div class="self-preview">
                            <video id="self-video" autoplay muted playsinline></video>
                            <div class="preview-label">You</div>
                        </div>

                        <!-- Waveform Visualizer -->
                        <div class="waveform-container ${isPlayingAudio ? 'ai-speaking' : isRecording ? 'user-speaking' : ''}">
                            <div class="waveform" id="waveform">
                                ${Array(24).fill(0).map(() => '<div class="wave-bar"></div>').join('')}
                            </div>
                            <div class="waveform-label">
                                ${isPlayingAudio ? 'AI Speaking' : isRecording ? 'Listening...' : currentPhase === 'processing' ? 'Processing...' : 'Ready'}
                            </div>
                        </div>

                        <!-- Mic Status -->
                        <div class="mic-status ${isRecording ? 'active' : ''}" id="mic-status">
                            <div class="mic-icon">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3z"></path>
                                    <path d="M19 10v2a7 7 0 0 1-14 0v-2"></path>
                                    <line x1="12" y1="19" x2="12" y2="22"></line>
                                </svg>
                            </div>
                            <span class="mic-label">${isRecording ? 'Mic Active' : 'Mic Ready'}</span>
                        </div>

                        ${isRecording ? `
                            <button class="done-btn" id="done-speaking-btn">
                                Done Speaking
                            </button>
                        ` : ''}
                    </div>
                </div>
            </div>
        `;
        attachInterviewListeners();

        // Re-attach video stream after render (use setTimeout to ensure DOM is ready)
        setTimeout(() => {
            if (mediaStream) {
                const video = document.getElementById('self-video');
                if (video) {
                    video.srcObject = mediaStream;
                    video.play().catch(e => console.log('Video autoplay:', e));
                }
            }

            const container = document.getElementById('transcript-container');
            if (container) {
                // Only scroll to bottom if new messages were added
                const hasNewMessages = currentMessageCount > lastMessageCount;
                lastMessageCount = currentMessageCount;

                if (hasNewMessages && !isUserScrolledUp) {
                    // New message - smooth scroll to bottom
                    smoothScrollToBottom(container);
                } else if (!hasNewMessages && lastScrollTop > 0) {
                    // No new messages - restore previous scroll position
                    container.scrollTop = lastScrollTop;
                }
            }

            // Set up scroll listener for "Jump to live" button
            setupScrollListener();
        }, 50);
    }

    // Track user scroll position
    function setupScrollListener() {
        const container = document.getElementById('transcript-container');
        if (!container || container.dataset.scrollListenerAdded) return;

        container.dataset.scrollListenerAdded = 'true';
        container.addEventListener('scroll', () => {
            const isAtBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 100;
            const wasScrolledUp = isUserScrolledUp;
            isUserScrolledUp = !isAtBottom;

            // Only update button visibility, don't re-render entire UI
            if (wasScrolledUp !== isUserScrolledUp) {
                const existingBtn = document.querySelector('.jump-to-live');
                const zone = document.querySelector('.conversation-zone');
                if (isUserScrolledUp && !existingBtn && zone) {
                    const btn = document.createElement('button');
                    btn.className = 'jump-to-live';
                    btn.id = 'jump-to-live';
                    btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 19V5M5 12l7-7 7 7"/></svg> Jump to live`;
                    btn.onclick = () => {
                        isUserScrolledUp = false;
                        smoothScrollToBottom(container);
                        btn.remove();
                    };
                    zone.appendChild(btn);
                } else if (!isUserScrolledUp && existingBtn) {
                    existingBtn.remove();
                }
            }
        });
    }

    // Elapsed time interval
    let timeInterval = null;
    function startTimeInterval() {
        if (timeInterval) return;
        timeInterval = setInterval(() => {
            const timeEl = document.getElementById('elapsed-time');
            if (timeEl) {
                timeEl.textContent = getElapsedTime();
            }
        }, 1000);
    }

    // AI speaking visualizer animation
    let visualizerInterval = null;

    function startSpeakingAnimation() {
        const visualizer = document.getElementById('audio-visualizer');
        if (!visualizer) return;
        visualizer.classList.add('speaking');

        // Animate bars randomly
        const bars = visualizer.querySelectorAll('.visualizer-bar');
        visualizerInterval = setInterval(() => {
            bars.forEach(bar => {
                const height = Math.random() * 80 + 20;
                bar.style.height = `${height}%`;
            });
        }, 100);
    }

    function stopSpeakingAnimation() {
        const visualizer = document.getElementById('audio-visualizer');
        if (visualizer) {
            visualizer.classList.remove('speaking');
        }
        if (visualizerInterval) {
            clearInterval(visualizerInterval);
            visualizerInterval = null;
        }
        // Reset bar heights
        const bars = document.querySelectorAll('.visualizer-bar');
        bars.forEach(bar => bar.style.height = '20%');
    }

    function getStatusText() {
        switch (currentPhase) {
            case 'connecting': return 'Connecting to interviewer...';
            case 'rapport': return 'Getting to know you';
            case 'question': return 'Interviewer is speaking';
            case 'listening': return 'Your turn to respond';
            case 'processing': return 'Processing your response...';
            case 'complete': return 'Interview complete!';
            default: return '';
        }
    }

    // Streaming type-on effect variables
    let streamingInterval = null;
    let streamingMessageIndex = -1;
    let streamingCharIndex = 0;
    let streamingFullText = '';

    function updateConversation(message, type = 'interviewer', useStreaming = false) {
        // Store message in array for persistence across renders
        const messageObj = { text: message, type: type, isLive: useStreaming };
        conversationMessages.push(messageObj);

        // Try to add message directly to DOM without full re-render
        const thread = document.getElementById('transcript-thread');
        const container = document.getElementById('transcript-container');

        if (thread && container) {
            // Add new entry directly to DOM
            const entry = document.createElement('div');
            entry.className = `transcript-entry ${type} ${useStreaming ? 'live' : 'final'}`;
            entry.innerHTML = `
                <div class="entry-indicator ${type}">
                    ${type === 'interviewer' ? 'AI' : 'You'}
                </div>
                <div class="entry-content">
                    <p class="${useStreaming ? 'streaming' : ''}">${useStreaming ? '' : escapeHtml(message)}${useStreaming ? '<span class="typing-caret"></span>' : ''}</p>
                </div>
            `;
            thread.appendChild(entry);

            if (useStreaming && type === 'interviewer') {
                // Start streaming effect
                streamingMessageIndex = conversationMessages.length - 1;
                streamingFullText = message;
                streamingCharIndex = 0;
                conversationMessages[streamingMessageIndex].text = '';
                conversationMessages[streamingMessageIndex].isLive = true;
                startStreamingEffect();
            }

            // Smooth scroll to bottom for new messages
            if (!isUserScrolledUp) {
                smoothScrollToBottom(container);
            }
        } else {
            // Fallback to full render if DOM elements not found
            if (useStreaming && type === 'interviewer') {
                streamingMessageIndex = conversationMessages.length - 1;
                streamingFullText = message;
                streamingCharIndex = 0;
                conversationMessages[streamingMessageIndex].text = '';
                conversationMessages[streamingMessageIndex].isLive = true;
            }
            render();
            if (useStreaming) {
                startStreamingEffect();
            }
        }
    }

    function startStreamingEffect() {
        if (streamingInterval) clearInterval(streamingInterval);

        const CHARS_PER_TICK = 3; // Characters to add per tick
        const TICK_INTERVAL = 25; // ms between ticks

        streamingInterval = setInterval(() => {
            if (streamingMessageIndex < 0 || streamingMessageIndex >= conversationMessages.length) {
                stopStreamingEffect();
                return;
            }

            const container = document.getElementById('transcript-container');
            const thread = document.getElementById('transcript-thread');
            if (!thread || !container) {
                stopStreamingEffect();
                return;
            }

            const entries = thread.querySelectorAll('.transcript-entry');
            const entry = entries[streamingMessageIndex];
            if (!entry) {
                stopStreamingEffect();
                return;
            }

            const textEl = entry.querySelector('.entry-content p');
            if (!textEl) {
                stopStreamingEffect();
                return;
            }

            streamingCharIndex += CHARS_PER_TICK;

            if (streamingCharIndex >= streamingFullText.length) {
                // Finished streaming - update DOM directly WITHOUT re-render
                conversationMessages[streamingMessageIndex].text = streamingFullText;
                conversationMessages[streamingMessageIndex].isLive = false;

                // Preserve scroll position before DOM update
                const scrollTop = container.scrollTop;
                const wasAtBottom = container.scrollHeight - scrollTop - container.clientHeight < 50;

                // Update DOM directly - just remove caret and update classes
                textEl.textContent = streamingFullText;
                textEl.classList.remove('streaming');
                entry.classList.remove('live');
                entry.classList.add('final');

                // Restore scroll position (prevents jump to top)
                if (!wasAtBottom) {
                    container.scrollTop = scrollTop;
                }

                stopStreamingEffect();
            } else {
                // Still streaming - update text
                conversationMessages[streamingMessageIndex].text = streamingFullText.substring(0, streamingCharIndex);
                textEl.innerHTML = escapeHtml(conversationMessages[streamingMessageIndex].text) + '<span class="typing-caret"></span>';

                // Keep at bottom during streaming (instant, not smooth)
                if (!isUserScrolledUp) {
                    container.scrollTop = container.scrollHeight;
                }
            }
        }, TICK_INTERVAL);
    }

    function stopStreamingEffect() {
        if (streamingInterval) {
            clearInterval(streamingInterval);
            streamingInterval = null;
        }
        streamingMessageIndex = -1;
        streamingCharIndex = 0;
        streamingFullText = '';
    }

    function smoothScrollToBottom(element) {
        if (!element) return;
        element.scrollTo({
            top: element.scrollHeight,
            behavior: 'smooth'
        });
    }

    function updateLastCandidateMessage(transcription) {
        // Update the last candidate message in the stored array
        let msgIndex = -1;
        for (let i = conversationMessages.length - 1; i >= 0; i--) {
            if (conversationMessages[i].type === 'candidate') {
                conversationMessages[i].text = transcription;
                conversationMessages[i].isLive = false;
                msgIndex = i;
                break;
            }
        }

        // Update DOM directly without full re-render
        if (msgIndex >= 0) {
            const thread = document.getElementById('transcript-thread');
            if (thread) {
                const entries = thread.querySelectorAll('.transcript-entry');
                const entry = entries[msgIndex];
                if (entry) {
                    const textEl = entry.querySelector('.entry-content p');
                    if (textEl) {
                        textEl.textContent = transcription;
                        textEl.classList.remove('streaming');
                    }
                    entry.classList.remove('live');
                    entry.classList.add('final');
                }
            }
        }
    }

    function showStatus(message, type = 'info') {
        const statusText = document.getElementById('status-text');
        if (statusText) {
            statusText.textContent = message;
            statusText.className = `interview-status-text ${type}`;
        }
    }

    // Audio playback queue
    async function playAudio(base64Audio) {
        if (!base64Audio) return Promise.resolve();

        return new Promise((resolve) => {
            try {
                const audio = new Audio(`data:audio/mp3;base64,${base64Audio}`);
                startSpeakingAnimation();
                audio.onended = () => {
                    stopSpeakingAnimation();
                    resolve();
                };
                audio.onerror = () => {
                    console.error('Audio playback error');
                    stopSpeakingAnimation();
                    resolve();
                };
                audio.play().catch(() => {
                    stopSpeakingAnimation();
                    resolve();
                });
            } catch (e) {
                console.error('Audio playback failed:', e);
                stopSpeakingAnimation();
                resolve();
            }
        });
    }

    async function processAudioQueue() {
        if (isPlayingAudio || audioQueue.length === 0) return;

        isPlayingAudio = true;
        updateControlZoneState(); // Show AI speaking waveform
        while (audioQueue.length > 0) {
            const audioData = audioQueue.shift();
            await playAudio(audioData);
        }
        isPlayingAudio = false;
        updateControlZoneState(); // Clear AI speaking waveform

        // Auto-start listening after interviewer finishes speaking
        if (currentPhase !== 'complete' && currentPhase !== 'processing' && currentPhase !== 'connecting') {
            startContinuousListening();
        }
    }

    // Continuous listening with silence detection
    let audioContext = null;
    let analyser = null;
    let silenceTimeout = null;
    let listeningStartTime = null;
    let hasSpokenYet = false; // Track if user has started speaking
    const SILENCE_THRESHOLD = 15; // Audio level below this is considered silence
    const SPEECH_THRESHOLD = 25; // Audio level above this means user is speaking
    const SILENCE_DURATION = 2000; // ms of silence AFTER speaking before stopping
    const MIN_RECORDING_TIME = 4000; // minimum ms before checking silence (give time to think)
    const MIN_SPEECH_BEFORE_CUTOFF = 1000; // must have at least 1s of speech before auto-cutoff

    function startContinuousListening() {
        if (isRecording || isPlayingAudio || currentPhase === 'processing') {
            console.log('[INTERVIEW] Cannot start listening:', { isRecording, isPlayingAudio, currentPhase });
            return;
        }

        console.log('[INTERVIEW] Starting continuous listening...');
        currentPhase = 'listening';
        showStatus('Listening... speak naturally, then click "Done Speaking"');
        // Use targeted update instead of full render to preserve scroll
        updateStatusIndicators();
        updateControlZoneState();

        startRecordingWithSilenceDetection();
    }

    async function startRecordingWithSilenceDetection() {
        if (!mediaStream || isRecording) {
            console.log('[INTERVIEW] Cannot start recording:', { mediaStream: !!mediaStream, isRecording });
            return;
        }
        console.log('[INTERVIEW] Starting audio recording...');

        // Create audio-only stream for recording
        const audioTracks = mediaStream.getAudioTracks();
        if (audioTracks.length === 0) {
            console.error('[INTERVIEW] No audio tracks to record!');
            return;
        }

        const audioOnlyStream = new MediaStream(audioTracks);
        audioChunks = [];

        try {
            mediaRecorder = new MediaRecorder(audioOnlyStream, { mimeType: 'audio/webm' });
            console.log('[INTERVIEW] MediaRecorder created successfully');
        } catch (err) {
            console.error('[INTERVIEW] MediaRecorder creation failed:', err);
            // Try without specifying mimeType
            mediaRecorder = new MediaRecorder(audioOnlyStream);
            console.log('[INTERVIEW] MediaRecorder created with default mimeType');
        }

        mediaRecorder.ondataavailable = (e) => {
            if (e.data.size > 0) {
                audioChunks.push(e.data);
                console.log('[INTERVIEW] Audio chunk received:', e.data.size, 'bytes');
            }
        };

        mediaRecorder.onerror = (e) => {
            console.error('[INTERVIEW] MediaRecorder error:', e);
        };

        mediaRecorder.start(100);
        isRecording = true;
        listeningStartTime = Date.now();
        hasSpokenYet = false; // Reset speech tracking
        speechStartTime = null;
        console.log('[INTERVIEW] Recording started - waiting for speech');
        // Use targeted update instead of full render to preserve scroll
        updateControlZoneState();

        // Set up audio analysis for silence detection
        if (!audioContext) {
            audioContext = new (window.AudioContext || window.webkitAudioContext)();
        }

        const source = audioContext.createMediaStreamSource(mediaStream);
        analyser = audioContext.createAnalyser();
        analyser.fftSize = 256;
        source.connect(analyser);

        // Start monitoring for silence
        monitorAudioLevel();
    }

    let speechStartTime = null; // When user started speaking

    function monitorAudioLevel() {
        if (!isRecording || !analyser) return;

        const dataArray = new Uint8Array(analyser.frequencyBinCount);
        analyser.getByteFrequencyData(dataArray);

        // Calculate average volume
        const average = dataArray.reduce((a, b) => a + b, 0) / dataArray.length;

        const timeSinceStart = Date.now() - listeningStartTime;

        // Update visual feedback - show listening indicator
        updateListeningIndicator(average);

        // Track when user starts speaking
        if (average >= SPEECH_THRESHOLD && !hasSpokenYet) {
            hasSpokenYet = true;
            speechStartTime = Date.now();
            console.log('[INTERVIEW] User started speaking');
        }

        // Calculate how long user has been speaking
        const speechDuration = speechStartTime ? Date.now() - speechStartTime : 0;

        // Only check for silence cutoff if:
        // 1. Enough time has passed (MIN_RECORDING_TIME) to give user time to think
        // 2. User has actually spoken (hasSpokenYet)
        // 3. User has spoken for at least MIN_SPEECH_BEFORE_CUTOFF
        const canCutOff = timeSinceStart > MIN_RECORDING_TIME &&
                          hasSpokenYet &&
                          speechDuration > MIN_SPEECH_BEFORE_CUTOFF;

        if (average < SILENCE_THRESHOLD && canCutOff) {
            // Silence detected after speaking
            if (!silenceTimeout) {
                silenceTimeout = setTimeout(() => {
                    if (isRecording) {
                        console.log('[INTERVIEW] Silence detected after speaking, stopping recording...');
                        finishRecordingAndSend();
                    }
                }, SILENCE_DURATION);
            }
        } else if (average >= SILENCE_THRESHOLD) {
            // Sound detected, clear silence timeout
            if (silenceTimeout) {
                clearTimeout(silenceTimeout);
                silenceTimeout = null;
            }
        }

        // Continue monitoring
        if (isRecording) {
            requestAnimationFrame(monitorAudioLevel);
        }
    }

    function updateListeningIndicator(level) {
        const indicator = document.getElementById('listening-indicator');
        if (indicator) {
            const normalizedLevel = Math.min(level / 50, 1);
            indicator.style.transform = `scale(${1 + normalizedLevel * 0.5})`;
            indicator.style.opacity = 0.5 + normalizedLevel * 0.5;
        }
    }

    async function finishRecordingAndSend() {
        console.log('[INTERVIEW] finishRecordingAndSend called, mediaRecorder:', !!mediaRecorder, 'isRecording:', isRecording);
        if (!mediaRecorder || !isRecording) {
            console.log('[INTERVIEW] Cannot finish recording - not recording');
            return;
        }

        clearTimeout(silenceTimeout);
        silenceTimeout = null;

        return new Promise((resolve) => {
            mediaRecorder.onstop = async () => {
                console.log('[INTERVIEW] Recording stopped, chunks:', audioChunks.length);
                const blob = new Blob(audioChunks, { type: 'audio/webm' });
                console.log('[INTERVIEW] Audio blob size:', blob.size, 'bytes');

                // Only send if we have meaningful audio (more than ~0.5 seconds)
                if (blob.size > 5000) {
                    const reader = new FileReader();
                    reader.onloadend = () => {
                        const base64 = reader.result.split(',')[1];
                        console.log('[INTERVIEW] Sending audio, base64 length:', base64.length);
                        updateConversation('[Processing your response...]', 'candidate');
                        sendResponse(base64, 'audio');
                        resolve();
                    };
                    reader.readAsDataURL(blob);
                } else {
                    console.log('[INTERVIEW] Audio too short:', blob.size, 'bytes, restarting listening...');
                    // Restart listening if audio was too short
                    setTimeout(() => {
                        if (currentPhase !== 'processing' && currentPhase !== 'complete') {
                            startContinuousListening();
                        }
                    }, 500);
                    resolve();
                }
            };
            mediaRecorder.stop();
            isRecording = false;
            // Don't call full render() here - just update status indicators in-place
            // to avoid resetting scroll position
            updateStatusIndicators();
        });
    }

    function updateStatusIndicators() {
        // Update status and control zone without full re-render
        const statusEl = document.querySelector('.status-message');
        if (statusEl) {
            statusEl.textContent = statusMessage || 'Processing...';
        }
        const controlZone = document.querySelector('.control-zone');
        if (controlZone) {
            const isProcessing = currentPhase === 'processing';
            const doneBtn = document.getElementById('done-speaking-btn');
            if (doneBtn) {
                doneBtn.style.display = isProcessing ? 'none' : 'flex';
            }
        }

        // Update progress indicator
        const progressContainer = document.querySelector('.studio-progress');
        if (progressContainer && totalQuestions > 0) {
            const progressPercent = Math.max(((questionNumber || 1) / totalQuestions) * 100, 10);
            progressContainer.innerHTML = `
                <span class="progress-label">Question ${questionNumber || 1} of ${totalQuestions}</span>
                <div class="progress-track">
                    <div class="progress-fill" style="width: ${progressPercent}%"></div>
                </div>
            `;
        }
    }

    function updateControlZoneState() {
        // Update the waveform/recording indicator state without full re-render
        const waveformContainer = document.querySelector('.waveform-container');
        if (waveformContainer) {
            // Remove both classes first
            waveformContainer.classList.remove('ai-speaking', 'user-speaking');
            // Add the appropriate class based on state
            if (isPlayingAudio) {
                waveformContainer.classList.add('ai-speaking');
            } else if (isRecording) {
                waveformContainer.classList.add('user-speaking');
            }
        }

        // Update waveform label
        const waveformLabel = document.querySelector('.waveform-label');
        if (waveformLabel) {
            if (isPlayingAudio) {
                waveformLabel.textContent = 'AI Speaking';
            } else if (isRecording) {
                waveformLabel.textContent = 'Listening...';
            } else if (currentPhase === 'processing') {
                waveformLabel.textContent = 'Processing...';
            } else {
                waveformLabel.textContent = 'Ready';
            }
        }

        // Update mic status
        const micStatus = document.getElementById('mic-status');
        if (micStatus) {
            if (isRecording) {
                micStatus.classList.add('active');
            } else {
                micStatus.classList.remove('active');
            }
            const micLabel = micStatus.querySelector('.mic-label');
            if (micLabel) {
                micLabel.textContent = isRecording ? 'Mic Active' : 'Mic Ready';
            }
        }

        // Update done speaking button visibility
        const doneBtn = document.getElementById('done-speaking-btn');
        if (doneBtn) {
            const showBtn = isRecording && currentPhase === 'listening';
            doneBtn.style.display = showBtn ? 'flex' : 'none';
        }
    }

    function sendResponse(content, type = 'text') {
        if (ws && ws.readyState === WebSocket.OPEN) {
            console.log(`[INTERVIEW] Sending ${type} response, length: ${content.length}`);
            currentPhase = 'processing';
            showStatus('Processing your response...');
            // Use updateStatusIndicators instead of full render to preserve scroll
            updateStatusIndicators();
            ws.send(JSON.stringify({
                type: type,
                content: content,
                timestamp: new Date().toISOString()
            }));
        } else {
            console.error('[INTERVIEW] WebSocket not open, cannot send response');
        }
    }

    function showExitConfirmation() {
        // Create custom modal instead of browser confirm()
        const modal = document.createElement('div');
        modal.className = 'exit-confirm-overlay';
        modal.innerHTML = `
            <div class="exit-confirm-modal">
                <div class="exit-confirm-icon">
                    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#EF4444" stroke-width="2">
                        <circle cx="12" cy="12" r="10"></circle>
                        <line x1="12" y1="8" x2="12" y2="12"></line>
                        <line x1="12" y1="16" x2="12.01" y2="16"></line>
                    </svg>
                </div>
                <h3 class="exit-confirm-title">Exit Interview?</h3>
                <p class="exit-confirm-message">If you leave now, your interview progress will be lost and you'll need to start over.</p>
                <div class="exit-confirm-buttons">
                    <button class="btn btn-secondary exit-confirm-back">Continue Interview</button>
                    <button class="btn btn-danger exit-confirm-leave">Exit Anyway</button>
                </div>
            </div>
        `;
        overlay.appendChild(modal);

        // Handle buttons
        modal.querySelector('.exit-confirm-back').addEventListener('click', () => {
            modal.remove();
        });
        modal.querySelector('.exit-confirm-leave').addEventListener('click', () => {
            modal.remove();
            cleanup();
            onClose?.();
        });
        // Click outside to dismiss
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
    }

    function attachInterviewListeners() {
        // Close button
        document.getElementById('close-interview')?.addEventListener('click', () => {
            showExitConfirmation();
        });

        // Done speaking button - manual override for silence detection
        document.getElementById('done-speaking-btn')?.addEventListener('click', () => {
            console.log('[INTERVIEW] Done speaking button clicked');
            if (isRecording) {
                finishRecordingAndSend();
            }
        });

        // Jump to live button
        document.getElementById('jump-to-live')?.addEventListener('click', () => {
            const container = document.getElementById('transcript-container');
            if (container) {
                isUserScrolledUp = false;
                smoothScrollToBottom(container);
                render();
            }
        });
    }

    async function initMedia() {
        try {
            console.log('[INTERVIEW] Requesting media permissions...');
            mediaStream = await navigator.mediaDevices.getUserMedia({
                video: true,
                audio: true
            });

            const audioTracks = mediaStream.getAudioTracks();
            const videoTracks = mediaStream.getVideoTracks();
            console.log('[INTERVIEW] Media granted - Audio tracks:', audioTracks.length, 'Video tracks:', videoTracks.length);

            if (audioTracks.length === 0) {
                console.error('[INTERVIEW] No audio tracks available!');
                showStatus('Microphone not available', 'error');
                return false;
            }

            const video = document.getElementById('self-video');
            if (video) {
                video.srcObject = mediaStream;
            }
            return true;
        } catch (err) {
            console.error('[INTERVIEW] Media access error:', err);
            showStatus('Camera/microphone access required', 'error');
            return false;
        }
    }

    function connectWebSocket() {
        ws = new WebSocket(`${INTERVIEW_WS_BASE}/ws/interview/${applicationId}`);

        ws.onopen = () => {
            console.log('WebSocket connected, sending auth...');
            ws.send(JSON.stringify({
                type: 'auth',
                token: state.token
            }));
        };

        ws.onmessage = async (event) => {
            const data = JSON.parse(event.data);
            console.log('WS message:', data.type, data);

            switch (data.type) {
                case 'connected':
                    currentPhase = 'rapport';
                    showStatus('Connected! Preparing interview...');
                    break;

                case 'info':
                    showStatus(data.content);
                    break;

                case 'processing':
                    currentPhase = 'processing';
                    showStatus(data.content || 'Processing your response...');
                    break;

                case 'transcription':
                    // Update the last candidate message with the actual transcription
                    updateLastCandidateMessage(data.content);
                    break;

                case 'audio':
                    // Standalone audio message (text was sent separately for faster display)
                    if (data.audio_base64) {
                        audioQueue.push(data.audio_base64);
                        processAudioQueue();
                    }
                    break;

                case 'rapport':
                    currentPhase = 'rapport';
                    totalQuestions = data.total_questions || 0;
                    updateConversation(data.content, 'interviewer', true); // Enable streaming
                    if (data.audio_base64) {
                        audioQueue.push(data.audio_base64);
                        processAudioQueue();
                    }
                    showStatus('Your turn to respond');
                    break;

                case 'acknowledgment':
                    // AI acknowledging what user said before moving to questions
                    updateConversation(data.content, 'interviewer', true); // Enable streaming
                    if (data.audio_base64) {
                        audioQueue.push(data.audio_base64);
                        processAudioQueue();
                    }
                    break;

                case 'question':
                    currentPhase = 'question';
                    questionNumber = data.question_number || 0;
                    totalQuestions = data.total_questions || totalQuestions;
                    updateStatusIndicators(); // Update progress indicator
                    updateConversation(data.content, 'interviewer', true); // Enable streaming
                    if (data.audio_base64) {
                        audioQueue.push(data.audio_base64);
                        processAudioQueue();
                    }
                    showStatus('Your turn to respond');
                    break;

                case 'follow_up':
                    currentPhase = 'question';
                    updateConversation(data.content, 'interviewer', true); // Enable streaming
                    if (data.audio_base64) {
                        audioQueue.push(data.audio_base64);
                        processAudioQueue();
                    }
                    showStatus('Follow-up question');
                    break;

                case 'complete':
                    currentPhase = 'complete';
                    updateConversation(data.content, 'interviewer', true); // Enable streaming
                    if (data.audio_base64) {
                        audioQueue.push(data.audio_base64);
                        await processAudioQueue();
                    }
                    showStatus('Interview complete!', 'success');

                    // Launch confetti celebration!
                    launchConfetti();

                    // Show completion after a moment
                    setTimeout(() => {
                        cleanup();
                        onComplete?.(data.metadata);
                    }, 3000);
                    break;

                case 'error':
                    showStatus(data.content, 'error');
                    console.error('Interview error:', data.content);
                    break;
            }
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            showStatus('Connection error. Please try again.', 'error');
        };

        ws.onclose = () => {
            console.log('WebSocket closed');
            if (currentPhase !== 'complete') {
                showStatus('Connection lost. Your progress has been saved.', 'error');
            }
        };
    }

    function cleanup() {
        // Close WebSocket
        if (ws) {
            ws.close();
            ws = null;
        }

        // Stop media tracks
        if (mediaStream) {
            mediaStream.getTracks().forEach(track => track.stop());
            mediaStream = null;
        }

        // Clear time interval
        if (timeInterval) {
            clearInterval(timeInterval);
            timeInterval = null;
        }

        // Stop streaming effect
        stopStreamingEffect();

        // Remove overlay
        overlay.remove();
    }

    // Initialize
    render();
    document.body.appendChild(overlay);

    const mediaReady = await initMedia();
    if (mediaReady) {
        connectWebSocket();
        startTimeInterval(); // Start elapsed time counter
    }

    // Return cleanup function
    return cleanup;
}

/**
 * Request media permissions and return status
 */
async function checkMediaPermissions() {
    try {
        const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        stream.getTracks().forEach(track => track.stop());
        return { camera: true, microphone: true };
    } catch (err) {
        console.error('Permission check failed:', err);
        // Try to determine which failed
        try {
            const audioStream = await navigator.mediaDevices.getUserMedia({ audio: true });
            audioStream.getTracks().forEach(track => track.stop());
            return { camera: false, microphone: true };
        } catch {
            return { camera: false, microphone: false };
        }
    }
}

// =============================================================================
// END AI INTERVIEW MODULE
// =============================================================================

async function loadMyApplications() {
    const data = await api('/shortlist/my-applications');
    state.myApplications = data.applications;
    renderMyShortlist();
}

// Profile Functions
async function loadProfileData() {
    try {
        const data = await api('/profile/preferences');
        state.profilePreferences = data.preferences || {};
        renderProfileContent();
    } catch (err) {
        console.error('Failed to load profile:', err);
        state.profilePreferences = {};
        renderProfileContent();
    }
}

// Employer Functions
async function loadEmployerRoles() {
    const data = await api('/employer/roles');
    state.employerRoles = data.roles;
    renderEmployerDashboard();
}

async function loadApplicants(roleId) {
    try {
        // Build query params for ranked endpoint
        const params = new URLSearchParams();
        params.set('min_score', state.employerFilters.minScore.toString());
        params.set('include_hidden', state.showHiddenApplicants.toString());
        if (state.employerFilters.seniority.length > 0) {
            state.employerFilters.seniority.forEach(s => params.append('seniority', s));
        }

        const data = await api(`/employer/roles/${roleId}/applicants/ranked?${params.toString()}`);
        state.applicants = data.applicants || [];
        state.hiddenApplicantsCount = data.hidden_count || 0;
        state.totalApplicantsCount = data.total_count || 0;
        state.selectedEmployerRole = data.job || state.employerRoles.find(r => r.id === roleId);
        state.selectedApplicantDetail = null;
        state.drawerOpen = false;
        renderPremiumApplicantsList();
    } catch (err) {
        console.error('Failed to load applicants:', err);
        // Fallback to old endpoint
        try {
            const data = await api(`/employer/roles/${roleId}/applicants`);
            state.applicants = data.applicants || [];
            state.selectedEmployerRole = state.employerRoles.find(r => r.id === roleId);
            renderApplicantsList();
        } catch (fallbackErr) {
            console.error('Fallback also failed:', fallbackErr);
            alert('Failed to load candidates. Please try again.');
        }
    }
}

// Navigation
function navigate(page) {
    state.currentPage = page;
    render();

    // Load data for specific pages
    if (page === 'for-you') {
        // Only fetch if not already loaded (cache results)
        if (!state.forYouLoaded) {
            loadForYouJobs();
        } else {
            // Already loaded - just render the list immediately
            renderForYouList();
        }
    } else if (page === 'explore') {
        loadRoles();
    } else if (page === 'browse') {
        // Legacy - redirect to explore
        loadRoles();
    } else if (page === 'shortlist') {
        loadMyApplications();
    } else if (page === 'employer') {
        loadEmployerRoles();
    } else if (page === 'profile') {
        loadProfileData();
    }
}

// Render Functions
function render() {
    const app = document.getElementById('app');

    switch (state.currentPage) {
        case 'loading':
            app.innerHTML = '<div class="loading">Loading...</div>';
            break;
        case 'home':
            renderLanding();
            break;
        case 'login':
            renderLogin();
            break;
        case 'signup':
            renderSignup();
            break;
        case 'setup':
            renderSetup();
            break;
        case 'resume-upload':
            renderResumeUpload();
            break;
        case 'for-you':
            renderForYou();
            break;
        case 'explore':
            renderExplore();
            break;
        case 'browse':
            renderExplore();  // Legacy - redirect to explore
            break;
        case 'role':
            renderRoleDetail();
            break;
        case 'shortlist':
            renderMyShortlist();
            break;
        case 'employer':
            renderEmployerDashboard();
            break;
        case 'applicants':
            renderApplicantsList();
            break;
        case 'profile':
            renderProfile();
            break;
        default:
            app.innerHTML = '<div class="loading">Page not found</div>';
    }
}

function renderLanding() {
    const app = document.getElementById('app');
    const isLoggedIn = state.user && state.token;
    const isEmployer = isLoggedIn && state.user.user_type === 'employer';

    // Different nav items based on login state
    let navHtml;
    if (isLoggedIn) {
        if (isEmployer) {
            navHtml = `
                <a href="#how-it-works" id="nav-how-it-works">How It Works</a>
                <a href="#" class="btn btn-secondary btn-small" id="nav-dashboard">Dashboard</a>
            `;
        } else {
            navHtml = `
                <a href="#how-it-works" id="nav-how-it-works">How It Works</a>
                <a href="#" class="btn btn-secondary btn-small" id="nav-browse">Browse Roles</a>
            `;
        }
    } else {
        navHtml = `
            <a href="#how-it-works" id="nav-how-it-works">How It Works</a>
            <a href="#" class="btn btn-secondary btn-small" id="nav-login">Sign In</a>
        `;
    }

    app.innerHTML = `
        <div class="landing-page">
            <!-- Header -->
            <header class="landing-header">
                <div class="container">
                    <a href="#" class="logo" id="landing-logo">Short<span>List</span></a>
                    <nav class="landing-nav">
                        ${navHtml}
                    </nav>
                </div>
            </header>

            <!-- Hero Section -->
            <section class="hero">
                <div class="hero-gradient-bg"></div>
                <div class="container">
                    <div class="hero-content reveal-hero">
                        <div class="hero-badge">Early Access to Top Roles</div>
                        <h1>Get hired <span>before</span><br>the job is posted</h1>
                        <p class="hero-subtitle">
                            ShortList connects you with Boston's best tech opportunities before they hit the job boards.
                            Be first in line for roles at companies that matter.
                        </p>
                        <div class="hero-ctas">
                            ${isLoggedIn
                                ? (isEmployer
                                    ? `<button class="btn btn-gradient" id="cta-dashboard">Go to Dashboard</button>`
                                    : `<button class="btn btn-gradient" id="cta-browse">Browse Roles</button>`)
                                : `<button class="btn btn-gradient" id="cta-get-started">Get Started</button>
                                   <button class="btn btn-outline" id="cta-hiring">I'm Hiring</button>`
                            }
                        </div>
                    </div>
                </div>
            </section>

            <!-- Why Now / Urgency Section -->
            <section class="urgency-section reveal">
                <div class="container">
                    <div class="urgency-card">
                        <div class="urgency-icon">
                            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="12" cy="12" r="10"/>
                                <polyline points="12 6 12 12 16 14"/>
                            </svg>
                        </div>
                        <div class="urgency-content">
                            <h3>Why timing matters</h3>
                            <p>Roles fill quietly through referrals and internal benches. <strong>ShortList gets you into that bench early</strong>—before positions hit the job boards, when hiring managers are still deciding who to interview.</p>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Product Preview Section -->
            <section class="product-preview-section">
                <div class="container">
                    <div class="section-header reveal">
                        <h2>See what's waiting for you</h2>
                        <p>A personalized experience for job seekers and employers alike</p>
                    </div>

                    <div class="product-preview-grid">
                        <!-- Job Seeker Preview -->
                        <div class="preview-card reveal" style="--reveal-delay: 0.1s">
                            <div class="preview-label">
                                <span class="preview-label-icon">👤</span>
                                For Job Seekers
                            </div>
                            <div class="preview-device">
                                <div class="preview-device-header">
                                    <div class="preview-device-dots">
                                        <span></span><span></span><span></span>
                                    </div>
                                    <div class="preview-device-title">For You</div>
                                </div>
                                <div class="preview-screen seeker-preview">
                                    <div class="preview-filters">
                                        <div class="preview-filter-pill active">All Roles</div>
                                        <div class="preview-filter-pill">Remote</div>
                                        <div class="preview-filter-pill">Boston</div>
                                        <div class="preview-filter-pill">Senior+</div>
                                    </div>
                                    <div class="preview-feed">
                                        <div class="preview-job-card" style="--card-delay: 0s">
                                            <div class="preview-job-header">
                                                <div class="preview-company-logo">A</div>
                                                <div class="preview-job-info">
                                                    <div class="preview-job-title">Senior Software Engineer</div>
                                                    <div class="preview-company-name">Acme Technologies</div>
                                                </div>
                                                <div class="preview-match-badge">94% Match</div>
                                            </div>
                                            <div class="preview-job-tags">
                                                <span>Remote</span><span>$180-220k</span><span>Series B</span>
                                            </div>
                                        </div>
                                        <div class="preview-job-card" style="--card-delay: 0.15s">
                                            <div class="preview-job-header">
                                                <div class="preview-company-logo" style="background: linear-gradient(135deg, #10b981, #059669)">B</div>
                                                <div class="preview-job-info">
                                                    <div class="preview-job-title">Engineering Manager</div>
                                                    <div class="preview-company-name">Beacon Health</div>
                                                </div>
                                                <div class="preview-match-badge">91% Match</div>
                                            </div>
                                            <div class="preview-job-tags">
                                                <span>Hybrid</span><span>$200-250k</span><span>Growth</span>
                                            </div>
                                        </div>
                                        <div class="preview-job-card" style="--card-delay: 0.3s">
                                            <div class="preview-job-header">
                                                <div class="preview-company-logo" style="background: linear-gradient(135deg, #8b5cf6, #6366f1)">C</div>
                                                <div class="preview-job-info">
                                                    <div class="preview-job-title">Staff Data Scientist</div>
                                                    <div class="preview-company-name">CloudStack AI</div>
                                                </div>
                                                <div class="preview-match-badge">88% Match</div>
                                            </div>
                                            <div class="preview-job-tags">
                                                <span>On-site</span><span>$190-230k</span><span>AI/ML</span>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Employer Preview -->
                        <div class="preview-card reveal" style="--reveal-delay: 0.25s">
                            <div class="preview-label">
                                <span class="preview-label-icon">🏢</span>
                                For Employers
                            </div>
                            <div class="preview-device">
                                <div class="preview-device-header">
                                    <div class="preview-device-dots">
                                        <span></span><span></span><span></span>
                                    </div>
                                    <div class="preview-device-title">Candidate Review</div>
                                </div>
                                <div class="preview-screen employer-preview">
                                    <div class="preview-candidate-card">
                                        <div class="preview-candidate-header">
                                            <div class="preview-candidate-avatar">JD</div>
                                            <div class="preview-candidate-info">
                                                <div class="preview-candidate-name">Jane Doe</div>
                                                <div class="preview-candidate-role">Senior Engineer @ TechCorp</div>
                                            </div>
                                            <div class="preview-fit-score">
                                                <div class="preview-fit-score-value">92%</div>
                                                <div class="preview-fit-score-label">Fit</div>
                                            </div>
                                        </div>

                                        <div class="preview-evidence-section">
                                            <div class="preview-evidence-header">
                                                <span class="preview-evidence-icon good">✓</span>
                                                Top Skills + Evidence
                                            </div>
                                            <div class="preview-skills-list">
                                                <div class="preview-skill-chip" style="--chip-delay: 0s">
                                                    <span class="skill-name">Python</span>
                                                    <span class="skill-years">6 yrs</span>
                                                </div>
                                                <div class="preview-skill-chip" style="--chip-delay: 0.1s">
                                                    <span class="skill-name">AWS</span>
                                                    <span class="skill-years">4 yrs</span>
                                                </div>
                                                <div class="preview-skill-chip" style="--chip-delay: 0.2s">
                                                    <span class="skill-name">React</span>
                                                    <span class="skill-years">5 yrs</span>
                                                </div>
                                                <div class="preview-skill-chip" style="--chip-delay: 0.3s">
                                                    <span class="skill-name">System Design</span>
                                                    <span class="skill-years">3 yrs</span>
                                                </div>
                                            </div>
                                        </div>

                                        <div class="preview-evidence-section gaps">
                                            <div class="preview-evidence-header">
                                                <span class="preview-evidence-icon caution">!</span>
                                                Gaps / Risks
                                            </div>
                                            <div class="preview-gaps-list">
                                                <div class="preview-gap-item" style="--gap-delay: 0s">
                                                    <span>No Kubernetes experience listed</span>
                                                </div>
                                                <div class="preview-gap-item" style="--gap-delay: 0.1s">
                                                    <span>May require management ramp-up</span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Product Walkthrough - How ShortList Works -->
            <section class="walkthrough-section">
                <div class="container">
                    <div class="section-header reveal">
                        <h2>From resume to shortlist in minutes</h2>
                        <p>See how ShortList turns your experience into opportunities</p>
                    </div>

                    <div class="walkthrough-steps">
                        <!-- Step 1: Upload Resume -->
                        <div class="walkthrough-step reveal" style="--reveal-delay: 0.1s">
                            <div class="walkthrough-step-number">1</div>
                            <div class="walkthrough-step-content">
                                <h3>Upload your resume</h3>
                                <p>Drop your PDF and watch the magic happen</p>
                            </div>
                            <div class="walkthrough-animation upload-animation">
                                <div class="upload-zone">
                                    <div class="upload-icon">
                                        <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                                            <polyline points="17 8 12 3 7 8"/>
                                            <line x1="12" y1="3" x2="12" y2="15"/>
                                        </svg>
                                    </div>
                                    <div class="upload-text">Drop resume here</div>
                                    <div class="upload-file">
                                        <div class="file-icon">📄</div>
                                        <span>resume.pdf</span>
                                    </div>
                                </div>
                                <div class="upload-progress">
                                    <div class="progress-bar">
                                        <div class="progress-fill"></div>
                                    </div>
                                    <div class="progress-text">Analyzing...</div>
                                </div>
                            </div>
                        </div>

                        <!-- Step 2: Skills Extraction -->
                        <div class="walkthrough-step reveal" style="--reveal-delay: 0.2s">
                            <div class="walkthrough-step-number">2</div>
                            <div class="walkthrough-step-content">
                                <h3>We extract your skills</h3>
                                <p>AI identifies your experience and expertise</p>
                            </div>
                            <div class="walkthrough-animation skills-animation">
                                <div class="skills-extraction">
                                    <div class="extraction-source">
                                        <div class="source-line" style="--line-delay: 0s">Led team of 8 engineers...</div>
                                        <div class="source-line" style="--line-delay: 0.2s">Built microservices in Python...</div>
                                        <div class="source-line" style="--line-delay: 0.4s">Deployed to AWS using Terraform...</div>
                                    </div>
                                    <div class="extraction-arrow">
                                        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                            <line x1="5" y1="12" x2="19" y2="12"/>
                                            <polyline points="12 5 19 12 12 19"/>
                                        </svg>
                                    </div>
                                    <div class="extraction-skills">
                                        <div class="extracted-skill" style="--skill-delay: 0.3s">
                                            <span class="skill-icon">👥</span>
                                            <span>Team Leadership</span>
                                        </div>
                                        <div class="extracted-skill" style="--skill-delay: 0.5s">
                                            <span class="skill-icon">🐍</span>
                                            <span>Python</span>
                                        </div>
                                        <div class="extracted-skill" style="--skill-delay: 0.7s">
                                            <span class="skill-icon">☁️</span>
                                            <span>AWS</span>
                                        </div>
                                        <div class="extracted-skill" style="--skill-delay: 0.9s">
                                            <span class="skill-icon">🔧</span>
                                            <span>Terraform</span>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Step 3: Match to Roles -->
                        <div class="walkthrough-step reveal" style="--reveal-delay: 0.3s">
                            <div class="walkthrough-step-number">3</div>
                            <div class="walkthrough-step-content">
                                <h3>Get matched to roles</h3>
                                <p>See your fit score for every opportunity</p>
                            </div>
                            <div class="walkthrough-animation match-animation">
                                <div class="match-visualization">
                                    <div class="match-role">
                                        <div class="match-role-title">Senior Engineer @ TechCo</div>
                                        <div class="match-bar-container">
                                            <div class="match-bar">
                                                <div class="match-bar-fill" style="--match-percent: 94%"></div>
                                            </div>
                                            <span class="match-percent">94%</span>
                                        </div>
                                    </div>
                                    <div class="match-role">
                                        <div class="match-role-title">Staff Engineer @ DataFlow</div>
                                        <div class="match-bar-container">
                                            <div class="match-bar">
                                                <div class="match-bar-fill" style="--match-percent: 89%"></div>
                                            </div>
                                            <span class="match-percent">89%</span>
                                        </div>
                                    </div>
                                    <div class="match-role">
                                        <div class="match-role-title">Engineering Lead @ BuildIt</div>
                                        <div class="match-bar-container">
                                            <div class="match-bar">
                                                <div class="match-bar-fill" style="--match-percent: 86%"></div>
                                            </div>
                                            <span class="match-percent">86%</span>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- How It Works -->
            <section class="how-it-works" id="how-it-works">
                <div class="container">
                    <h2>How It Works</h2>
                    <p>Whether you're looking for your next role or building your team, ShortList makes it simple.</p>

                    <div class="how-it-works-grid">
                        <!-- Job Seekers -->
                        <div class="how-it-works-card seeker">
                            <div class="how-it-works-card-header">
                                <div class="how-it-works-icon">🎯</div>
                                <h3>For Job Seekers<span>Find your next opportunity</span></h3>
                            </div>
                            <div class="steps-list">
                                <div class="step">
                                    <div class="step-number">1</div>
                                    <div class="step-content">
                                        <h4>Create your profile</h4>
                                        <p>Tell us about your experience level and work preferences in under a minute.</p>
                                    </div>
                                </div>
                                <div class="step">
                                    <div class="step-number">2</div>
                                    <div class="step-content">
                                        <h4>Browse exclusive roles</h4>
                                        <p>Access positions before they're publicly posted. See roles that match your profile.</p>
                                    </div>
                                </div>
                                <div class="step">
                                    <div class="step-number">3</div>
                                    <div class="step-content">
                                        <h4>Join the shortlist</h4>
                                        <p>Express interest in roles you love. Upload your resume and get on the employer's radar first.</p>
                                    </div>
                                </div>
                                <div class="step">
                                    <div class="step-number">4</div>
                                    <div class="step-content">
                                        <h4>Get contacted directly</h4>
                                        <p>Employers review the shortlist and reach out to candidates they want to interview.</p>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Employers -->
                        <div class="how-it-works-card employer">
                            <div class="how-it-works-card-header">
                                <div class="how-it-works-icon">🏢</div>
                                <h3>For Employers<span>Build your team faster</span></h3>
                            </div>
                            <div class="steps-list">
                                <div class="step">
                                    <div class="step-number">1</div>
                                    <div class="step-content">
                                        <h4>List your open roles</h4>
                                        <p>Add positions you're hiring for. They'll be visible to qualified candidates immediately.</p>
                                    </div>
                                </div>
                                <div class="step">
                                    <div class="step-number">2</div>
                                    <div class="step-content">
                                        <h4>Attract interested candidates</h4>
                                        <p>Candidates who are genuinely interested join your shortlist with their resumes ready.</p>
                                    </div>
                                </div>
                                <div class="step">
                                    <div class="step-number">3</div>
                                    <div class="step-content">
                                        <h4>Review pre-screened applicants</h4>
                                        <p>See experience levels, work preferences, and resumes. No more sorting through hundreds of unqualified applications.</p>
                                    </div>
                                </div>
                                <div class="step">
                                    <div class="step-number">4</div>
                                    <div class="step-content">
                                        <h4>Connect with top talent</h4>
                                        <p>Reach out directly to candidates you want to interview. Skip the noise and hire faster.</p>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Value Props -->
            <section class="value-props">
                <div class="container">
                    <h2>Why ShortList?</h2>
                    <div class="value-props-grid">
                        <div class="value-prop-card">
                            <div class="value-prop-icon">⚡</div>
                            <h3>Early Access</h3>
                            <p>See roles before they're posted publicly. Be first in line at companies you want to work for.</p>
                        </div>
                        <div class="value-prop-card">
                            <div class="value-prop-icon">🎯</div>
                            <h3>Quality Matches</h3>
                            <p>Pre-screened candidates and curated roles mean less noise and better connections.</p>
                        </div>
                        <div class="value-prop-card">
                            <div class="value-prop-icon">🚀</div>
                            <h3>Move Fast</h3>
                            <p>Skip the endless applications. Express interest, upload your resume, and get noticed.</p>
                        </div>
                    </div>
                </div>
            </section>

            <!-- CTA Section -->
            <section class="cta-section">
                <div class="container">
                    ${isLoggedIn
                        ? `<h2>Welcome back!</h2>
                           <p>Continue exploring opportunities on ShortList.</p>
                           <div class="hero-ctas">
                               ${isEmployer
                                   ? `<button class="btn btn-gradient" id="cta-dashboard-bottom">Go to Dashboard</button>`
                                   : `<button class="btn btn-gradient" id="cta-browse-bottom">Browse Roles</button>`
                               }
                           </div>`
                        : `<h2>Ready to get started?</h2>
                           <p>Join ShortList and find your next opportunity before everyone else.</p>
                           <div class="hero-ctas">
                               <button class="btn btn-gradient" id="cta-get-started-bottom">Get Started</button>
                               <button class="btn btn-outline" id="cta-hiring-bottom">I'm Hiring</button>
                           </div>`
                    }
                </div>
            </section>

            <!-- Footer -->
            <footer class="landing-footer">
                <div class="container">
                    <a href="#" class="logo">Short<span>List</span></a>
                    <p>&copy; ${new Date().getFullYear()} ShortList. Boston's tech job shortlist.</p>
                </div>
            </footer>
        </div>
    `;

    // Event listeners
    document.getElementById('landing-logo').addEventListener('click', (e) => {
        e.preventDefault();
        window.scrollTo({ top: 0, behavior: 'smooth' });
    });

    document.getElementById('nav-how-it-works').addEventListener('click', (e) => {
        e.preventDefault();
        document.getElementById('how-it-works').scrollIntoView({ behavior: 'smooth' });
    });

    // Nav button - depends on login state
    if (isLoggedIn) {
        if (isEmployer) {
            document.getElementById('nav-dashboard')?.addEventListener('click', (e) => {
                e.preventDefault();
                navigate('employer');
            });
        } else {
            document.getElementById('nav-browse')?.addEventListener('click', (e) => {
                e.preventDefault();
                navigate('browse');
            });
        }
    } else {
        document.getElementById('nav-login')?.addEventListener('click', (e) => {
            e.preventDefault();
            navigate('login');
        });
    }

    // CTA buttons - depend on login state
    if (isLoggedIn) {
        if (isEmployer) {
            document.getElementById('cta-dashboard')?.addEventListener('click', () => navigate('employer'));
            document.getElementById('cta-dashboard-bottom')?.addEventListener('click', () => navigate('employer'));
        } else {
            document.getElementById('cta-browse')?.addEventListener('click', () => navigate('browse'));
            document.getElementById('cta-browse-bottom')?.addEventListener('click', () => navigate('browse'));
        }
    } else {
        // Get Started buttons - go to seeker signup
        document.getElementById('cta-get-started')?.addEventListener('click', () => navigate('signup'));
        document.getElementById('cta-get-started-bottom')?.addEventListener('click', () => navigate('signup'));

        // I'm Hiring buttons - go to employer signup
        document.getElementById('cta-hiring')?.addEventListener('click', () => navigateEmployerSignup());
        document.getElementById('cta-hiring-bottom')?.addEventListener('click', () => navigateEmployerSignup());
    }
}

function navigateEmployerSignup() {
    // Set a flag to indicate employer signup, then navigate
    state.signupAsEmployer = true;
    navigate('signup');
}

function renderLogin() {
    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="auth-page">
            <div class="auth-box">
                <a href="#" id="back-to-home" style="display:inline-block;margin-bottom:20px;color:#525252;text-decoration:none;font-size:13px;">← Back to home</a>
                <h1>Sign in to ShortList</h1>
                <p>Boston's tech job shortlist</p>
                <div id="auth-error"></div>
                <form id="login-form">
                    <div class="form-group">
                        <label>Email</label>
                        <input type="email" id="login-email" required>
                    </div>
                    <div class="form-group">
                        <label>Password</label>
                        <input type="password" id="login-password" required>
                    </div>
                    <button type="submit" class="btn btn-primary">Sign In</button>
                </form>
                <div class="auth-switch">
                    Don't have an account? <a href="#" id="goto-signup">Sign up</a>
                </div>
            </div>
        </div>
    `;

    document.getElementById('back-to-home').addEventListener('click', (e) => {
        e.preventDefault();
        navigate('home');
    });

    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const email = document.getElementById('login-email').value;
        const password = document.getElementById('login-password').value;

        try {
            await login(email, password);
        } catch (err) {
            const errorEl = document.getElementById('auth-error');
            if (errorEl) {
                errorEl.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
            }
        }
    });

    document.getElementById('goto-signup').addEventListener('click', (e) => {
        e.preventDefault();
        navigate('signup');
    });
}

async function renderSignup() {
    const isEmployer = state.signupAsEmployer === true;
    const app = document.getElementById('app');

    // For employer signup, use autocomplete text input
    let companiesHtml = '';
    if (isEmployer) {
        companiesHtml = `
            <div class="form-group company-autocomplete-group">
                <label>Company</label>
                <div class="autocomplete-wrapper">
                    <input type="text" id="signup-company" required placeholder="Start typing your company name..." autocomplete="off">
                    <div id="company-suggestions" class="autocomplete-suggestions"></div>
                </div>
                <div class="form-hint" style="font-size:12px;color:#737373;margin-top:4px;">We'll match you with companies we have roles for</div>
            </div>
        `;
    }

    app.innerHTML = `
        <div class="auth-page">
            <div class="auth-box">
                <a href="#" id="back-to-home" style="display:inline-block;margin-bottom:20px;color:#525252;text-decoration:none;font-size:13px;">← Back to home</a>
                <h1>${isEmployer ? 'Create an employer account' : 'Create your account'}</h1>
                <p>${isEmployer ? 'Start building your shortlist of candidates' : 'Join ShortList to find your next role'}</p>
                <div id="auth-error"></div>
                <form id="signup-form">
                    <div class="form-group">
                        <label>Full Name</label>
                        <input type="text" id="signup-name" required placeholder="John Smith">
                    </div>
                    ${companiesHtml}
                    <div class="form-group">
                        <label>${isEmployer ? 'Company Email' : 'Email'}</label>
                        <input type="email" id="signup-email" required ${isEmployer ? 'placeholder="you@company.com"' : ''}>
                        ${isEmployer ? '<div class="form-hint" style="font-size:12px;color:#737373;margin-top:4px;">Use your work email address</div>' : ''}
                    </div>
                    <div class="form-group">
                        <label>Password</label>
                        <input type="password" id="signup-password" required minlength="6">
                        <div class="form-hint" style="font-size:12px;color:#737373;margin-top:4px;">At least 6 characters</div>
                    </div>
                    <button type="submit" class="btn btn-primary">Create Account</button>
                </form>
                <div class="auth-switch">
                    Already have an account? <a href="#" id="goto-login">Sign in</a>
                </div>
                ${!isEmployer ? `<div class="auth-switch" style="margin-top:8px;">
                    Looking to hire? <a href="#" id="goto-employer-signup">Create an employer account</a>
                </div>` : `<div class="auth-switch" style="margin-top:8px;">
                    Looking for a job? <a href="#" id="goto-seeker-signup">Create a job seeker account</a>
                </div>`}
            </div>
        </div>
    `;

    document.getElementById('back-to-home').addEventListener('click', (e) => {
        e.preventDefault();
        state.signupAsEmployer = false;
        navigate('home');
    });

    // Setup company autocomplete for employer signup
    if (isEmployer) {
        const companyInput = document.getElementById('signup-company');
        const suggestionsDiv = document.getElementById('company-suggestions');
        let debounceTimer = null;

        companyInput.addEventListener('input', () => {
            clearTimeout(debounceTimer);
            const query = companyInput.value.trim();

            if (query.length < 2) {
                suggestionsDiv.innerHTML = '';
                suggestionsDiv.style.display = 'none';
                return;
            }

            debounceTimer = setTimeout(async () => {
                try {
                    const data = await api(`/companies?search=${encodeURIComponent(query)}`);
                    const companies = data.companies || [];

                    if (companies.length === 0) {
                        suggestionsDiv.innerHTML = '';
                        suggestionsDiv.style.display = 'none';
                        return;
                    }

                    suggestionsDiv.innerHTML = companies.map(c =>
                        `<div class="autocomplete-item" data-value="${escapeHtml(c)}">${escapeHtml(c)}</div>`
                    ).join('');
                    suggestionsDiv.style.display = 'block';

                    // Add click handlers to suggestions
                    suggestionsDiv.querySelectorAll('.autocomplete-item').forEach(item => {
                        item.addEventListener('click', () => {
                            companyInput.value = item.dataset.value;
                            suggestionsDiv.innerHTML = '';
                            suggestionsDiv.style.display = 'none';
                        });
                    });
                } catch (err) {
                    console.error('Failed to fetch companies:', err);
                }
            }, 200);
        });

        // Hide suggestions when clicking outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.company-autocomplete-group')) {
                suggestionsDiv.style.display = 'none';
            }
        });

        // Handle keyboard navigation
        companyInput.addEventListener('keydown', (e) => {
            const items = suggestionsDiv.querySelectorAll('.autocomplete-item');
            const activeItem = suggestionsDiv.querySelector('.autocomplete-item.active');

            if (e.key === 'ArrowDown') {
                e.preventDefault();
                if (!activeItem && items.length > 0) {
                    items[0].classList.add('active');
                } else if (activeItem && activeItem.nextElementSibling) {
                    activeItem.classList.remove('active');
                    activeItem.nextElementSibling.classList.add('active');
                }
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                if (activeItem && activeItem.previousElementSibling) {
                    activeItem.classList.remove('active');
                    activeItem.previousElementSibling.classList.add('active');
                }
            } else if (e.key === 'Enter' && activeItem) {
                e.preventDefault();
                companyInput.value = activeItem.dataset.value;
                suggestionsDiv.innerHTML = '';
                suggestionsDiv.style.display = 'none';
            } else if (e.key === 'Escape') {
                suggestionsDiv.style.display = 'none';
            }
        });
    }

    document.getElementById('signup-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const fullName = document.getElementById('signup-name').value;
        const email = document.getElementById('signup-email').value;
        const password = document.getElementById('signup-password').value;
        const userType = isEmployer ? 'employer' : 'seeker';

        // Get company for employer
        let company = null;
        if (isEmployer) {
            const companyInput = document.getElementById('signup-company');
            company = companyInput.value.trim();

            if (!company) {
                document.getElementById('auth-error').innerHTML =
                    '<div class="alert alert-error">Please enter your company name</div>';
                return;
            }
        }

        try {
            await signup(fullName, email, password, userType, company);
            state.signupAsEmployer = false; // Reset the flag
        } catch (err) {
            document.getElementById('auth-error').innerHTML =
                `<div class="alert alert-error">${err.message}</div>`;
        }
    });

    document.getElementById('goto-login').addEventListener('click', (e) => {
        e.preventDefault();
        state.signupAsEmployer = false;
        navigate('login');
    });

    // Toggle between employer and seeker signup
    const employerSignupLink = document.getElementById('goto-employer-signup');
    const seekerSignupLink = document.getElementById('goto-seeker-signup');

    if (employerSignupLink) {
        employerSignupLink.addEventListener('click', (e) => {
            e.preventDefault();
            state.signupAsEmployer = true;
            navigate('signup');
        });
    }

    if (seekerSignupLink) {
        seekerSignupLink.addEventListener('click', (e) => {
            e.preventDefault();
            state.signupAsEmployer = false;
            navigate('signup');
        });
    }
}

function renderSetup() {
    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="preferences-page">
            <div class="preferences-container">
                <div class="preferences-header">
                    <h1>What are you looking for?</h1>
                    <p>Help us match you with the perfect roles. All fields are optional.</p>
                </div>
                <div id="setup-error"></div>
                <form id="preferences-form">
                    <!-- Location -->
                    <div class="pref-section">
                        <label class="pref-label">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"></path>
                                <circle cx="12" cy="10" r="3"></circle>
                            </svg>
                            Preferred Location
                        </label>
                        <div class="location-autocomplete">
                            <input type="text" id="pref-location-input" placeholder="Type a city..." autocomplete="off">
                            <div id="location-suggestions" class="autocomplete-suggestions"></div>
                        </div>
                        <div id="selected-locations" class="selected-tags"></div>
                    </div>

                    <!-- Salary Range -->
                    <div class="pref-section">
                        <label class="pref-label">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <line x1="12" y1="1" x2="12" y2="23"></line>
                                <path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"></path>
                            </svg>
                            Salary Expectation
                        </label>
                        <div class="salary-range-container">
                            <div class="salary-inputs">
                                <div class="salary-input-group">
                                    <span class="salary-prefix">$</span>
                                    <input type="text" id="pref-salary-min" placeholder="80,000" class="salary-input">
                                </div>
                                <span class="salary-separator">to</span>
                                <div class="salary-input-group">
                                    <span class="salary-prefix">$</span>
                                    <input type="text" id="pref-salary-max" placeholder="150,000" class="salary-input">
                                </div>
                            </div>
                            <div class="salary-presets">
                                <button type="button" class="salary-preset" data-min="50000" data-max="80000">$50k-$80k</button>
                                <button type="button" class="salary-preset" data-min="80000" data-max="120000">$80k-$120k</button>
                                <button type="button" class="salary-preset" data-min="120000" data-max="180000">$120k-$180k</button>
                                <button type="button" class="salary-preset" data-min="180000" data-max="300000">$180k+</button>
                            </div>
                        </div>
                    </div>

                    <!-- Role Type -->
                    <div class="pref-section">
                        <label class="pref-label">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <rect x="2" y="7" width="20" height="14" rx="2" ry="2"></rect>
                                <path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"></path>
                            </svg>
                            Role Type
                            <span class="pref-hint">Select all that apply</span>
                        </label>
                        <div class="role-type-grid" id="role-type-options">
                            <button type="button" class="role-type-btn" data-value="software_engineer">
                                <span class="role-icon">💻</span>
                                Software Engineer
                            </button>
                            <button type="button" class="role-type-btn" data-value="data_scientist">
                                <span class="role-icon">🔬</span>
                                Data Science
                            </button>
                            <button type="button" class="role-type-btn" data-value="data_analyst">
                                <span class="role-icon">📊</span>
                                Data Analyst
                            </button>
                            <button type="button" class="role-type-btn" data-value="product_manager">
                                <span class="role-icon">📋</span>
                                Product Manager
                            </button>
                            <button type="button" class="role-type-btn" data-value="engineering_manager">
                                <span class="role-icon">👥</span>
                                Engineering Manager
                            </button>
                            <button type="button" class="role-type-btn" data-value="sales">
                                <span class="role-icon">💼</span>
                                Sales
                            </button>
                            <button type="button" class="role-type-btn" data-value="marketing">
                                <span class="role-icon">📣</span>
                                Marketing
                            </button>
                            <button type="button" class="role-type-btn" data-value="design">
                                <span class="role-icon">🎨</span>
                                Design
                            </button>
                            <button type="button" class="role-type-btn" data-value="operations">
                                <span class="role-icon">⚙️</span>
                                Operations
                            </button>
                            <button type="button" class="role-type-btn" data-value="finance">
                                <span class="role-icon">💰</span>
                                Finance
                            </button>
                            <button type="button" class="role-type-btn" data-value="hr">
                                <span class="role-icon">🤝</span>
                                HR / People
                            </button>
                            <button type="button" class="role-type-btn" data-value="support">
                                <span class="role-icon">🎧</span>
                                Customer Support
                            </button>
                        </div>
                    </div>

                    <!-- Experience Level -->
                    <div class="pref-section">
                        <label class="pref-label">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
                                <polyline points="22 4 12 14.01 9 11.01"></polyline>
                            </svg>
                            Experience Level
                        </label>
                        <div class="exp-level-options" id="exp-level-options">
                            <button type="button" class="exp-btn" data-value="intern">
                                <span class="exp-title">Intern</span>
                                <span class="exp-desc">Student or recent grad</span>
                            </button>
                            <button type="button" class="exp-btn" data-value="entry">
                                <span class="exp-title">Entry Level</span>
                                <span class="exp-desc">0-2 years</span>
                            </button>
                            <button type="button" class="exp-btn" data-value="mid">
                                <span class="exp-title">Mid Level</span>
                                <span class="exp-desc">3-5 years</span>
                            </button>
                            <button type="button" class="exp-btn" data-value="senior">
                                <span class="exp-title">Senior</span>
                                <span class="exp-desc">6+ years</span>
                            </button>
                        </div>
                    </div>

                    <!-- Work Arrangement -->
                    <div class="pref-section">
                        <label class="pref-label">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path>
                                <polyline points="9 22 9 12 15 12 15 22"></polyline>
                            </svg>
                            Work Arrangement
                        </label>
                        <div class="work-arrangement-options" id="work-arrangement-options">
                            <button type="button" class="work-btn" data-value="remote">
                                <span class="work-icon">🏠</span>
                                <span class="work-title">Remote</span>
                            </button>
                            <button type="button" class="work-btn" data-value="hybrid">
                                <span class="work-icon">🔄</span>
                                <span class="work-title">Hybrid</span>
                            </button>
                            <button type="button" class="work-btn" data-value="onsite">
                                <span class="work-icon">🏢</span>
                                <span class="work-title">On-site</span>
                            </button>
                        </div>
                    </div>

                    <div class="pref-actions">
                        <button type="submit" class="btn btn-primary" style="width:100%;">Continue</button>
                    </div>
                </form>
            </div>
        </div>
    `;

    // State for selected values
    const selectedLocations = [];
    const selectedRoles = [];
    let selectedExpLevel = null;
    let selectedWorkArrangement = null;

    // Location autocomplete
    const locationInput = document.getElementById('pref-location-input');
    const locationSuggestions = document.getElementById('location-suggestions');
    const selectedLocationsDiv = document.getElementById('selected-locations');
    let locationDebounce = null;

    function renderSelectedLocations() {
        selectedLocationsDiv.innerHTML = selectedLocations.map(loc =>
            `<span class="selected-tag">${escapeHtml(loc)} <button type="button" class="tag-remove" data-loc="${escapeHtml(loc)}">&times;</button></span>`
        ).join('');

        selectedLocationsDiv.querySelectorAll('.tag-remove').forEach(btn => {
            btn.addEventListener('click', () => {
                const loc = btn.dataset.loc;
                const idx = selectedLocations.indexOf(loc);
                if (idx > -1) selectedLocations.splice(idx, 1);
                renderSelectedLocations();
            });
        });
    }

    locationInput.addEventListener('input', () => {
        clearTimeout(locationDebounce);
        const query = locationInput.value.trim();

        if (query.length < 2) {
            locationSuggestions.style.display = 'none';
            return;
        }

        locationDebounce = setTimeout(async () => {
            try {
                const data = await api(`/locations?q=${encodeURIComponent(query)}`);
                const locations = (data.locations || []).filter(l => !selectedLocations.includes(l));

                if (locations.length === 0) {
                    locationSuggestions.style.display = 'none';
                    return;
                }

                locationSuggestions.innerHTML = locations.map(l =>
                    `<div class="autocomplete-item" data-value="${escapeHtml(l)}">${escapeHtml(l)}</div>`
                ).join('');
                locationSuggestions.style.display = 'block';

                locationSuggestions.querySelectorAll('.autocomplete-item').forEach(item => {
                    item.addEventListener('click', () => {
                        selectedLocations.push(item.dataset.value);
                        renderSelectedLocations();
                        locationInput.value = '';
                        locationSuggestions.style.display = 'none';
                    });
                });
            } catch (err) {
                console.error('Failed to fetch locations:', err);
            }
        }, 200);
    });

    document.addEventListener('click', (e) => {
        if (!e.target.closest('.location-autocomplete')) {
            locationSuggestions.style.display = 'none';
        }
    });

    // Salary formatting and presets
    const salaryMinInput = document.getElementById('pref-salary-min');
    const salaryMaxInput = document.getElementById('pref-salary-max');

    function formatSalaryInput(input) {
        let value = input.value.replace(/[^0-9]/g, '');
        if (value) {
            value = parseInt(value).toLocaleString();
        }
        input.value = value;
    }

    salaryMinInput.addEventListener('input', () => formatSalaryInput(salaryMinInput));
    salaryMaxInput.addEventListener('input', () => formatSalaryInput(salaryMaxInput));

    document.querySelectorAll('.salary-preset').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.salary-preset').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
            salaryMinInput.value = parseInt(btn.dataset.min).toLocaleString();
            salaryMaxInput.value = parseInt(btn.dataset.max).toLocaleString();
        });
    });

    // Role type selection (multi-select)
    document.querySelectorAll('#role-type-options .role-type-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            btn.classList.toggle('selected');
            const value = btn.dataset.value;
            const idx = selectedRoles.indexOf(value);
            if (idx > -1) {
                selectedRoles.splice(idx, 1);
            } else {
                selectedRoles.push(value);
            }
        });
    });

    // Experience level selection (single select)
    document.querySelectorAll('#exp-level-options .exp-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#exp-level-options .exp-btn').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
            selectedExpLevel = btn.dataset.value;
        });
    });

    // Work arrangement selection (single select)
    document.querySelectorAll('#work-arrangement-options .work-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#work-arrangement-options .work-btn').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
            selectedWorkArrangement = btn.dataset.value;
        });
    });

    // Form submit
    document.getElementById('preferences-form').addEventListener('submit', async (e) => {
        e.preventDefault();

        const salaryMin = salaryMinInput.value ? parseInt(salaryMinInput.value.replace(/[^0-9]/g, '')) : null;
        const salaryMax = salaryMaxInput.value ? parseInt(salaryMaxInput.value.replace(/[^0-9]/g, '')) : null;

        const preferences = {
            preferred_locations: selectedLocations.length > 0 ? selectedLocations : null,
            salary_min: salaryMin,
            salary_max: salaryMax,
            open_to_roles: selectedRoles.length > 0 ? selectedRoles : null,
            experience_level: selectedExpLevel,
            work_arrangement: selectedWorkArrangement
        };

        try {
            await savePreferences(preferences);
        } catch (err) {
            document.getElementById('setup-error').innerHTML =
                `<div class="alert alert-error">${err.message}</div>`;
        }
    });
}

function renderResumeUpload() {
    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="preferences-page">
            <div class="preferences-container resume-upload-container">
                <div class="preferences-header">
                    <h1>Get smarter recommendations</h1>
                    <p>Upload your resume or CV and we'll match your skills to the best opportunities.</p>
                </div>
                <div id="upload-error"></div>

                <!-- Initial upload dropzone -->
                <div class="resume-upload-section" id="upload-dropzone-section">
                    <div class="upload-dropzone" id="resume-dropzone">
                        <div class="dropzone-content">
                            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                                <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"></path>
                                <polyline points="14 2 14 8 20 8"></polyline>
                                <line x1="12" y1="18" x2="12" y2="12"></line>
                                <line x1="9" y1="15" x2="15" y2="15"></line>
                            </svg>
                            <p class="dropzone-text">Drag and drop your resume here</p>
                            <p class="dropzone-subtext">or click to browse (PDF only)</p>
                        </div>
                        <input type="file" id="resume-file-input" accept=".pdf" style="display: none;">
                    </div>
                </div>

                <!-- Step-based progress indicator -->
                <div id="upload-progress-section" class="upload-progress-section" style="display: none;">
                    <div class="progress-steps">
                        <div class="progress-step" id="step-upload">
                            <div class="step-icon">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                                    <polyline points="17 8 12 3 7 8"/>
                                    <line x1="12" y1="3" x2="12" y2="15"/>
                                </svg>
                            </div>
                            <div class="step-content">
                                <div class="step-label">Uploading</div>
                                <div class="step-desc">Sending your resume</div>
                            </div>
                            <div class="step-status">
                                <div class="step-spinner"></div>
                                <svg class="step-check" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                                    <polyline points="20 6 9 17 4 12"/>
                                </svg>
                            </div>
                        </div>
                        <div class="progress-step" id="step-extract">
                            <div class="step-icon">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <circle cx="12" cy="12" r="10"/>
                                    <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/>
                                    <line x1="12" y1="17" x2="12.01" y2="17"/>
                                </svg>
                            </div>
                            <div class="step-content">
                                <div class="step-label">Extracting skills</div>
                                <div class="step-desc">Analyzing your experience</div>
                            </div>
                            <div class="step-status">
                                <div class="step-spinner"></div>
                                <svg class="step-check" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                                    <polyline points="20 6 9 17 4 12"/>
                                </svg>
                            </div>
                        </div>
                        <div class="progress-step" id="step-match">
                            <div class="step-icon">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M12 2L2 7l10 5 10-5-10-5z"/>
                                    <path d="M2 17l10 5 10-5"/>
                                    <path d="M2 12l10 5 10-5"/>
                                </svg>
                            </div>
                            <div class="step-content">
                                <div class="step-label">Building matches</div>
                                <div class="step-desc">Finding your best opportunities</div>
                            </div>
                            <div class="step-status">
                                <div class="step-spinner"></div>
                                <svg class="step-check" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                                    <polyline points="20 6 9 17 4 12"/>
                                </svg>
                            </div>
                        </div>
                    </div>

                    <!-- Progress bar -->
                    <div class="upload-progress-bar-container">
                        <div class="upload-progress-bar">
                            <div class="upload-progress-fill" id="upload-progress-fill"></div>
                        </div>
                        <div class="upload-progress-percent" id="upload-progress-percent">0%</div>
                    </div>

                    <!-- Skills preview (shown during extraction) -->
                    <div id="skills-preview" class="skills-preview" style="display: none;">
                        <div class="skills-preview-label">Skills detected:</div>
                        <div class="skills-preview-chips" id="skills-chips"></div>
                    </div>
                </div>

                <!-- Success state -->
                <div id="upload-success" class="upload-success-enhanced" style="display: none;">
                    <div class="success-icon-large">
                        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
                            <polyline points="22 4 12 14.01 9 11.01"></polyline>
                        </svg>
                    </div>
                    <h3>You're all set!</h3>
                    <p id="success-message">We found <span id="match-count">0</span> jobs that match your profile</p>
                    <div class="top-matches-preview" id="top-matches-preview"></div>
                </div>

                <div class="pref-actions" style="margin-top: 24px;">
                    <button type="button" class="btn btn-primary" id="continue-btn" style="width: 100%;">Continue</button>
                    <button type="button" class="btn btn-link" id="skip-btn" style="width: 100%; margin-top: 12px;">Skip for now</button>
                </div>
            </div>
        </div>
    `;

    const dropzoneSection = document.getElementById('upload-dropzone-section');
    const dropzone = document.getElementById('resume-dropzone');
    const fileInput = document.getElementById('resume-file-input');
    const progressSection = document.getElementById('upload-progress-section');
    const progressFill = document.getElementById('upload-progress-fill');
    const progressPercent = document.getElementById('upload-progress-percent');
    const uploadSuccess = document.getElementById('upload-success');
    const continueBtn = document.getElementById('continue-btn');
    const skipBtn = document.getElementById('skip-btn');

    let uploadComplete = false;
    let matchesPreloaded = false;

    // Step elements
    const stepUpload = document.getElementById('step-upload');
    const stepExtract = document.getElementById('step-extract');
    const stepMatch = document.getElementById('step-match');

    function setStepState(stepEl, state) {
        stepEl.classList.remove('pending', 'active', 'complete');
        stepEl.classList.add(state);
    }

    function updateProgress(percent) {
        progressFill.style.width = `${percent}%`;
        progressPercent.textContent = `${Math.round(percent)}%`;
    }

    function animateProgress(from, to, duration) {
        const startTime = performance.now();
        function update(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const easeOut = 1 - Math.pow(1 - progress, 3);
            const current = from + (to - from) * easeOut;
            updateProgress(current);
            if (progress < 1) {
                requestAnimationFrame(update);
            }
        }
        requestAnimationFrame(update);
    }

    function showSkillsPreview(skills) {
        const skillsPreview = document.getElementById('skills-preview');
        const skillsChips = document.getElementById('skills-chips');

        if (!skills || skills.length === 0) return;

        // Show top 6 skills with staggered animation
        const topSkills = skills.slice(0, 6);
        skillsChips.innerHTML = topSkills.map((skill, i) =>
            `<span class="skill-chip-preview" style="animation-delay: ${i * 100}ms">${escapeHtml(skill)}</span>`
        ).join('');

        skillsPreview.style.display = 'block';
    }

    function showTopMatchesPreview() {
        const previewContainer = document.getElementById('top-matches-preview');
        const matchCountEl = document.getElementById('match-count');

        if (state.forYouJobs && state.forYouJobs.length > 0) {
            matchCountEl.textContent = state.forYouJobs.length;

            // Show top 3 matches
            const topMatches = state.forYouJobs.slice(0, 3);
            previewContainer.innerHTML = topMatches.map(job => `
                <div class="match-preview-card">
                    <div class="match-preview-score">${Math.round(job.match_score)}%</div>
                    <div class="match-preview-info">
                        <div class="match-preview-title">${escapeHtml(job.title)}</div>
                        <div class="match-preview-company">${escapeHtml(job.company_name)}</div>
                    </div>
                </div>
            `).join('');
        } else {
            matchCountEl.textContent = 'several';
            previewContainer.innerHTML = '';
        }
    }

    // Click to upload
    dropzone.addEventListener('click', () => fileInput.click());

    // Drag and drop handlers
    dropzone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropzone.classList.add('dragover');
    });

    dropzone.addEventListener('dragleave', () => {
        dropzone.classList.remove('dragover');
    });

    dropzone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropzone.classList.remove('dragover');
        const files = e.dataTransfer.files;
        if (files.length > 0) {
            handleFileUpload(files[0]);
        }
    });

    // File input change
    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length > 0) {
            handleFileUpload(e.target.files[0]);
        }
    });

    async function handleFileUpload(file) {
        if (!file.name.toLowerCase().endsWith('.pdf')) {
            document.getElementById('upload-error').innerHTML =
                '<div class="alert alert-error">Please upload a PDF file</div>';
            return;
        }

        // Switch to progress view
        dropzoneSection.style.display = 'none';
        progressSection.style.display = 'block';
        skipBtn.style.display = 'none';
        continueBtn.disabled = true;
        continueBtn.textContent = 'Processing...';

        // Step 1: Uploading
        setStepState(stepUpload, 'active');
        setStepState(stepExtract, 'pending');
        setStepState(stepMatch, 'pending');
        animateProgress(0, 20, 500);

        const formData = new FormData();
        formData.append('file', file);

        try {
            // Animate to 30% while uploading
            await new Promise(r => setTimeout(r, 300));
            animateProgress(20, 35, 800);

            // Step 2: Extracting skills (upload completes, backend processes)
            setStepState(stepUpload, 'complete');
            setStepState(stepExtract, 'active');
            animateProgress(35, 50, 500);

            const response = await fetch(`${API_BASE}/profile/upload-resume`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${state.token}`
                },
                body: formData
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Upload failed');
            }

            // Update user state
            if (state.user) {
                state.user.has_resume = true;
            }

            // Show skills if returned
            animateProgress(50, 65, 400);
            if (data.skills && data.skills.length > 0) {
                showSkillsPreview(data.skills);
            }

            await new Promise(r => setTimeout(r, 600));
            setStepState(stepExtract, 'complete');

            // Step 3: Building matches
            setStepState(stepMatch, 'active');
            animateProgress(65, 80, 500);

            // Mark profile complete and pre-load recommendations
            await api('/profile/preferences', {
                method: 'PUT',
                body: JSON.stringify({ profile_complete: true })
            });
            state.user.profile_complete = true;

            // Pre-load For You jobs while user sees progress
            animateProgress(80, 90, 800);

            try {
                const forYouData = await api('/for-you?limit=100&min_score=60');
                state.forYouJobs = forYouData.jobs || [];
                state.forYouLoaded = true;
                matchesPreloaded = true;
            } catch (err) {
                console.log('Could not pre-load matches:', err.message);
                state.forYouJobs = [];
                state.forYouLoaded = true;
            }

            // Complete
            animateProgress(90, 100, 400);
            await new Promise(r => setTimeout(r, 500));
            setStepState(stepMatch, 'complete');

            // Show success state
            uploadComplete = true;
            progressSection.style.display = 'none';
            uploadSuccess.style.display = 'block';
            showTopMatchesPreview();

            continueBtn.disabled = false;
            continueBtn.textContent = 'See your matches';
            continueBtn.classList.add('btn-gradient');

        } catch (err) {
            progressSection.style.display = 'none';
            dropzoneSection.style.display = 'block';
            skipBtn.style.display = 'block';
            continueBtn.disabled = false;
            continueBtn.textContent = 'Continue';
            document.getElementById('upload-error').innerHTML =
                `<div class="alert alert-error">${err.message}</div>`;
        }
    }

    // Continue button
    continueBtn.addEventListener('click', async () => {
        if (!uploadComplete) {
            // If no upload, mark profile complete and go to explore
            try {
                await api('/profile/preferences', {
                    method: 'PUT',
                    body: JSON.stringify({ profile_complete: true })
                });
                state.user.profile_complete = true;
            } catch (err) {
                console.error('Failed to mark profile complete:', err);
            }
            navigate('explore');
            return;
        }

        // If matches were preloaded, navigation should feel instant
        if (matchesPreloaded && state.forYouJobs.length > 0) {
            navigate('for-you');
        } else {
            // Reset and let For You page load fresh
            state.recommendationsLoaded = false;
            state.recommendations = [];
            state.forYouLoaded = false;
            state.forYouJobs = [];
            navigate('for-you');
        }
    });

    // Skip button
    skipBtn.addEventListener('click', async () => {
        try {
            await api('/profile/preferences', {
                method: 'PUT',
                body: JSON.stringify({ profile_complete: true })
            });
            state.user.profile_complete = true;
        } catch (err) {
            console.error('Failed to mark profile complete:', err);
        }
        navigate('explore');
    });
}

// Legacy function - redirects to renderExplore
function renderBrowse() {
    renderExplore();
}

function renderForYou() {
    const app = document.getElementById('app');

    // Determine initial content for the list
    let listContent = '';

    if (state.forYouLoaded) {
        // Data already cached - render jobs immediately inline
        listContent = getForYouListContent();
    } else {
        // Show skeleton while loading
        listContent = `
            <div class="for-you-header" style="grid-column: 1/-1; margin-bottom: 16px;">
                <div class="skeleton skeleton-text" style="width: 180px; height: 20px;"></div>
            </div>
            ${[1, 2, 3, 4, 5, 6].map(i => `
                <div class="role-card skeleton-card" style="animation-delay: ${i * 50}ms">
                    <div class="role-card-header">
                        <div class="skeleton skeleton-avatar"></div>
                        <div class="skeleton skeleton-badge" style="width: 60px; height: 32px;"></div>
                    </div>
                    <div class="role-card-body">
                        <div class="skeleton skeleton-text" style="width: 80%; height: 20px; margin-bottom: 8px;"></div>
                        <div class="skeleton skeleton-text" style="width: 50%; height: 16px;"></div>
                    </div>
                    <div class="role-card-footer">
                        <div class="skeleton skeleton-text" style="width: 100px; height: 24px;"></div>
                    </div>
                </div>
            `).join('')}
        `;
    }

    app.innerHTML = `
        <div class="browse-layout">
            ${renderHeader()}
            <div class="browse-hero for-you-hero">
                <div class="container">
                    <h1>For You</h1>
                    <p>Personalized job matches based on your resume and preferences</p>
                </div>
            </div>
            <div class="browse-content">
                <div class="container">
                    <div id="for-you-list" class="roles-grid">
                        ${listContent}
                    </div>
                </div>
            </div>
        </div>
    `;

    setupNavListeners();

    // Setup click listeners for job cards if already loaded
    if (state.forYouLoaded) {
        setupForYouListeners();
    }
}

// Helper to generate For You list content HTML
function getForYouListContent() {
    if (!state.forYouJobs.length) {
        return `
            <div class="empty-state" style="grid-column: 1/-1;">
                <div class="empty-icon">
                    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                        <circle cx="11" cy="11" r="8"></circle>
                        <path d="M21 21l-4.35-4.35"></path>
                    </svg>
                </div>
                <h3>No strong matches found</h3>
                <p>We couldn't find jobs with 60%+ match. Try exploring all roles or update your preferences.</p>
                <button class="btn btn-primary" style="margin-top:16px;" id="go-explore-btn">Explore All Jobs</button>
            </div>
        `;
    }

    return `
        <div class="for-you-header" style="grid-column: 1/-1; margin-bottom: 16px;">
            <div class="for-you-count">${state.forYouJobs.length} jobs with 60%+ match</div>
        </div>
        ${state.forYouJobs.map(job => {
            const scoreClass = job.match_score >= 85 ? 'high' : job.match_score >= 75 ? 'medium' : 'low';
            return `
                <div class="role-card for-you-card" data-role-id="${job.id}">
                    <div class="role-card-header">
                        <div class="company-badge">${escapeHtml(job.company_name?.charAt(0) || 'C')}</div>
                        <div class="role-header-right">
                            <div class="match-score ${scoreClass}">
                                <span class="match-percent">${Math.round(job.match_score)}%</span>
                                <span class="match-label">match</span>
                            </div>
                        </div>
                    </div>
                    <div class="role-card-body">
                        <h3 class="role-title">${escapeHtml(job.title)}</h3>
                        <div class="role-company">${escapeHtml(job.company_name)}</div>
                        ${job.match_reason ? `<div class="match-reason">${escapeHtml(job.match_reason)}</div>` : ''}
                    </div>
                    <div class="role-card-footer">
                        <div class="role-tags">
                            <span class="role-tag">${escapeHtml(job.location || 'Boston Area')}</span>
                            ${job.salary_range ? `<span class="role-tag salary">${escapeHtml(job.salary_range)}</span>` : ''}
                        </div>
                        <button class="view-role-btn" data-role-id="${job.id}">
                            <span>View</span>
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M5 12h14M12 5l7 7-7 7"/>
                            </svg>
                        </button>
                    </div>
                </div>
            `;
        }).join('')}
    `;
}

// Setup click listeners for For You job cards
function setupForYouListeners() {
    const container = document.getElementById('for-you-list');
    if (!container) return;

    // Handle empty state explore button
    document.getElementById('go-explore-btn')?.addEventListener('click', () => navigate('explore'));

    // Add click listeners for job cards
    container.querySelectorAll('.role-card').forEach(card => {
        card.addEventListener('click', (e) => {
            if (!e.target.classList.contains('view-role-btn')) {
                const roleId = parseInt(card.dataset.roleId);
                const job = state.forYouJobs.find(j => j.id === roleId);
                const matchScore = job ? Math.round(job.match_score) : null;
                state.previousPage = 'for-you';
                loadRole(roleId, matchScore);
                state.currentPage = 'role';
            }
        });
    });

    container.querySelectorAll('.view-role-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const roleId = parseInt(btn.dataset.roleId);
            const job = state.forYouJobs.find(j => j.id === roleId);
            const matchScore = job ? Math.round(job.match_score) : null;
            state.previousPage = 'for-you';
            loadRole(roleId, matchScore);
            state.currentPage = 'role';
        });
    });
}

function renderForYouList() {
    const container = document.getElementById('for-you-list');
    if (!container) return;

    container.innerHTML = getForYouListContent();
    setupForYouListeners();
}

function renderForYouNeedsResume() {
    const container = document.getElementById('for-you-list');
    if (!container) return;

    container.innerHTML = `
        <div class="empty-state" style="grid-column: 1/-1;">
            <div class="empty-icon">
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                    <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"></path>
                    <polyline points="14 2 14 8 20 8"></polyline>
                    <line x1="12" y1="18" x2="12" y2="12"></line>
                    <line x1="9" y1="15" x2="15" y2="15"></line>
                </svg>
            </div>
            <h3>Upload your resume</h3>
            <p>We need your resume to find personalized job matches for you.</p>
            <button class="btn btn-primary" style="margin-top:16px;" id="upload-resume-btn">Upload Resume</button>
            <button class="btn btn-secondary" style="margin-top:8px;" id="go-explore-btn">Explore Jobs Instead</button>
        </div>
    `;
    document.getElementById('upload-resume-btn')?.addEventListener('click', () => navigate('resume-upload'));
    document.getElementById('go-explore-btn')?.addEventListener('click', () => navigate('explore'));
}

function renderExplore() {
    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="browse-layout">
            ${renderHeader()}
            <div class="browse-hero">
                <div class="container">
                    <h1>Explore</h1>
                    <p>Discover all roles at Boston's top companies</p>
                </div>
            </div>
            <div class="browse-content">
                <div class="container">
                    <div class="browse-controls">
                        <div class="search-wrapper">
                            <svg class="search-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="11" cy="11" r="8"></circle>
                                <path d="M21 21l-4.35-4.35"></path>
                            </svg>
                            <input type="text" id="search-input" placeholder="Search by role or company..." value="${escapeHtml(state.filters.search)}">
                        </div>
                        <div class="filter-pills">
                            <select id="filter-role-type" class="filter-select">
                                <option value="">All roles</option>
                                <option value="software_engineer">Software Engineer</option>
                                <option value="data_scientist">Data Science</option>
                                <option value="data_analyst">Data Analyst</option>
                                <option value="product_manager">Product Manager</option>
                                <option value="engineering_manager">Engineering Manager</option>
                                <option value="sales">Sales</option>
                                <option value="marketing">Marketing</option>
                                <option value="design">Design</option>
                                <option value="operations">Operations</option>
                                <option value="finance">Finance</option>
                                <option value="hr">HR / People</option>
                                <option value="support">Customer Support</option>
                            </select>
                            <select id="filter-exp-level" class="filter-select">
                                <option value="">All levels</option>
                                <option value="intern">Intern</option>
                                <option value="entry">Entry Level</option>
                                <option value="mid">Mid Level</option>
                                <option value="senior">Senior</option>
                            </select>
                            <div class="location-filter-wrapper">
                                <input type="text" id="filter-location" class="filter-input" placeholder="Filter by location..." value="${escapeHtml(state.filters.location)}" autocomplete="off">
                                <div id="filter-location-suggestions" class="autocomplete-suggestions"></div>
                                ${state.filters.location ? `<button class="clear-filter-btn" id="clear-location-filter">&times;</button>` : ''}
                            </div>
                            <select id="filter-work-arrangement" class="filter-select">
                                <option value="">All work types</option>
                                <option value="onsite">On-site</option>
                                <option value="remote">Remote</option>
                                <option value="hybrid">Hybrid</option>
                            </select>
                        </div>
                    </div>
                    <div class="roles-count" id="roles-count"></div>
                    <div id="roles-list" class="roles-grid">
                        <div class="loading">Loading roles...</div>
                    </div>
                </div>
            </div>
        </div>
    `;

    // Set filter values
    document.getElementById('filter-role-type').value = state.filters.role_type;
    document.getElementById('filter-exp-level').value = state.filters.experience_level;
    document.getElementById('filter-work-arrangement').value = state.filters.work_arrangement;

    // Event listeners
    document.getElementById('search-input').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            state.filters.search = e.target.value;
            loadRoles();
        }
    });

    document.getElementById('filter-role-type').addEventListener('change', (e) => {
        state.filters.role_type = e.target.value;
        loadRoles();
    });

    document.getElementById('filter-exp-level').addEventListener('change', (e) => {
        state.filters.experience_level = e.target.value;
        loadRoles();
    });

    document.getElementById('filter-work-arrangement').addEventListener('change', (e) => {
        state.filters.work_arrangement = e.target.value;
        loadRoles();
    });

    // Location filter with autocomplete
    const locationFilterInput = document.getElementById('filter-location');
    const locationFilterSuggestions = document.getElementById('filter-location-suggestions');
    let locationFilterDebounce = null;

    locationFilterInput.addEventListener('input', () => {
        clearTimeout(locationFilterDebounce);
        const query = locationFilterInput.value.trim();

        if (query.length < 2) {
            locationFilterSuggestions.style.display = 'none';
            return;
        }

        locationFilterDebounce = setTimeout(async () => {
            try {
                const data = await api(`/locations?q=${encodeURIComponent(query)}`);
                const locations = data.locations || [];

                if (locations.length === 0) {
                    locationFilterSuggestions.style.display = 'none';
                    return;
                }

                locationFilterSuggestions.innerHTML = locations.map(l =>
                    `<div class="autocomplete-item" data-value="${escapeHtml(l)}">${escapeHtml(l)}</div>`
                ).join('');
                locationFilterSuggestions.style.display = 'block';

                locationFilterSuggestions.querySelectorAll('.autocomplete-item').forEach(item => {
                    item.addEventListener('click', () => {
                        state.filters.location = item.dataset.value;
                        locationFilterInput.value = item.dataset.value;
                        locationFilterSuggestions.style.display = 'none';
                        loadRoles();
                        // Re-render to show clear button
                        renderExplore();
                    });
                });
            } catch (err) {
                console.error('Failed to fetch locations:', err);
            }
        }, 200);
    });

    // Allow pressing enter to search with typed location
    locationFilterInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            state.filters.location = locationFilterInput.value.trim();
            locationFilterSuggestions.style.display = 'none';
            loadRoles();
        }
    });

    // Clear location filter button
    document.getElementById('clear-location-filter')?.addEventListener('click', () => {
        state.filters.location = '';
        loadRoles();
        renderExplore();
    });

    // Close suggestions when clicking outside
    document.addEventListener('click', (e) => {
        if (!e.target.closest('.location-filter-wrapper')) {
            locationFilterSuggestions.style.display = 'none';
        }
    });

    setupNavListeners();
}

function renderRolesList() {
    const container = document.getElementById('roles-list');
    const countEl = document.getElementById('roles-count');

    // Guard against null container (page not rendered yet)
    if (!container) {
        console.warn('renderRolesList called but roles-list container not found');
        return;
    }

    if (countEl) {
        countEl.textContent = `${state.roles.length} role${state.roles.length !== 1 ? 's' : ''} found`;
    }

    if (!state.roles.length) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">
                    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                        <circle cx="11" cy="11" r="8"></circle>
                        <path d="M21 21l-4.35-4.35"></path>
                    </svg>
                </div>
                <h3>No roles found</h3>
                <p>Try adjusting your search or filters to find more opportunities</p>
            </div>
        `;
        return;
    }

    container.innerHTML = state.roles.map(role => {
        // Match scores are NOT shown on Explore page cards (only in detail view)
        // This keeps Explore focused on discovery rather than ranking
        return `
            <div class="role-card" data-role-id="${role.id}" data-match-score="${role.match_score || ''}">
                <div class="role-card-header">
                    <div class="company-badge">${escapeHtml(role.company_name?.charAt(0) || 'C')}</div>
                    <div class="role-header-right">
                        <div class="role-status-badge ${role.status === 'open' ? 'status-open' : 'status-closed'}">
                            ${role.status === 'open' ? 'Actively hiring' : 'Closed'}
                        </div>
                    </div>
                </div>
                <div class="role-card-body">
                    <h3 class="role-title">${escapeHtml(role.title)}</h3>
                    <div class="role-company">${escapeHtml(role.company_name)}</div>
                </div>
                <div class="role-card-footer">
                    <div class="role-tags">
                        <span class="role-tag">${escapeHtml(role.location || 'Boston Area')}</span>
                        ${role.salary_range ? `<span class="role-tag salary">${escapeHtml(role.salary_range)}</span>` : ''}
                    </div>
                    <button class="view-role-btn" data-role-id="${role.id}">
                        <span>View</span>
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M5 12h14M12 5l7 7-7 7"/>
                        </svg>
                    </button>
                </div>
            </div>
        `;
    }).join('');

    // Add click listeners - track that we came from Explore page (no match score shown)
    container.querySelectorAll('.role-card').forEach(card => {
        card.addEventListener('click', (e) => {
            if (!e.target.classList.contains('view-role-btn')) {
                const roleId = card.dataset.roleId;
                state.previousPage = 'explore';
                loadRole(roleId, null);  // Don't show match score from Explore
                state.currentPage = 'role';
            }
        });
    });

    container.querySelectorAll('.view-role-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const roleId = btn.dataset.roleId;
            state.previousPage = 'explore';
            loadRole(roleId, null);  // Don't show match score from Explore
            state.currentPage = 'role';
        });
    });
}

async function renderRoleDetail() {
    const role = state.selectedRole;
    if (!role) {
        navigate('explore');
        return;
    }

    // Always load user's applications fresh when viewing role detail (if logged in)
    if (state.token) {
        try {
            const appsData = await api('/shortlist/my-applications');
            state.myApplications = appsData.applications || [];
            console.log('Loaded applications:', state.myApplications);
        } catch (err) {
            console.log('Could not load applications:', err);
            state.myApplications = [];
        }
    }

    // Check if user already applied to this role
    const existingApp = state.myApplications?.find(a => a.role_id === role.id);
    const hasApplied = !!existingApp;
    console.log('Role ID:', role.id, 'Existing app:', existingApp, 'hasApplied:', hasApplied);
    const interviewPending = existingApp?.interview_status === 'pending';

    // Determine back navigation text and destination
    const backPage = state.previousPage || 'explore';
    const backText = backPage === 'for-you' ? '← Back to For You' : '← Back to Explore';

    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="main-layout">
            ${renderHeader()}
            <div class="container page-content">
                <button class="btn btn-secondary btn-small" id="back-to-browse" style="margin-bottom:24px;">${backText}</button>
                <div class="role-detail">
                    <div class="role-detail-header">
                        <h1>${escapeHtml(role.title)}</h1>
                        <div class="company">${escapeHtml(role.company_name)}</div>
                    </div>
                    <div class="role-detail-meta">
                        ${role.match_score !== null && role.match_score !== undefined ? `
                        <div>
                            <strong>Match</strong><br>
                            <span class="match-score-detail ${role.match_score >= 75 ? 'high' : role.match_score >= 50 ? 'medium' : 'low'}">${role.match_score}%</span>
                        </div>
                        ` : ''}
                        <div>
                            <strong>Status</strong><br>
                            <span class="role-status">
                                <span class="status-dot ${role.status === 'open' ? 'open' : 'closed'}"></span>
                                ${role.status === 'open' ? 'Open' : 'Closed'}
                            </span>
                        </div>
                        <div>
                            <strong>Location</strong><br>
                            ${escapeHtml(role.location || 'Boston Area')}
                        </div>
                        ${role.salary_range ? `
                        <div>
                            <strong>Salary</strong><br>
                            ${escapeHtml(role.salary_range)}
                        </div>
                        ` : ''}
                    </div>
                    ${role.description ? `
                    <div style="margin-top:24px;">
                        <h3 style="font-size:14px;font-weight:600;margin-bottom:12px;">About this role</h3>
                        <div class="role-description">${formatDescription(role.description)}</div>
                    </div>
                    ` : ''}
                    <div class="role-detail-actions">
                        ${hasApplied ? `
                            <div class="application-status-banner">
                                <div class="status-icon">
                                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                        <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
                                        <polyline points="22 4 12 14.01 9 11.01"></polyline>
                                    </svg>
                                </div>
                                <div class="status-text">
                                    <strong>You joined the Shortlist</strong>
                                    <span>Applied ${formatDate(existingApp.applied_at)}</span>
                                </div>
                                ${interviewPending ? `
                                    <button class="btn btn-primary btn-small" id="complete-interview-btn">
                                        Complete Interview
                                    </button>
                                ` : ''}
                            </div>
                        ` : `
                            <button class="btn btn-primary" id="join-shortlist-btn">Join the Shortlist</button>
                        `}
                    </div>
                    <p style="margin-top:12px;font-size:13px;color:#737373;">
                        ${role.applicant_count || 0} candidate${role.applicant_count !== 1 ? 's' : ''} on the shortlist
                    </p>
                </div>
            </div>
        </div>
    `;

    document.getElementById('back-to-browse').addEventListener('click', () => {
        const backPage = state.previousPage || 'explore';
        navigate(backPage);
    });

    document.getElementById('join-shortlist-btn')?.addEventListener('click', () => {
        applyToShortlist(role.id);
    });

    document.getElementById('complete-interview-btn')?.addEventListener('click', async () => {
        // Launch the AI interview for this application
        if (existingApp && existingApp.id) {
            await startAIInterview(
                existingApp.id,
                // On complete
                (metadata) => {
                    showSuccessModal(
                        'Interview Complete!',
                        `Great job! Your interview has been submitted. The hiring team will review your responses.`
                    );
                    // Reload to update status
                    renderRoleDetail();
                },
                // On close/cancel
                () => {
                    // Just close, progress saved
                    renderRoleDetail();
                }
            );
        }
    });

    setupNavListeners();
}

function renderMyShortlist() {
    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="main-layout">
            ${renderHeader()}
            <div class="container page-content shortlist-page">
                <h1>My Shortlist</h1>
                <div id="applications-list">
                    <div class="loading">Loading...</div>
                </div>
            </div>
        </div>
    `;

    setupNavListeners();

    // Render applications
    const container = document.getElementById('applications-list');

    if (!state.myApplications.length) {
        container.innerHTML = `
            <div class="empty-state">
                <h3>No applications yet</h3>
                <p>Browse roles and join the shortlist to get started</p>
                <button class="btn btn-primary" style="margin-top:16px;" id="browse-roles-btn">Browse Roles</button>
            </div>
        `;
        document.getElementById('browse-roles-btn')?.addEventListener('click', () => navigate('browse'));
        return;
    }

    container.innerHTML = state.myApplications.map(app => {
        const interviewPending = app.interview_status === 'pending';
        return `
            <div class="application-card" data-role-id="${app.role_id}">
                <div class="application-info">
                    <h3>${escapeHtml(app.title)}</h3>
                    <div class="application-company">${escapeHtml(app.company_name)}</div>
                    <div class="application-date">Applied ${formatDate(app.applied_at)}</div>
                </div>
                <div class="application-actions">
                    <div class="role-status">
                        <span class="status-dot ${app.role_status === 'open' ? 'open' : 'closed'}"></span>
                        Role ${app.role_status === 'open' ? 'Open' : 'Closed'}
                    </div>
                    ${interviewPending ? `
                        <span class="interview-badge pending">
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="12" cy="12" r="10"></circle>
                                <polyline points="12 6 12 12 16 14"></polyline>
                            </svg>
                            Interview pending
                        </span>
                        <button class="btn btn-primary btn-small complete-interview-btn" data-app-id="${app.id}">
                            Complete
                        </button>
                    ` : `
                        <span class="application-status ${app.status}">
                            ${app.status === 'submitted' ? 'On shortlist' : app.status}
                        </span>
                    `}
                </div>
            </div>
        `;
    }).join('');

    // Add click handlers for interview buttons
    container.querySelectorAll('.complete-interview-btn').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            e.stopPropagation();
            const appId = parseInt(btn.dataset.appId);
            if (appId) {
                await startAIInterview(
                    appId,
                    // On complete
                    () => {
                        showSuccessModal(
                            'Interview Complete!',
                            'Great job! Your interview has been submitted. The hiring team will review your responses.'
                        );
                        loadMyApplications();
                    },
                    // On close/cancel
                    () => {
                        loadMyApplications();
                    }
                );
            }
        });
    });

    // Add click handlers to view role details
    container.querySelectorAll('.application-card').forEach(card => {
        card.addEventListener('click', async () => {
            const roleId = card.dataset.roleId;
            try {
                const data = await api(`/roles/${roleId}`);
                state.selectedRole = data.role;
                state.previousPage = 'shortlist';
                navigate('role-detail');
            } catch (err) {
                console.error('Failed to load role:', err);
            }
        });
    });
}

function renderProfile() {
    const app = document.getElementById('app');
    const initials = state.user ?
        ((state.user.first_name?.[0] || '') + (state.user.last_name?.[0] || '')).toUpperCase() ||
        (state.user.email?.[0] || 'U').toUpperCase()
        : 'U';

    app.innerHTML = `
        <div class="main-layout">
            ${renderHeader()}
            <div class="container page-content profile-page">
                <div class="profile-header-section">
                    <div class="profile-avatar-large">${initials}</div>
                    <div class="profile-header-info">
                        <h1>${escapeHtml((state.user?.first_name || '') + ' ' + (state.user?.last_name || '')) || 'Your Profile'}</h1>
                        <p class="profile-email">${escapeHtml(state.user?.email || '')}</p>
                    </div>
                </div>

                <div id="profile-content">
                    <div class="loading">Loading profile...</div>
                </div>
            </div>
        </div>
    `;

    setupNavListeners();
}

function renderProfileContent() {
    const container = document.getElementById('profile-content');
    if (!container) return;

    const prefs = state.profilePreferences || {};
    const hasResume = state.user?.has_resume || state.user?.resume_path;

    // Format selected locations
    const selectedLocations = prefs.preferred_locations || [];
    const selectedRoles = prefs.open_to_roles || [];

    // Role type display names
    const roleTypeLabels = {
        'software_engineer': 'Software Engineer',
        'data_scientist': 'Data Science',
        'data_analyst': 'Data Analyst',
        'product_manager': 'Product Manager',
        'engineering_manager': 'Engineering Manager',
        'sales': 'Sales',
        'marketing': 'Marketing',
        'design': 'Design',
        'operations': 'Operations',
        'finance': 'Finance',
        'hr': 'HR / People',
        'support': 'Customer Support'
    };

    container.innerHTML = `
        <div class="profile-sections">
            <!-- Resume Section -->
            <div class="profile-section">
                <h2>Resume</h2>
                <div class="resume-status-box ${hasResume ? 'has-resume' : 'no-resume'}">
                    ${hasResume ? `
                        <div class="resume-uploaded">
                            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                                <polyline points="14 2 14 8 20 8"></polyline>
                            </svg>
                            <div>
                                <strong>Resume uploaded</strong>
                                <p>Your resume is being used for job matching</p>
                            </div>
                        </div>
                    ` : `
                        <div class="no-resume-msg">
                            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="12" cy="12" r="10"></circle>
                                <line x1="12" y1="8" x2="12" y2="12"></line>
                                <line x1="12" y1="16" x2="12.01" y2="16"></line>
                            </svg>
                            <div>
                                <strong>No resume uploaded</strong>
                                <p>Upload your resume for better job matches</p>
                            </div>
                        </div>
                    `}
                    <div class="resume-upload-area" id="profile-resume-upload-area">
                        <input type="file" id="profile-resume-input" accept=".pdf,.doc,.docx" style="display:none;">
                        <button class="btn ${hasResume ? 'btn-secondary' : 'btn-primary'}" id="profile-upload-resume-btn">
                            ${hasResume ? 'Upload New Resume' : 'Upload Resume'}
                        </button>
                    </div>

                    <!-- Progress steps for profile resume upload -->
                    <div id="profile-upload-progress" class="profile-upload-progress" style="display: none;">
                        <div class="progress-steps compact">
                            <div class="progress-step pending" id="profile-step-upload">
                                <div class="step-icon">
                                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                                        <polyline points="17 8 12 3 7 8"/>
                                        <line x1="12" y1="3" x2="12" y2="15"/>
                                    </svg>
                                </div>
                                <div class="step-content">
                                    <div class="step-label">Uploading</div>
                                </div>
                                <div class="step-status">
                                    <div class="step-spinner"></div>
                                    <svg class="step-check" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                                        <polyline points="20 6 9 17 4 12"/>
                                    </svg>
                                </div>
                            </div>
                            <div class="progress-step pending" id="profile-step-extract">
                                <div class="step-icon">
                                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                        <circle cx="12" cy="12" r="10"/>
                                        <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/>
                                        <line x1="12" y1="17" x2="12.01" y2="17"/>
                                    </svg>
                                </div>
                                <div class="step-content">
                                    <div class="step-label">Extracting skills</div>
                                </div>
                                <div class="step-status">
                                    <div class="step-spinner"></div>
                                    <svg class="step-check" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                                        <polyline points="20 6 9 17 4 12"/>
                                    </svg>
                                </div>
                            </div>
                            <div class="progress-step pending" id="profile-step-match">
                                <div class="step-icon">
                                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                        <path d="M12 2L2 7l10 5 10-5-10-5z"/>
                                        <path d="M2 17l10 5 10-5"/>
                                        <path d="M2 12l10 5 10-5"/>
                                    </svg>
                                </div>
                                <div class="step-content">
                                    <div class="step-label">Updating matches</div>
                                </div>
                                <div class="step-status">
                                    <div class="step-spinner"></div>
                                    <svg class="step-check" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                                        <polyline points="20 6 9 17 4 12"/>
                                    </svg>
                                </div>
                            </div>
                        </div>
                        <div class="profile-upload-progress-bar">
                            <div class="profile-upload-progress-fill" id="profile-progress-fill"></div>
                        </div>
                    </div>

                    <div id="profile-resume-status"></div>
                </div>
            </div>

            <!-- Preferences Section -->
            <div class="profile-section">
                <h2>Job Preferences</h2>
                <p class="section-description">These preferences affect your "For You" recommendations and match scores.</p>

                <div class="preference-group">
                    <label>Preferred Locations</label>
                    <div class="location-autocomplete profile-location-autocomplete">
                        <div id="profile-selected-locations" class="selected-tags">
                            ${selectedLocations.map(loc => `
                                <span class="tag">${escapeHtml(loc)} <button class="tag-remove" data-location="${escapeHtml(loc)}">&times;</button></span>
                            `).join('')}
                        </div>
                        <input type="text" id="profile-location-input" placeholder="Type a city..." autocomplete="off">
                        <div id="profile-location-suggestions" class="autocomplete-suggestions"></div>
                    </div>
                </div>

                <div class="preference-group">
                    <label>Open to Roles</label>
                    <div class="checkbox-grid">
                        ${Object.entries(roleTypeLabels).map(([value, label]) => `
                            <label class="checkbox-item">
                                <input type="checkbox" name="profile-roles" value="${value}" ${selectedRoles.includes(value) ? 'checked' : ''}>
                                <span>${label}</span>
                            </label>
                        `).join('')}
                    </div>
                </div>

                <div class="preference-row">
                    <div class="preference-group">
                        <label>Experience Level</label>
                        <select id="profile-exp-level" class="profile-select">
                            <option value="">Select level...</option>
                            <option value="intern" ${prefs.experience_level === 'intern' ? 'selected' : ''}>Intern</option>
                            <option value="entry" ${prefs.experience_level === 'entry' ? 'selected' : ''}>Entry Level</option>
                            <option value="mid" ${prefs.experience_level === 'mid' ? 'selected' : ''}>Mid Level</option>
                            <option value="senior" ${prefs.experience_level === 'senior' ? 'selected' : ''}>Senior</option>
                        </select>
                    </div>

                    <div class="preference-group">
                        <label>Work Preference</label>
                        <select id="profile-work-pref" class="profile-select">
                            <option value="">Select preference...</option>
                            <option value="onsite" ${prefs.work_preference === 'onsite' ? 'selected' : ''}>On-site</option>
                            <option value="remote" ${prefs.work_preference === 'remote' ? 'selected' : ''}>Remote</option>
                            <option value="hybrid" ${prefs.work_preference === 'hybrid' ? 'selected' : ''}>Hybrid</option>
                        </select>
                    </div>
                </div>

                <div class="preference-row">
                    <div class="preference-group">
                        <label>Minimum Salary</label>
                        <input type="text" id="profile-salary-min" class="profile-input" placeholder="e.g. 80,000" value="${prefs.salary_min ? prefs.salary_min.toLocaleString() : ''}">
                    </div>
                    <div class="preference-group">
                        <label>Maximum Salary</label>
                        <input type="text" id="profile-salary-max" class="profile-input" placeholder="e.g. 150,000" value="${prefs.salary_max ? prefs.salary_max.toLocaleString() : ''}">
                    </div>
                </div>

                <div class="profile-actions">
                    <button class="btn btn-primary" id="save-preferences-btn">Save Preferences</button>
                    <span id="save-status"></span>
                </div>
            </div>

            <!-- Sign Out Section -->
            <div class="profile-section sign-out-section">
                <button class="btn btn-secondary" id="profile-logout-btn">Sign Out</button>
            </div>
        </div>
    `;

    setupProfileListeners();
}

function setupProfileListeners() {
    // Resume upload with step-based progress
    const resumeInput = document.getElementById('profile-resume-input');
    const uploadBtn = document.getElementById('profile-upload-resume-btn');
    const uploadArea = document.getElementById('profile-resume-upload-area');
    const progressSection = document.getElementById('profile-upload-progress');
    const progressFill = document.getElementById('profile-progress-fill');
    const statusDiv = document.getElementById('profile-resume-status');

    const stepUpload = document.getElementById('profile-step-upload');
    const stepExtract = document.getElementById('profile-step-extract');
    const stepMatch = document.getElementById('profile-step-match');

    function setStepState(stepEl, state) {
        if (!stepEl) return;
        stepEl.classList.remove('pending', 'active', 'complete');
        stepEl.classList.add(state);
    }

    function updateProgress(percent) {
        if (progressFill) {
            progressFill.style.width = `${percent}%`;
        }
    }

    function animateProgress(from, to, duration) {
        const startTime = performance.now();
        function update(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const easeOut = 1 - Math.pow(1 - progress, 3);
            const current = from + (to - from) * easeOut;
            updateProgress(current);
            if (progress < 1) {
                requestAnimationFrame(update);
            }
        }
        requestAnimationFrame(update);
    }

    uploadBtn?.addEventListener('click', () => resumeInput?.click());

    resumeInput?.addEventListener('change', async (e) => {
        const file = e.target.files[0];
        if (!file) return;

        // Show progress UI, hide upload button
        uploadArea.style.display = 'none';
        progressSection.style.display = 'block';
        statusDiv.innerHTML = '';

        // Step 1: Uploading
        setStepState(stepUpload, 'active');
        setStepState(stepExtract, 'pending');
        setStepState(stepMatch, 'pending');
        animateProgress(0, 25, 400);

        try {
            const formData = new FormData();
            formData.append('file', file);

            // Animate while uploading
            await new Promise(r => setTimeout(r, 200));
            animateProgress(25, 40, 600);

            // Step 2: Extracting skills
            setStepState(stepUpload, 'complete');
            setStepState(stepExtract, 'active');
            animateProgress(40, 55, 400);

            const response = await fetch(`${API_BASE}/profile/upload-resume`, {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${state.token}` },
                body: formData
            });

            const data = await response.json();
            if (!response.ok) throw new Error(data.error || 'Upload failed');

            // Skills extracted
            animateProgress(55, 70, 400);
            await new Promise(r => setTimeout(r, 400));
            setStepState(stepExtract, 'complete');

            // Step 3: Updating matches
            setStepState(stepMatch, 'active');
            animateProgress(70, 85, 500);

            // Refresh user data
            const userData = await api('/auth/me');
            state.user = userData.user;
            state.forYouLoaded = false; // Force refresh of For You jobs

            animateProgress(85, 100, 400);
            await new Promise(r => setTimeout(r, 400));
            setStepState(stepMatch, 'complete');

            // Show success and reset UI
            await new Promise(r => setTimeout(r, 600));
            progressSection.style.display = 'none';
            uploadArea.style.display = 'block';
            statusDiv.innerHTML = '<div class="upload-success">Resume updated! Your job matches will refresh.</div>';

            // Re-render profile content to show updated status
            setTimeout(() => loadProfileData(), 1500);
        } catch (err) {
            progressSection.style.display = 'none';
            uploadArea.style.display = 'block';
            statusDiv.innerHTML = `<div class="upload-error">Error: ${escapeHtml(err.message)}</div>`;
            // Reset step states
            setStepState(stepUpload, 'pending');
            setStepState(stepExtract, 'pending');
            setStepState(stepMatch, 'pending');
            updateProgress(0);
        }
    });

    // Location autocomplete
    const locationInput = document.getElementById('profile-location-input');
    const locationSuggestions = document.getElementById('profile-location-suggestions');
    let locationDebounce = null;

    // Get current selected locations
    let selectedLocations = [...(state.profilePreferences?.preferred_locations || [])];

    function renderSelectedLocations() {
        const container = document.getElementById('profile-selected-locations');
        if (!container) return;
        container.innerHTML = selectedLocations.map(loc => `
            <span class="tag">${escapeHtml(loc)} <button class="tag-remove" data-location="${escapeHtml(loc)}">&times;</button></span>
        `).join('');

        // Re-attach remove listeners
        container.querySelectorAll('.tag-remove').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.preventDefault();
                const loc = btn.dataset.location;
                selectedLocations = selectedLocations.filter(l => l !== loc);
                renderSelectedLocations();
            });
        });
    }

    // Initial render of remove listeners
    document.querySelectorAll('#profile-selected-locations .tag-remove').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            const loc = btn.dataset.location;
            selectedLocations = selectedLocations.filter(l => l !== loc);
            renderSelectedLocations();
        });
    });

    locationInput?.addEventListener('input', () => {
        clearTimeout(locationDebounce);
        const query = locationInput.value.trim();

        if (query.length < 2) {
            locationSuggestions.style.display = 'none';
            return;
        }

        locationDebounce = setTimeout(async () => {
            try {
                const data = await api(`/locations?q=${encodeURIComponent(query)}`);
                const locations = (data.locations || []).filter(l => !selectedLocations.includes(l));

                if (locations.length === 0) {
                    locationSuggestions.style.display = 'none';
                    return;
                }

                locationSuggestions.innerHTML = locations.map(l =>
                    `<div class="autocomplete-item" data-value="${escapeHtml(l)}">${escapeHtml(l)}</div>`
                ).join('');
                locationSuggestions.style.display = 'block';

                locationSuggestions.querySelectorAll('.autocomplete-item').forEach(item => {
                    item.addEventListener('click', () => {
                        selectedLocations.push(item.dataset.value);
                        renderSelectedLocations();
                        locationInput.value = '';
                        locationSuggestions.style.display = 'none';
                    });
                });
            } catch (err) {
                console.error('Failed to fetch locations:', err);
            }
        }, 200);
    });

    document.addEventListener('click', (e) => {
        if (!e.target.closest('.profile-location-autocomplete')) {
            locationSuggestions.style.display = 'none';
        }
    });

    // Salary formatting
    const salaryMin = document.getElementById('profile-salary-min');
    const salaryMax = document.getElementById('profile-salary-max');

    function formatSalaryInput(input) {
        let value = input.value.replace(/[^0-9]/g, '');
        if (value) {
            value = parseInt(value).toLocaleString();
        }
        input.value = value;
    }

    salaryMin?.addEventListener('input', () => formatSalaryInput(salaryMin));
    salaryMax?.addEventListener('input', () => formatSalaryInput(salaryMax));

    // Save preferences
    document.getElementById('save-preferences-btn')?.addEventListener('click', async () => {
        const statusSpan = document.getElementById('save-status');
        statusSpan.textContent = 'Saving...';
        statusSpan.className = '';

        try {
            // Gather selected roles
            const selectedRoles = [...document.querySelectorAll('input[name="profile-roles"]:checked')]
                .map(cb => cb.value);

            const expLevel = document.getElementById('profile-exp-level').value;
            const workPref = document.getElementById('profile-work-pref').value;
            const salaryMinVal = parseInt((salaryMin?.value || '').replace(/[^0-9]/g, '')) || null;
            const salaryMaxVal = parseInt((salaryMax?.value || '').replace(/[^0-9]/g, '')) || null;

            await api('/profile/preferences', {
                method: 'PUT',
                body: JSON.stringify({
                    preferred_locations: selectedLocations.length > 0 ? selectedLocations : null,
                    open_to_roles: selectedRoles.length > 0 ? selectedRoles : null,
                    experience_level: expLevel || null,
                    work_preference: workPref || null,
                    salary_min: salaryMinVal,
                    salary_max: salaryMaxVal
                })
            });

            statusSpan.textContent = 'Saved!';
            statusSpan.className = 'save-success';

            // Invalidate For You cache so it refreshes with new preferences
            state.forYouLoaded = false;
            state.profilePreferences = {
                ...state.profilePreferences,
                preferred_locations: selectedLocations,
                open_to_roles: selectedRoles,
                experience_level: expLevel,
                work_preference: workPref,
                salary_min: salaryMinVal,
                salary_max: salaryMaxVal
            };

            setTimeout(() => {
                statusSpan.textContent = '';
            }, 2000);
        } catch (err) {
            statusSpan.textContent = 'Error saving';
            statusSpan.className = 'save-error';
            console.error('Failed to save preferences:', err);
        }
    });

    // Logout
    document.getElementById('profile-logout-btn')?.addEventListener('click', logout);
}

function renderEmployerDashboard() {
    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="main-layout">
            ${renderEmployerHeader()}
            <div class="container page-content">
                <h1>Employer Dashboard</h1>
                <p style="color:#525252;margin-bottom:24px;">Roles with shortlisted candidates</p>
                <div id="employer-roles-list">
                    <div class="loading">Loading roles...</div>
                </div>
            </div>
        </div>
    `;

    setupEmployerNavListeners();

    // Render roles
    const container = document.getElementById('employer-roles-list');

    if (!state.employerRoles.length) {
        container.innerHTML = `
            <div class="empty-state">
                <h3>No roles with applicants yet</h3>
                <p>When candidates join shortlists for your roles, they'll appear here.</p>
            </div>
        `;
        return;
    }

    container.innerHTML = state.employerRoles.map(role => `
        <div class="role-card employer-role-card" data-role-id="${role.id}">
            <div class="role-info">
                <h3>${escapeHtml(role.title)}</h3>
                <div class="role-company">${escapeHtml(role.company_name)}</div>
            </div>
            <div class="role-actions">
                <div class="role-status">
                    <span class="status-dot ${role.status === 'open' ? 'open' : 'closed'}"></span>
                    ${role.status === 'open' ? 'Open' : 'Closed'}
                </div>
                <div class="applicant-count">${role.applicant_count} candidate${role.applicant_count !== 1 ? 's' : ''}</div>
                <button class="btn btn-small btn-primary view-applicants-btn" data-role-id="${role.id}">View Candidates</button>
            </div>
        </div>
    `).join('');

    // Add click listeners
    container.querySelectorAll('.view-applicants-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const roleId = parseInt(btn.dataset.roleId);
            loadApplicants(roleId);
            state.currentPage = 'applicants';
        });
    });

    container.querySelectorAll('.employer-role-card').forEach(card => {
        card.addEventListener('click', () => {
            const roleId = parseInt(card.dataset.roleId);
            loadApplicants(roleId);
            state.currentPage = 'applicants';
        });
    });
}

function renderApplicantsList() {
    const role = state.selectedEmployerRole;
    const app = document.getElementById('app');

    // Find first applicant with a resume
    const firstWithResume = state.applicants.find(a => a.resume_path);
    const selectedApplicant = firstWithResume || null;

    // Helper to get match score display
    const getMatchScoreHtml = (score) => {
        if (score === null || score === undefined) return '';
        const scoreClass = score >= 75 ? 'high' : score >= 50 ? 'medium' : 'low';
        return `<span class="candidate-match-badge ${scoreClass}">${score}% Match</span>`;
    };

    // Helper to render the resume card content
    const renderResumeCard = (applicant) => {
        if (!applicant) {
            return `
                <div class="resume-card-empty">
                    <div class="resume-card-empty-content">
                        <svg width="56" height="56" viewBox="0 0 24 24" fill="none" stroke="#d1d5db" stroke-width="1.5">
                            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                            <polyline points="14 2 14 8 20 8"></polyline>
                            <line x1="16" y1="13" x2="8" y2="13"></line>
                            <line x1="16" y1="17" x2="8" y2="17"></line>
                        </svg>
                        <h3>Select a candidate</h3>
                        <p>Click on a candidate to view their resume</p>
                    </div>
                </div>
            `;
        }

        if (!applicant.resume_path) {
            return `
                <div class="resume-card">
                    <div class="resume-card-eyebrow">
                        <span class="eyebrow-label">CANDIDATE</span>
                        ${getMatchScoreHtml(applicant.match_score)}
                    </div>
                    <div class="resume-card-header">
                        <div class="resume-card-avatar">${(applicant.full_name || applicant.email).charAt(0).toUpperCase()}</div>
                        <div class="resume-card-info">
                            <h3 class="resume-card-name">${escapeHtml(applicant.full_name || applicant.email.split('@')[0])}</h3>
                            <p class="resume-card-email">${escapeHtml(applicant.email)}</p>
                        </div>
                    </div>
                    <div class="resume-card-meta">
                        <span class="meta-tag">${formatExpLevel(applicant.experience_level) || 'Not specified'}</span>
                        <span class="meta-tag">${formatWorkPref(applicant.work_preference) || 'Flexible'}</span>
                        <span class="meta-tag">Applied ${formatDate(applicant.applied_at)}</span>
                    </div>
                    <div class="resume-card-empty-content" style="padding: 60px 0;">
                        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#d1d5db" stroke-width="1.5">
                            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                            <polyline points="14 2 14 8 20 8"></polyline>
                        </svg>
                        <p style="margin-top:16px;color:#737373;">No resume uploaded yet</p>
                    </div>
                </div>
            `;
        }

        // Build fit responses section
        const fitResponsesHtml = renderFitResponses(applicant.fit_responses);

        return `
            <div class="resume-card">
                <div class="resume-card-eyebrow">
                    <span class="eyebrow-label">CANDIDATE</span>
                    ${getMatchScoreHtml(applicant.match_score)}
                </div>
                <div class="resume-card-header">
                    <div class="resume-card-avatar">${(applicant.full_name || applicant.email).charAt(0).toUpperCase()}</div>
                    <div class="resume-card-info">
                        <h3 class="resume-card-name">${escapeHtml(applicant.full_name || applicant.email.split('@')[0])}</h3>
                        <p class="resume-card-email">${escapeHtml(applicant.email)}</p>
                    </div>
                    <a href="${API_BASE}/employer/download-resume/${applicant.application_id}?token=${state.token}"
                       class="btn btn-small btn-secondary resume-download-btn" download>
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                            <polyline points="7 10 12 15 17 10"></polyline>
                            <line x1="12" y1="15" x2="12" y2="3"></line>
                        </svg>
                        Download
                    </a>
                </div>
                <div class="resume-card-meta">
                    <span class="meta-tag">${formatExpLevel(applicant.experience_level) || 'Not specified'}</span>
                    <span class="meta-tag">${formatWorkPref(applicant.work_preference) || 'Flexible'}</span>
                    <span class="meta-tag">Applied ${formatDate(applicant.applied_at)}</span>
                </div>
                ${fitResponsesHtml}
                <div class="resume-card-preview">
                    <iframe src="${API_BASE}/employer/view-resume/${applicant.application_id}?token=${state.token}"
                            title="Resume Preview"></iframe>
                </div>
            </div>
        `;
    };

    // Helper to render fit responses section
    const renderFitResponses = (responses) => {
        if (!responses || responses.length === 0) return '';

        const mcResponses = responses.filter(r => r.question_type === 'multiple_choice');
        const frResponses = responses.filter(r => r.question_type === 'free_response');

        return `
            <div class="fit-responses-section">
                <div class="fit-responses-header">
                    <span class="fit-responses-label">Work Style Responses</span>
                </div>
                ${mcResponses.length > 0 ? `
                    <div class="fit-responses-mc">
                        ${mcResponses.map(r => `
                            <div class="fit-response-item">
                                <div class="fit-response-q">${escapeHtml(r.question_text)}</div>
                                <div class="fit-response-a">
                                    <span class="fit-response-badge">${r.response_value}</span>
                                    ${escapeHtml(r.response_label || '')}
                                </div>
                            </div>
                        `).join('')}
                    </div>
                ` : ''}
                ${frResponses.length > 0 ? `
                    <div class="fit-responses-fr">
                        ${frResponses.map(r => `
                            <div class="fit-response-item fit-response-fr-item">
                                <div class="fit-response-q">${escapeHtml(r.question_text)}</div>
                                <div class="fit-response-text">"${escapeHtml(r.response_text || '')}"</div>
                            </div>
                        `).join('')}
                    </div>
                ` : ''}
            </div>
        `;
    };

    app.innerHTML = `
        <div class="main-layout">
            ${renderEmployerHeader()}
            <div class="applicants-split-view">
                <!-- Left panel: Candidate list -->
                <div class="candidates-panel">
                    <div class="candidates-header">
                        <button class="btn btn-secondary btn-small" id="back-to-employer">← Back</button>
                        <div class="candidates-title">
                            <h2>${role ? escapeHtml(role.title) : 'Candidates'}</h2>
                            <span class="candidates-company">${role ? escapeHtml(role.company_name) : ''}</span>
                        </div>
                    </div>
                    <div class="candidates-count">${state.applicants.length} candidate${state.applicants.length !== 1 ? 's' : ''}</div>
                    <div class="candidates-list" id="candidates-list">
                        ${state.applicants.length === 0 ? `
                            <div class="empty-state" style="padding:40px 20px;">
                                <h3>No candidates yet</h3>
                                <p>Candidates who join the shortlist will appear here.</p>
                            </div>
                        ` : state.applicants.map(applicant => `
                            <div class="candidate-card ${selectedApplicant && applicant.application_id === selectedApplicant.application_id ? 'selected' : ''}"
                                 data-applicant-id="${applicant.application_id}">
                                <div class="candidate-main">
                                    <div class="candidate-name">${escapeHtml(applicant.full_name || applicant.email.split('@')[0])}</div>
                                    <div class="candidate-email-small">${escapeHtml(applicant.email)}</div>
                                </div>
                                <div class="candidate-right">
                                    ${applicant.match_score !== null && applicant.match_score !== undefined
                                        ? `<span class="candidate-score ${applicant.match_score >= 75 ? 'high' : applicant.match_score >= 50 ? 'medium' : 'low'}">${applicant.match_score}%</span>`
                                        : ''}
                                    ${applicant.resume_path
                                        ? '<span class="resume-indicator" title="Has resume">📄</span>'
                                        : '<span class="no-resume-indicator" title="No resume">—</span>'}
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>

                <!-- Right panel: Resume preview card (Notion-style) -->
                <div class="resume-panel" id="resume-panel">
                    ${renderResumeCard(selectedApplicant)}
                </div>
            </div>
        </div>
    `;

    setupEmployerNavListeners();

    document.getElementById('back-to-employer').addEventListener('click', () => {
        navigate('employer');
    });

    // Add click listeners to candidate cards
    document.querySelectorAll('.candidate-card').forEach(card => {
        card.addEventListener('click', () => {
            const applicantId = parseInt(card.dataset.applicantId);
            const applicant = state.applicants.find(a => a.application_id === applicantId);

            // Update selection
            document.querySelectorAll('.candidate-card').forEach(c => c.classList.remove('selected'));
            card.classList.add('selected');

            // Update resume panel with the card
            const resumePanel = document.getElementById('resume-panel');
            resumePanel.innerHTML = renderResumeCard(applicant);
        });
    });
}

// ============================================================================
// PREMIUM EMPLOYER VIEW - Ranked Inbox with Side Drawer
// ============================================================================

function renderPremiumApplicantsList() {
    const role = state.selectedEmployerRole;
    const app = document.getElementById('app');

    // Separate visible and hidden candidates
    const visibleCandidates = state.applicants.filter(a =>
        (a.fit_score >= 70 || a.fit_score === null) && !a.hard_filter_failed
    );

    // Helper functions
    const getScoreClass = (score) => {
        if (score >= 85) return 'excellent';
        if (score >= 70) return 'good';
        if (score >= 50) return 'fair';
        return 'low';
    };

    const renderSkillChips = (chips) => {
        if (!chips || chips.length === 0) return '';
        return chips.slice(0, 5).map(chip => `
            <span class="premium-skill-chip ${chip.is_must_have ? 'must-have' : ''}">${escapeHtml(chip.skill)}</span>
        `).join('');
    };

    const renderHardFilterIcon = (type, passed, tooltip) => {
        const icons = {
            work_authorization: passed ? '✓' : '✗',
            location: passed ? '📍' : '⚠',
            start_date: passed ? '📅' : '⏳',
            seniority: passed ? '👤' : '⚠'
        };
        const statusClass = passed === true ? 'passed' : passed === false ? 'failed' : 'unknown';
        return `<span class="filter-icon ${statusClass}" title="${tooltip}">${icons[type] || '?'}</span>`;
    };

    const renderCandidateCard = (applicant) => {
        const scoreClass = getScoreClass(applicant.fit_score);
        const skillChips = applicant.matched_skill_chips || [];
        const hardFilters = applicant.hard_filter_breakdown || {};
        const strengths = applicant.strengths || [];
        const risks = applicant.risks || [];

        return `
            <div class="premium-candidate-card ${state.selectedApplicantDetail?.application_id === applicant.application_id ? 'selected' : ''}"
                 data-application-id="${applicant.application_id}">
                <div class="card-main">
                    <div class="card-left">
                        <div class="fit-score-badge ${scoreClass}">
                            <span class="score-value">${applicant.fit_score ?? '—'}</span>
                            ${applicant.confidence_level ? `
                                <span class="confidence-dot ${applicant.confidence_level}"
                                      title="${applicant.confidence_level} confidence"></span>
                            ` : ''}
                        </div>
                    </div>
                    <div class="card-center">
                        <div class="candidate-name">${escapeHtml(applicant.full_name || applicant.email.split('@')[0])}</div>
                        <div class="why-this-person">${escapeHtml(applicant.why_this_person || '')}</div>
                        <div class="skill-chips">
                            ${renderSkillChips(skillChips)}
                        </div>
                    </div>
                    <div class="card-right">
                        <div class="hard-filter-icons">
                            ${renderHardFilterIcon('work_authorization', hardFilters.work_authorization, 'Work Authorization')}
                            ${renderHardFilterIcon('location', hardFilters.location, 'Location/Hybrid')}
                            ${renderHardFilterIcon('start_date', hardFilters.start_date, 'Availability')}
                        </div>
                        <div class="card-interview-status">
                            ${applicant.interview_status === 'completed'
                                ? '<span class="interview-done">✓ Interviewed</span>'
                                : '<span class="interview-pending">Pending</span>'}
                        </div>
                    </div>
                </div>
                ${(strengths.length > 0 || risks.length > 0) ? `
                    <div class="card-preview">
                        ${strengths.slice(0, 1).map(s => `
                            <span class="strength-preview">✓ ${escapeHtml(s.text)}</span>
                        `).join('')}
                        ${risks.slice(0, 1).map(r => `
                            <span class="risk-preview">⚠ ${escapeHtml(r.text)}</span>
                        `).join('')}
                    </div>
                ` : ''}
            </div>
        `;
    };

    const renderFilterPills = () => {
        const seniorityOptions = ['entry', 'mid', 'senior'];
        return `
            <div class="filter-pills-container">
                <div class="filter-group">
                    <label>Seniority:</label>
                    ${seniorityOptions.map(level => `
                        <button class="filter-pill ${state.employerFilters.seniority.includes(level) ? 'active' : ''}"
                                data-filter="seniority" data-value="${level}">
                            ${level.charAt(0).toUpperCase() + level.slice(1)}
                        </button>
                    `).join('')}
                </div>
                ${state.employerFilters.seniority.length > 0 ? `
                    <button class="clear-filters-btn" id="clear-filters">Clear</button>
                ` : ''}
            </div>
        `;
    };

    app.innerHTML = `
        <div class="main-layout">
            ${renderEmployerHeader()}
            <div class="premium-applicants-view">
                <!-- Left: Ranked Inbox -->
                <div class="ranked-inbox">
                    <div class="inbox-header">
                        <button class="btn btn-secondary btn-small" id="back-to-employer">← Back</button>
                        <div class="inbox-title">
                            <h2>${escapeHtml(role?.title || 'Candidates')}</h2>
                            <span class="inbox-company">${escapeHtml(role?.company_name || '')}</span>
                        </div>
                    </div>

                    <!-- Filter Pills -->
                    <div class="filter-pills">
                        ${renderFilterPills()}
                    </div>

                    <!-- Candidates Count -->
                    <div class="candidates-summary">
                        <span class="visible-count">${visibleCandidates.length} candidate${visibleCandidates.length !== 1 ? 's' : ''}</span>
                        ${state.hiddenApplicantsCount > 0 ? `
                            <button class="hidden-toggle" id="toggle-hidden">
                                ${state.showHiddenApplicants ? 'Hide' : 'Show'} ${state.hiddenApplicantsCount} below 70%
                            </button>
                        ` : ''}
                    </div>

                    <!-- Candidate Cards -->
                    <div class="candidate-cards-list" id="candidate-cards">
                        ${state.applicants.length === 0 ? `
                            <div class="empty-state" style="padding:40px 20px;">
                                <h3>No candidates yet</h3>
                                <p>Candidates who join the shortlist will appear here.</p>
                            </div>
                        ` : state.applicants.map(a => renderCandidateCard(a)).join('')}

                        ${!state.showHiddenApplicants && state.hiddenApplicantsCount > 0 ? `
                            <div class="hidden-candidates-divider">
                                <span>Hidden: ${state.hiddenApplicantsCount} candidates under 70%</span>
                            </div>
                        ` : ''}
                    </div>
                </div>

                <!-- Right: Detail Drawer -->
                <div class="detail-drawer ${state.drawerOpen ? 'open' : ''}" id="detail-drawer">
                    ${state.selectedApplicantDetail ?
                        renderCandidateDrawer(state.selectedApplicantDetail) :
                        renderEmptyDrawer()
                    }
                </div>
            </div>
        </div>
    `;

    setupPremiumApplicantsListeners();
}

function renderEmptyDrawer() {
    return `
        <div class="drawer-empty">
            <div class="drawer-empty-content">
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#d1d5db" stroke-width="1.5">
                    <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"></path>
                    <circle cx="9" cy="7" r="4"></circle>
                    <path d="M22 21v-2a4 4 0 0 0-3-3.87"></path>
                    <path d="M16 3.13a4 4 0 0 1 0 7.75"></path>
                </svg>
                <h3>Select a candidate</h3>
                <p>Click on a candidate to view their details</p>
            </div>
        </div>
    `;
}

function renderCandidateDrawer(detail) {
    const { application, score_breakdown, insights, fit_responses, materials } = detail;

    const scoreClass = application.fit_score >= 85 ? 'excellent' :
                       application.fit_score >= 70 ? 'good' :
                       application.fit_score >= 50 ? 'fair' : 'low';

    const renderStrengthsSection = () => {
        const strengths = insights?.strengths || [];
        if (strengths.length === 0) return '<p class="no-data">No strengths data available</p>';
        return `<ul class="strengths-list">
            ${strengths.map(s => `
                <li class="strength-item">
                    <span class="strength-text">${escapeHtml(s.text)}</span>
                    <span class="evidence-source">(${s.evidence_source})</span>
                </li>
            `).join('')}
        </ul>`;
    };

    const renderRisksSection = () => {
        const risks = insights?.risks || [];
        if (risks.length === 0) return '<p class="no-data">No concerns identified</p>';
        return `<ul class="risks-list">
            ${risks.map(r => `
                <li class="risk-item">
                    <span class="risk-text">${escapeHtml(r.text)}</span>
                    <span class="evidence-source">(${r.evidence_source})</span>
                </li>
            `).join('')}
        </ul>`;
    };

    const renderSuggestedQuestions = () => {
        const questions = insights?.suggested_questions || [];
        if (questions.length === 0) return '<p class="no-data">Complete interview for suggestions</p>';
        return `<ol class="interview-questions-list">
            ${questions.map(q => `
                <li class="interview-question">
                    <span class="question-text">"${escapeHtml(q.question)}"</span>
                    <span class="question-rationale">${escapeHtml(q.rationale || '')}</span>
                </li>
            `).join('')}
        </ol>`;
    };

    const renderFitResponsesDetailed = () => {
        if (!fit_responses || fit_responses.length === 0) return '<p class="no-data">No responses</p>';

        const mcResponses = fit_responses.filter(r => r.question_type === 'multiple_choice');
        const frResponses = fit_responses.filter(r => r.question_type === 'free_response');

        return `
            ${mcResponses.length > 0 ? `
                <div class="fit-responses-drawer-mc">
                    ${mcResponses.map(r => `
                        <div class="fit-response-drawer-item">
                            <div class="fit-response-q-drawer">${escapeHtml(r.question_text)}</div>
                            <div class="fit-response-a-drawer">
                                <span class="fit-response-badge-drawer">${r.response_value}</span>
                                ${escapeHtml(r.response_label || '')}
                            </div>
                        </div>
                    `).join('')}
                </div>
            ` : ''}
            ${frResponses.length > 0 ? `
                <div class="fit-responses-drawer-fr">
                    ${frResponses.map(r => `
                        <div class="fit-response-drawer-item">
                            <div class="fit-response-q-drawer">${escapeHtml(r.question_text)}</div>
                            <div class="fit-response-text-drawer">"${escapeHtml(r.response_text || '')}"</div>
                        </div>
                    `).join('')}
                </div>
            ` : ''}
        `;
    };

    const renderTranscriptSection = () => {
        const highlights = insights?.interview_highlights || [];
        const transcript = application.interview_transcript || [];

        if (!materials?.has_interview) {
            return '<p class="no-data">Interview not completed</p>';
        }

        return `
            ${highlights.length > 0 ? `
                <div class="transcript-highlights">
                    <h4>Key Moments</h4>
                    ${highlights.slice(0, 3).map(h => `
                        <div class="highlight-item ${h.type || 'neutral'}">
                            <span class="highlight-quote">"${escapeHtml(h.quote)}"</span>
                            ${h.competency ? `<span class="highlight-context">Re: ${escapeHtml(h.competency)}</span>` : ''}
                        </div>
                    `).join('')}
                </div>
            ` : ''}
            ${transcript.length > 0 ? `
                <div class="full-transcript-toggle">
                    <button class="btn btn-small btn-secondary" id="expand-transcript">
                        Show Full Transcript (${transcript.length} exchanges)
                    </button>
                </div>
                <div class="full-transcript" id="full-transcript" style="display:none;">
                    <input type="text" class="transcript-search" placeholder="Search transcript..." id="transcript-search">
                    <div class="transcript-messages">
                        ${transcript.map(entry => `
                            <div class="transcript-entry ${entry.speaker || 'unknown'}">
                                <span class="speaker-label">${entry.speaker === 'interviewer' ? 'AI' : 'Candidate'}:</span>
                                <span class="message-text">${escapeHtml(entry.text || entry.content || '')}</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}
        `;
    };

    return `
        <div class="drawer-content">
            <!-- Header with Score -->
            <div class="drawer-header">
                <button class="drawer-close" id="close-drawer">×</button>
                <div class="drawer-score-section">
                    <div class="big-score ${scoreClass}">
                        <span class="score-number">${application.fit_score ?? '—'}</span>
                        <span class="score-label">Fit Score</span>
                    </div>
                    ${application.confidence_level ? `
                        <span class="confidence-badge ${application.confidence_level}">
                            ${application.confidence_level.charAt(0).toUpperCase() + application.confidence_level.slice(1)} Confidence
                        </span>
                    ` : ''}
                </div>
                <div class="drawer-candidate-info">
                    <h2>${escapeHtml(application.full_name || application.email)}</h2>
                    <p>${escapeHtml(application.email)}</p>
                </div>
            </div>

            <!-- Strengths Section -->
            <div class="drawer-section">
                <h3 class="section-title">Strengths</h3>
                ${renderStrengthsSection()}
            </div>

            <!-- Risks Section -->
            <div class="drawer-section">
                <h3 class="section-title">Risks / Gaps</h3>
                ${renderRisksSection()}
            </div>

            <!-- Suggested Interview Focus -->
            <div class="drawer-section">
                <h3 class="section-title">Suggested Interview Focus</h3>
                ${renderSuggestedQuestions()}
            </div>

            <!-- Materials Section (Collapsible) -->
            <div class="drawer-section materials-section">
                <h3 class="section-title">Materials</h3>

                <!-- Resume -->
                <div class="material-item collapsible ${materials?.has_resume ? '' : 'disabled'}">
                    <div class="material-header" data-material="resume">
                        <span class="material-icon">📄</span>
                        <span class="material-label">Resume</span>
                        <span class="material-toggle">▼</span>
                    </div>
                    <div class="material-content" id="resume-content" style="display:none;">
                        ${materials?.has_resume ? `
                            <iframe src="${API_BASE}/employer/view-resume/${application.application_id}?token=${state.token}"
                                    class="resume-iframe" title="Resume"></iframe>
                            <a href="${API_BASE}/employer/download-resume/${application.application_id}?token=${state.token}"
                               class="btn btn-small btn-secondary" download style="margin-top:8px;">
                                Download Resume
                            </a>
                        ` : '<p>No resume uploaded</p>'}
                    </div>
                </div>

                <!-- Short Answers -->
                <div class="material-item collapsible">
                    <div class="material-header" data-material="answers">
                        <span class="material-icon">✍️</span>
                        <span class="material-label">Short Answers (${fit_responses?.length || 0})</span>
                        <span class="material-toggle">▼</span>
                    </div>
                    <div class="material-content" id="answers-content" style="display:none;">
                        ${renderFitResponsesDetailed()}
                    </div>
                </div>

                <!-- Interview Transcript -->
                <div class="material-item collapsible ${materials?.has_interview ? '' : 'disabled'}">
                    <div class="material-header" data-material="transcript">
                        <span class="material-icon">🎙️</span>
                        <span class="material-label">Interview Transcript</span>
                        <span class="material-toggle">▼</span>
                    </div>
                    <div class="material-content" id="transcript-content" style="display:none;">
                        ${renderTranscriptSection()}
                    </div>
                </div>
            </div>
        </div>
    `;
}

async function loadApplicantDetail(applicationId) {
    try {
        const data = await api(`/employer/applicants/${applicationId}/detail`);
        state.selectedApplicantDetail = data;
        state.drawerOpen = true;

        // Update drawer content
        const drawer = document.getElementById('detail-drawer');
        if (drawer) {
            drawer.innerHTML = renderCandidateDrawer(data);
            drawer.classList.add('open');
        }

        // Update card selection
        document.querySelectorAll('.premium-candidate-card').forEach(c => {
            c.classList.toggle('selected',
                parseInt(c.dataset.applicationId) === applicationId);
        });

        // Re-setup drawer listeners
        setupDrawerListeners();

    } catch (error) {
        console.error('Error loading applicant detail:', error);
        alert('Failed to load candidate details');
    }
}

function setupPremiumApplicantsListeners() {
    setupEmployerNavListeners();

    // Back button
    document.getElementById('back-to-employer')?.addEventListener('click', () => {
        state.drawerOpen = false;
        state.selectedApplicantDetail = null;
        navigate('employer');
    });

    // Hidden toggle
    document.getElementById('toggle-hidden')?.addEventListener('click', async () => {
        state.showHiddenApplicants = !state.showHiddenApplicants;
        await loadApplicants(state.selectedEmployerRole?.id);
    });

    // Candidate card clicks
    document.querySelectorAll('.premium-candidate-card').forEach(card => {
        card.addEventListener('click', async () => {
            const appId = parseInt(card.dataset.applicationId);
            await loadApplicantDetail(appId);
        });
    });

    // Filter pills
    document.querySelectorAll('.filter-pill').forEach(pill => {
        pill.addEventListener('click', async () => {
            const filterType = pill.dataset.filter;
            const value = pill.dataset.value;

            if (filterType === 'seniority') {
                const idx = state.employerFilters.seniority.indexOf(value);
                if (idx > -1) {
                    state.employerFilters.seniority.splice(idx, 1);
                } else {
                    state.employerFilters.seniority.push(value);
                }
                await loadApplicants(state.selectedEmployerRole?.id);
            }
        });
    });

    // Clear filters
    document.getElementById('clear-filters')?.addEventListener('click', async () => {
        state.employerFilters = { seniority: [], minScore: 70 };
        await loadApplicants(state.selectedEmployerRole?.id);
    });

    setupDrawerListeners();
}

function setupDrawerListeners() {
    // Drawer close
    document.getElementById('close-drawer')?.addEventListener('click', () => {
        state.drawerOpen = false;
        state.selectedApplicantDetail = null;
        const drawer = document.getElementById('detail-drawer');
        if (drawer) {
            drawer.classList.remove('open');
            drawer.innerHTML = renderEmptyDrawer();
        }
        document.querySelectorAll('.premium-candidate-card').forEach(c => c.classList.remove('selected'));
    });

    // Material collapsibles
    document.querySelectorAll('.material-header').forEach(header => {
        header.addEventListener('click', () => {
            const material = header.dataset.material;
            const content = document.getElementById(`${material}-content`);
            if (content) {
                const isExpanded = content.style.display !== 'none';
                content.style.display = isExpanded ? 'none' : 'block';
                const toggle = header.querySelector('.material-toggle');
                if (toggle) toggle.textContent = isExpanded ? '▼' : '▲';
            }
        });
    });

    // Expand full transcript
    document.getElementById('expand-transcript')?.addEventListener('click', () => {
        const full = document.getElementById('full-transcript');
        const toggle = document.getElementById('expand-transcript');
        if (full && toggle) {
            const isExpanded = full.style.display !== 'none';
            full.style.display = isExpanded ? 'none' : 'block';
            toggle.textContent = isExpanded ? `Show Full Transcript` : 'Hide Full Transcript';
        }
    });

    // Transcript search
    document.getElementById('transcript-search')?.addEventListener('input', (e) => {
        const query = e.target.value.toLowerCase();
        document.querySelectorAll('.transcript-entry').forEach(entry => {
            const text = entry.textContent.toLowerCase();
            entry.style.display = text.includes(query) || query === '' ? 'block' : 'none';
        });
    });
}

function renderEmployerHeader() {
    return `
        <header>
            <div class="container">
                <a href="#" class="logo" id="logo-link">Short<span>List</span></a>
                <nav>
                    <a href="#" class="${state.currentPage === 'employer' || state.currentPage === 'applicants' ? 'active' : ''}" data-nav="employer">Dashboard</a>
                    <a href="#" id="logout-link">Sign Out</a>
                </nav>
            </div>
        </header>
    `;
}

function setupEmployerNavListeners() {
    document.getElementById('logo-link')?.addEventListener('click', (e) => {
        e.preventDefault();
        navigate('home');
    });

    document.querySelectorAll('[data-nav]').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            navigate(link.dataset.nav);
        });
    });

    document.getElementById('logout-link')?.addEventListener('click', (e) => {
        e.preventDefault();
        logout();
    });
}

function renderHeader() {
    // Get user initials for avatar
    const initials = state.user ?
        ((state.user.first_name?.[0] || '') + (state.user.last_name?.[0] || '')).toUpperCase() ||
        (state.user.email?.[0] || 'U').toUpperCase()
        : 'U';

    return `
        <header>
            <div class="container">
                <a href="#" class="logo" id="logo-link">Short<span>List</span></a>
                <nav>
                    <a href="#" class="${state.currentPage === 'for-you' ? 'active' : ''}" data-nav="for-you">For You</a>
                    <a href="#" class="${state.currentPage === 'explore' || state.currentPage === 'browse' ? 'active' : ''}" data-nav="explore">Explore</a>
                    <a href="#" class="${state.currentPage === 'shortlist' ? 'active' : ''}" data-nav="shortlist">My Shortlist</a>
                    <div class="profile-avatar ${state.currentPage === 'profile' ? 'active' : ''}" data-nav="profile" title="Profile">
                        ${initials}
                    </div>
                </nav>
            </div>
        </header>
    `;
}

function setupNavListeners() {
    document.getElementById('logo-link')?.addEventListener('click', (e) => {
        e.preventDefault();
        navigate('home');
    });

    document.querySelectorAll('[data-nav]').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            navigate(link.dataset.nav);
        });
    });

    document.getElementById('logout-link')?.addEventListener('click', (e) => {
        e.preventDefault();
        logout();
    });
}

// Success Modal
function showSuccessModal(title, message) {
    const modal = document.createElement('div');
    modal.className = 'modal-overlay';
    modal.innerHTML = `
        <div class="modal" style="max-width:400px;">
            <div class="modal-header">
                <h2>${escapeHtml(title)}</h2>
                <button class="modal-close">&times;</button>
            </div>
            <div class="modal-body" style="text-align:center;padding:24px;">
                <div style="font-size:48px;margin-bottom:16px;">&#10003;</div>
                <p style="color:#525252;">${escapeHtml(message)}</p>
            </div>
            <div class="modal-footer" style="justify-content:center;">
                <button class="btn btn-primary" id="success-ok">View My Shortlists</button>
            </div>
        </div>
    `;

    document.body.appendChild(modal);

    const closeModal = () => {
        modal.remove();
        navigate('shortlist');
    };

    modal.querySelector('.modal-close').addEventListener('click', closeModal);
    modal.querySelector('#success-ok').addEventListener('click', closeModal);
}

// Upload Modal
function showUploadModal(applicationId) {
    const modal = document.createElement('div');
    modal.className = 'modal-overlay';
    modal.innerHTML = `
        <div class="modal">
            <div class="modal-header">
                <h2>Upload Your Resume or CV</h2>
                <button class="modal-close">&times;</button>
            </div>
            <div class="modal-body">
                <p style="margin-bottom:8px;color:#525252;">Upload your resume or CV to complete your application.</p>
                <p style="margin-bottom:16px;font-size:13px;color:#737373;">Your document will be saved and automatically used for future applications.</p>
                <div id="upload-error"></div>
                <div class="file-upload" id="file-upload-area">
                    <input type="file" id="resume-file" accept=".pdf">
                    <div class="file-upload-text">
                        <strong>Click to upload</strong> or drag and drop<br>
                        PDF files only
                    </div>
                </div>
                <div id="file-name" style="margin-top:12px;font-size:13px;color:#525252;"></div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" id="skip-upload">Skip for now</button>
                <button class="btn btn-primary" id="submit-upload" disabled>Submit Application</button>
            </div>
        </div>
    `;

    document.body.appendChild(modal);

    let selectedFile = null;

    const fileInput = modal.querySelector('#resume-file');
    const uploadArea = modal.querySelector('#file-upload-area');
    const fileName = modal.querySelector('#file-name');
    const submitBtn = modal.querySelector('#submit-upload');

    uploadArea.addEventListener('click', () => fileInput.click());

    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length) {
            selectedFile = e.target.files[0];
            fileName.textContent = selectedFile.name;
            uploadArea.classList.add('has-file');
            submitBtn.disabled = false;
        }
    });

    modal.querySelector('.modal-close').addEventListener('click', () => {
        modal.remove();
        navigate('shortlist');
    });

    modal.querySelector('#skip-upload').addEventListener('click', () => {
        modal.remove();
        navigate('shortlist');
    });

    submitBtn.addEventListener('click', async () => {
        if (!selectedFile) return;

        submitBtn.disabled = true;
        submitBtn.textContent = 'Uploading...';

        try {
            await uploadResume(applicationId, selectedFile);
            // Update user state to reflect they now have a resume
            if (state.user) {
                state.user.has_resume = true;
            }
            modal.remove();
            navigate('shortlist');
        } catch (err) {
            modal.querySelector('#upload-error').innerHTML =
                `<div class="alert alert-error">${err.message}</div>`;
            submitBtn.disabled = false;
            submitBtn.textContent = 'Submit Application';
        }
    });
}

// Fit Questions Modal
function showFitQuestionsModal(applicationId, questions, hasResume) {
    const modal = document.createElement('div');
    modal.className = 'modal-overlay';

    // Separate MC and free response questions
    const mcQuestions = questions.filter(q => q.question_type === 'multiple_choice');
    const frQuestions = questions.filter(q => q.question_type === 'free_response');

    // State for responses
    const responses = {};

    // Build question HTML
    const buildMCQuestion = (q, index) => `
        <div class="fit-question" data-question-id="${q.id}">
            <div class="fit-question-number">${index + 1}</div>
            <div class="fit-question-content">
                <p class="fit-question-text">${escapeHtml(q.question_text)}</p>
                <div class="fit-options">
                    ${q.options.map(opt => `
                        <label class="fit-option">
                            <input type="radio" name="q_${q.id}" value="${opt.value}">
                            <span class="fit-option-label">${opt.value}</span>
                            <span class="fit-option-text">${escapeHtml(opt.label)}</span>
                        </label>
                    `).join('')}
                </div>
            </div>
        </div>
    `;

    const buildFRQuestion = (q, index) => `
        <div class="fit-question fit-question-fr" data-question-id="${q.id}">
            <div class="fit-question-number">${mcQuestions.length + index + 1}</div>
            <div class="fit-question-content">
                <p class="fit-question-text">${escapeHtml(q.question_text)}</p>
                <textarea class="fit-textarea" name="q_${q.id}" placeholder="Your answer..." rows="3"></textarea>
            </div>
        </div>
    `;

    modal.innerHTML = `
        <div class="modal fit-questions-modal">
            <div class="modal-header">
                <h2>A few quick questions</h2>
                <p class="modal-subtitle">Help us understand your work style and preferences</p>
            </div>
            <div class="modal-body fit-questions-body">
                <div class="fit-questions-section">
                    ${mcQuestions.map((q, i) => buildMCQuestion(q, i)).join('')}
                </div>
                ${frQuestions.length > 0 ? `
                    <div class="fit-questions-divider">
                        <span>Short Answer</span>
                    </div>
                    <div class="fit-questions-section">
                        ${frQuestions.map((q, i) => buildFRQuestion(q, i)).join('')}
                    </div>
                ` : ''}
            </div>
            <div class="modal-footer">
                <div class="fit-progress">
                    <span id="fit-answered">0</span> of ${questions.length} answered
                </div>
                <button class="btn btn-primary" id="submit-fit" disabled>Continue</button>
            </div>
        </div>
    `;

    document.body.appendChild(modal);

    // Track responses
    const updateProgress = () => {
        const answered = Object.keys(responses).length;
        modal.querySelector('#fit-answered').textContent = answered;
        modal.querySelector('#submit-fit').disabled = answered < questions.length;
    };

    // MC radio listeners
    modal.querySelectorAll('.fit-options input[type="radio"]').forEach(radio => {
        radio.addEventListener('change', (e) => {
            const questionId = e.target.closest('.fit-question').dataset.questionId;
            responses[questionId] = { question_id: questionId, response_value: e.target.value };
            updateProgress();
        });
    });

    // FR textarea listeners
    modal.querySelectorAll('.fit-textarea').forEach(textarea => {
        textarea.addEventListener('input', (e) => {
            const questionId = e.target.closest('.fit-question').dataset.questionId;
            const value = e.target.value.trim();
            if (value) {
                responses[questionId] = { question_id: questionId, response_text: value };
            } else {
                delete responses[questionId];
            }
            updateProgress();
        });
    });

    // Submit button
    modal.querySelector('#submit-fit').addEventListener('click', async () => {
        const submitBtn = modal.querySelector('#submit-fit');
        submitBtn.disabled = true;
        submitBtn.textContent = 'Submitting...';

        try {
            const responseArray = Object.values(responses);
            const result = await api(`/shortlist/submit-fit-responses/${applicationId}`, {
                method: 'POST',
                body: JSON.stringify({ responses: responseArray })
            });

            modal.remove();

            // Check if resume is needed
            if (result.needs_resume) {
                showUploadModal(applicationId);
            } else {
                showSuccessModal('Application Submitted!', 'Your responses have been recorded and your resume was automatically attached.');
            }
        } catch (err) {
            submitBtn.disabled = false;
            submitBtn.textContent = 'Continue';
            alert('Failed to submit responses: ' + err.message);
        }
    });
}

// Utility Functions
function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function formatDescription(text) {
    if (!text) return '';

    // Escape HTML first for security
    let html = escapeHtml(text);

    // Convert **bold** to <strong>
    html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

    // Split into lines
    const lines = html.split('\n');
    let result = [];
    let inList = false;

    for (let i = 0; i < lines.length; i++) {
        let line = lines[i].trim();

        // Skip empty lines but close list if open
        if (!line) {
            if (inList) {
                result.push('</ul>');
                inList = false;
            }
            continue;
        }

        // Check if line is a bullet point
        if (line.startsWith('•') || line.startsWith('-') || line.startsWith('*')) {
            // Remove bullet character and trim
            let content = line.replace(/^[•\-\*]\s*/, '').trim();

            // Start list if not already in one
            if (!inList) {
                result.push('<ul>');
                inList = true;
            }
            result.push(`<li>${content}</li>`);
        } else {
            // Close list if open
            if (inList) {
                result.push('</ul>');
                inList = false;
            }

            // Check if it's a section header (ends with : and is short, or contains <strong>)
            if ((line.endsWith(':') && line.length < 50) || line.includes('<strong>')) {
                result.push(`<p><strong>${line.replace(/<\/?strong>/g, '')}</strong></p>`);
            } else {
                result.push(`<p>${line}</p>`);
            }
        }
    }

    // Close list if still open
    if (inList) {
        result.push('</ul>');
    }

    return result.join('\n');
}

function formatExpLevel(level) {
    const labels = {
        'intern': 'Intern',
        'entry': 'Entry Level',
        'mid': 'Mid Level',
        'senior': 'Senior'
    };
    return labels[level] || level;
}

function formatWorkPref(pref) {
    const labels = {
        'remote': 'Remote',
        'hybrid': 'Hybrid',
        'onsite': 'On-site'
    };
    return labels[pref] || pref;
}

function formatSalary(num) {
    if (!num) return '';
    return (num / 1000).toFixed(0) + 'k';
}

function formatDate(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

// ============================================================================
// PREMIUM UI ENHANCEMENTS
// ============================================================================

// Scroll reveal animation observer
function initScrollReveal() {
    const revealElements = document.querySelectorAll('.reveal, .reveal-stagger, .reveal-hero');
    if (revealElements.length === 0) return;

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('visible');
                observer.unobserve(entry.target);
            }
        });
    }, {
        threshold: 0.1,
        rootMargin: '0px 0px -50px 0px'
    });

    revealElements.forEach(el => observer.observe(el));
}

// Filter pill interaction animations
function initFilterPillAnimations() {
    const filterPills = document.querySelectorAll('.preview-filter-pill');
    if (filterPills.length === 0) return;

    let activeIndex = 0;
    const pillCount = filterPills.length;

    // Cycle through filters periodically
    setInterval(() => {
        filterPills.forEach(pill => {
            pill.classList.remove('active');
        });
        activeIndex = (activeIndex + 1) % pillCount;
        filterPills[activeIndex].classList.add('active');
    }, 3000);
}

// Parallax effects disabled - keeping interactions snappy
function initParallaxEffects() {
    // Parallax removed for utility-first design
    // Keeping function stub to avoid breaking initPremiumUI
}

// Walkthrough section animation triggers
function initWalkthroughAnimations() {
    const walkthroughSteps = document.querySelectorAll('.walkthrough-step');
    if (walkthroughSteps.length === 0) return;

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('animate');
                // Trigger match bar animations
                const matchBars = entry.target.querySelectorAll('.match-bar-fill');
                matchBars.forEach(bar => {
                    bar.style.animationPlayState = 'running';
                });
                observer.unobserve(entry.target);
            }
        });
    }, {
        threshold: 0.3,
        rootMargin: '0px 0px -100px 0px'
    });

    walkthroughSteps.forEach(step => observer.observe(step));
}

// Counter animation for stats
function initCounterAnimations() {
    const counters = document.querySelectorAll('[data-counter]');
    if (counters.length === 0) return;

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const counter = entry.target;
                const target = parseInt(counter.dataset.counter, 10);
                const duration = 2000;
                const start = 0;
                const startTime = performance.now();

                function updateCounter(currentTime) {
                    const elapsed = currentTime - startTime;
                    const progress = Math.min(elapsed / duration, 1);

                    // Ease out cubic
                    const easeOut = 1 - Math.pow(1 - progress, 3);
                    const current = Math.round(start + (target - start) * easeOut);

                    counter.textContent = current.toLocaleString();

                    if (progress < 1) {
                        requestAnimationFrame(updateCounter);
                    }
                }

                requestAnimationFrame(updateCounter);
                observer.unobserve(counter);
            }
        });
    }, { threshold: 0.5 });

    counters.forEach(counter => observer.observe(counter));
}

// Header scroll state
function initHeaderScroll() {
    const header = document.querySelector('header, .landing-header');
    if (!header) return;

    let ticking = false;

    function updateHeader() {
        if (window.scrollY > 20) {
            header.classList.add('scrolled');
        } else {
            header.classList.remove('scrolled');
        }
        ticking = false;
    }

    window.addEventListener('scroll', () => {
        if (!ticking) {
            window.requestAnimationFrame(updateHeader);
            ticking = true;
        }
    }, { passive: true });

    updateHeader();
}

// Magnetic effect disabled - keeping interactions snappy
function initMagneticButtons() {
    // Magnetic hover removed for utility-first design
    // State changes should snap into place
}

// Initialize all premium UI enhancements
function initPremiumUI() {
    requestAnimationFrame(() => {
        initScrollReveal();
        initHeaderScroll();
        initMagneticButtons();
        initFilterPillAnimations();
        initParallaxEffects();
        initWalkthroughAnimations();
        initCounterAnimations();
    });
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    initPremiumUI();
});

// Re-initialize after navigations
const _originalNavigate = navigate;
navigate = function(page) {
    _originalNavigate(page);
    initPremiumUI();
};
