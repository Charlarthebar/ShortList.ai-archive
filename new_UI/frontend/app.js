/**
 * ShortList - Minimal Frontend Application
 * Clean, vanilla JavaScript with no framework dependencies.
 */

const API_BASE = 'http://localhost:5002/api';

// State
const state = {
    user: null,
    token: localStorage.getItem('shortlist_token'),
    currentPage: 'loading',
    roles: [],
    myApplications: [],
    selectedRole: null,
    filters: {
        search: '',
        role_type: '',
        experience_level: ''
    },
    // Employer state
    employerRoles: [],
    selectedEmployerRole: null,
    applicants: [],
    // Signup state
    signupAsEmployer: false
};

// API Helper
async function api(endpoint, options = {}) {
    const headers = {
        'Content-Type': 'application/json',
        ...(state.token ? { 'Authorization': `Bearer ${state.token}` } : {})
    };

    try {
        const response = await fetch(`${API_BASE}${endpoint}`, {
            ...options,
            headers: { ...headers, ...options.headers }
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Request failed');
        }

        return data;
    } catch (err) {
        console.error('API Error:', err);
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
    } else {
        navigate('browse');
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

    state.user.profile_complete = true;
    state.user.preferences = preferences;
    navigate('browse');
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

        const data = await api(`/roles?${params}`);
        state.roles = data.roles || [];
        renderRolesList();
    } catch (err) {
        console.error('Failed to load roles:', err);
        state.roles = [];
        renderRolesList();
    }
}

async function loadRole(roleId) {
    const data = await api(`/roles/${roleId}`);
    state.selectedRole = data.role;
    renderRoleDetail();
}

// Shortlist Functions
async function applyToShortlist(roleId) {
    try {
        const data = await api('/shortlist/apply', {
            method: 'POST',
            body: JSON.stringify({ role_id: roleId })
        });

        // Always show fit questions first (they are required)
        if (data.questions && data.questions.length > 0) {
            showFitQuestionsModal(data.application_id, data.questions, data.has_resume);
        } else {
            // Fallback: No questions configured, proceed directly
            if (data.has_resume) {
                showSuccessModal('Application Submitted!', 'Your resume was automatically attached from your profile.');
            } else {
                showUploadModal(data.application_id);
            }
        }
    } catch (err) {
        if (err.message.includes('INCOMPLETE_PROFILE')) {
            navigate('setup');
        } else {
            alert(err.message);
        }
    }
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

async function loadMyApplications() {
    const data = await api('/shortlist/my-applications');
    state.myApplications = data.applications;
    renderMyShortlist();
}

// Employer Functions
async function loadEmployerRoles() {
    const data = await api('/employer/roles');
    state.employerRoles = data.roles;
    renderEmployerDashboard();
}

async function loadApplicants(roleId) {
    try {
        const data = await api(`/employer/roles/${roleId}/applicants`);
        state.applicants = data.applicants || [];
        state.selectedEmployerRole = state.employerRoles.find(r => r.id === roleId);
        renderApplicantsList();
    } catch (err) {
        console.error('Failed to load applicants:', err);
        alert('Failed to load candidates. Please try again.');
    }
}

// Navigation
function navigate(page) {
    state.currentPage = page;
    render();

    // Load data for specific pages
    if (page === 'browse') {
        loadRoles();
    } else if (page === 'shortlist') {
        loadMyApplications();
    } else if (page === 'employer') {
        loadEmployerRoles();
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
        case 'browse':
            renderBrowse();
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
                <div class="container">
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

                    <!-- What else are you looking for -->
                    <div class="pref-section">
                        <label class="pref-label">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"></path>
                                <polyline points="14 2 14 8 20 8"></polyline>
                                <line x1="16" y1="13" x2="8" y2="13"></line>
                                <line x1="16" y1="17" x2="8" y2="17"></line>
                            </svg>
                            Anything else you're looking for?
                        </label>
                        <textarea id="pref-text" placeholder="e.g., &quot;I want to work on AI/ML products at a startup with a strong engineering culture.&quot;" rows="3"></textarea>
                        <div class="pref-hint" style="margin-top:6px;font-size:12px;color:#737373;">We'll use AI to match you with jobs that fit what you describe.</div>
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
        const preferencesText = document.getElementById('pref-text').value.trim();

        const preferences = {
            preferred_locations: selectedLocations.length > 0 ? selectedLocations : null,
            salary_min: salaryMin,
            salary_max: salaryMax,
            open_to_roles: selectedRoles.length > 0 ? selectedRoles : null,
            experience_level: selectedExpLevel,
            work_arrangement: selectedWorkArrangement,
            preferences_text: preferencesText || null
        };

        try {
            await savePreferences(preferences);
        } catch (err) {
            document.getElementById('setup-error').innerHTML =
                `<div class="alert alert-error">${err.message}</div>`;
        }
    });
}

function renderBrowse() {
    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="browse-layout">
            ${renderHeader()}
            <div class="browse-hero">
                <div class="container">
                    <h1>Explore opportunities</h1>
                    <p>Discover roles at Boston's top companies before they go public</p>
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
        // Determine match score display
        let matchScoreHtml = '';
        if (role.match_score !== null && role.match_score !== undefined) {
            const scoreClass = role.match_score >= 75 ? 'high' : role.match_score >= 50 ? 'medium' : 'low';
            matchScoreHtml = `<div class="match-score ${scoreClass}">${role.match_score}% match</div>`;
        }

        return `
            <div class="role-card" data-role-id="${role.id}">
                <div class="role-card-header">
                    <div class="company-badge">${escapeHtml(role.company_name?.charAt(0) || 'C')}</div>
                    <div class="role-header-right">
                        ${matchScoreHtml}
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

    // Add click listeners
    container.querySelectorAll('.role-card').forEach(card => {
        card.addEventListener('click', (e) => {
            if (!e.target.classList.contains('view-role-btn')) {
                const roleId = card.dataset.roleId;
                loadRole(roleId);
                state.currentPage = 'role';
            }
        });
    });

    container.querySelectorAll('.view-role-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const roleId = btn.dataset.roleId;
            loadRole(roleId);
            state.currentPage = 'role';
        });
    });
}

function renderRoleDetail() {
    const role = state.selectedRole;
    if (!role) {
        navigate('browse');
        return;
    }

    const app = document.getElementById('app');
    app.innerHTML = `
        <div class="main-layout">
            ${renderHeader()}
            <div class="container page-content">
                <button class="btn btn-secondary btn-small" id="back-to-browse" style="margin-bottom:24px;">← Back to Browse</button>
                <div class="role-detail">
                    <div class="role-detail-header">
                        <h1>${escapeHtml(role.title)}</h1>
                        <div class="company">${escapeHtml(role.company_name)}</div>
                    </div>
                    <div class="role-detail-meta">
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
                        <div class="role-description">${role.description}</div>
                    </div>
                    ` : ''}
                    <div class="role-detail-actions">
                        <button class="btn btn-primary" id="join-shortlist-btn">Join the Shortlist</button>
                    </div>
                    <p style="margin-top:12px;font-size:13px;color:#737373;">
                        ${role.applicant_count || 0} candidate${role.applicant_count !== 1 ? 's' : ''} on the shortlist
                    </p>
                </div>
            </div>
        </div>
    `;

    document.getElementById('back-to-browse').addEventListener('click', () => {
        navigate('browse');
    });

    document.getElementById('join-shortlist-btn').addEventListener('click', () => {
        applyToShortlist(role.id);
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

    container.innerHTML = state.myApplications.map(app => `
        <div class="application-card">
            <div class="application-info">
                <h3>${escapeHtml(app.title)}</h3>
                <div class="application-company">${escapeHtml(app.company_name)}</div>
                <div class="application-date">Applied ${formatDate(app.applied_at)}</div>
            </div>
            <div style="display:flex;align-items:center;gap:12px;">
                <div class="role-status">
                    <span class="status-dot ${app.role_status === 'open' ? 'open' : 'closed'}"></span>
                    Role ${app.role_status === 'open' ? 'Open' : 'Closed'}
                </div>
                <span class="application-status ${app.status}">${app.status}</span>
            </div>
        </div>
    `).join('');
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
    return `
        <header>
            <div class="container">
                <a href="#" class="logo" id="logo-link">Short<span>List</span></a>
                <nav>
                    <a href="#" class="${state.currentPage === 'browse' ? 'active' : ''}" data-nav="browse">Browse</a>
                    <a href="#" class="${state.currentPage === 'shortlist' ? 'active' : ''}" data-nav="shortlist">My Shortlist</a>
                    <a href="#" id="logout-link">Sign Out</a>
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

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
});
