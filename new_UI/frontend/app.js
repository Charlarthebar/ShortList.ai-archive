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
    if (!state.token) {
        navigate('home');
        return;
    }

    try {
        const data = await api('/auth/me');
        state.user = data.user;

        // Route based on user type
        if (state.user.user_type === 'employer') {
            navigate('employer');
        } else if (!state.user.profile_complete) {
            navigate('setup');
        } else {
            navigate('browse');
        }
    } catch (err) {
        localStorage.removeItem('shortlist_token');
        state.token = null;
        navigate('login');
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
    navigate('login');
}

async function saveProfile(experienceLevel, workPreference) {
    await api('/profile', {
        method: 'PUT',
        body: JSON.stringify({
            experience_level: experienceLevel,
            work_preference: workPreference
        })
    });

    state.user.profile_complete = true;
    state.user.experience_level = experienceLevel;
    state.user.work_preference = workPreference;
    navigate('browse');
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

        // Show upload modal
        showUploadModal(data.application_id);
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
    const data = await api(`/employer/roles/${roleId}/applicants`);
    state.applicants = data.applicants;
    state.selectedEmployerRole = state.employerRoles.find(r => r.id === roleId);
    renderApplicantsList();
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
    app.innerHTML = `
        <div class="landing-page">
            <!-- Header -->
            <header class="landing-header">
                <div class="container">
                    <a href="#" class="logo" id="landing-logo">Short<span>List</span></a>
                    <nav class="landing-nav">
                        <a href="#how-it-works" id="nav-how-it-works">How It Works</a>
                        <a href="#" class="btn btn-secondary btn-small" id="nav-login">Sign In</a>
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
                        <button class="btn btn-gradient" id="cta-get-started">Get Started</button>
                        <button class="btn btn-outline" id="cta-hiring">I'm Hiring</button>
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
                    <h2>Ready to get started?</h2>
                    <p>Join ShortList and find your next opportunity before everyone else.</p>
                    <div class="hero-ctas">
                        <button class="btn btn-gradient" id="cta-get-started-bottom">Get Started</button>
                        <button class="btn btn-outline" id="cta-hiring-bottom">I'm Hiring</button>
                    </div>
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

    document.getElementById('nav-login').addEventListener('click', (e) => {
        e.preventDefault();
        navigate('login');
    });

    // CTA buttons - Get Started goes to seeker signup
    document.getElementById('cta-get-started').addEventListener('click', () => {
        navigate('signup');
    });
    document.getElementById('cta-get-started-bottom').addEventListener('click', () => {
        navigate('signup');
    });

    // I'm Hiring buttons - go to employer signup
    document.getElementById('cta-hiring').addEventListener('click', () => {
        navigateEmployerSignup();
    });
    document.getElementById('cta-hiring-bottom').addEventListener('click', () => {
        navigateEmployerSignup();
    });
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
        <div class="profile-setup">
            <div class="profile-box">
                <h1>Complete your profile</h1>
                <p>Tell us a bit about yourself so we can match you with the right roles.</p>
                <div id="setup-error"></div>
                <form id="setup-form">
                    <div class="form-group">
                        <label>Experience Level</label>
                        <div class="option-group" id="exp-level-options">
                            <button type="button" class="option-btn" data-value="intern">Intern</button>
                            <button type="button" class="option-btn" data-value="entry">Entry Level</button>
                            <button type="button" class="option-btn" data-value="mid">Mid Level</button>
                            <button type="button" class="option-btn" data-value="senior">Senior</button>
                        </div>
                        <input type="hidden" id="setup-exp-level" required>
                    </div>
                    <div class="form-group">
                        <label>Work Preference</label>
                        <div class="option-group" id="work-pref-options">
                            <button type="button" class="option-btn" data-value="remote">Remote</button>
                            <button type="button" class="option-btn" data-value="hybrid">Hybrid</button>
                            <button type="button" class="option-btn" data-value="onsite">On-site</button>
                        </div>
                        <input type="hidden" id="setup-work-pref" required>
                    </div>
                    <button type="submit" class="btn btn-primary" style="width:100%;margin-top:24px;">Continue</button>
                </form>
            </div>
        </div>
    `;

    // Handle option selection
    document.querySelectorAll('#exp-level-options .option-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#exp-level-options .option-btn').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
            document.getElementById('setup-exp-level').value = btn.dataset.value;
        });
    });

    document.querySelectorAll('#work-pref-options .option-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#work-pref-options .option-btn').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
            document.getElementById('setup-work-pref').value = btn.dataset.value;
        });
    });

    document.getElementById('setup-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const expLevel = document.getElementById('setup-exp-level').value;
        const workPref = document.getElementById('setup-work-pref').value;

        if (!expLevel || !workPref) {
            document.getElementById('setup-error').innerHTML =
                '<div class="alert alert-error">Please select both options to continue.</div>';
            return;
        }

        try {
            await saveProfile(expLevel, workPref);
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
                                <option value="data_analyst">Data Analyst</option>
                                <option value="data_scientist">Data Scientist</option>
                                <option value="product_manager">Product Manager</option>
                                <option value="hardware">Hardware</option>
                                <option value="security">Security Engineer</option>
                                <option value="qa">QA Engineer</option>
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

    container.innerHTML = state.roles.map(role => `
        <div class="role-card" data-role-id="${role.id}">
            <div class="role-card-header">
                <div class="company-badge">${escapeHtml(role.company_name?.charAt(0) || 'C')}</div>
                <div class="role-status-badge ${role.status === 'open' ? 'status-open' : 'status-closed'}">
                    ${role.status === 'open' ? 'Actively hiring' : 'Closed'}
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
    `).join('');

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

    app.innerHTML = `
        <div class="main-layout">
            ${renderEmployerHeader()}
            <div class="container page-content">
                <button class="btn btn-secondary btn-small" id="back-to-employer" style="margin-bottom:24px;">← Back to Dashboard</button>
                <h1>${role ? escapeHtml(role.title) : 'Candidates'}</h1>
                <p style="color:#525252;margin-bottom:24px;">${role ? escapeHtml(role.company_name) : ''}</p>
                <div id="applicants-list">
                    <div class="loading">Loading candidates...</div>
                </div>
            </div>
        </div>
    `;

    setupEmployerNavListeners();

    document.getElementById('back-to-employer').addEventListener('click', () => {
        navigate('employer');
    });

    // Render applicants
    const container = document.getElementById('applicants-list');

    if (!state.applicants.length) {
        container.innerHTML = `
            <div class="empty-state">
                <h3>No candidates yet</h3>
                <p>Candidates who join the shortlist will appear here.</p>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <div class="applicants-table">
            <div class="applicants-header">
                <div class="col-email">Email</div>
                <div class="col-level">Experience</div>
                <div class="col-pref">Work Pref</div>
                <div class="col-status">Status</div>
                <div class="col-date">Applied</div>
                <div class="col-actions">Resume</div>
            </div>
            ${state.applicants.map(applicant => `
                <div class="applicant-row">
                    <div class="col-email">${escapeHtml(applicant.email)}</div>
                    <div class="col-level">${formatExpLevel(applicant.experience_level)}</div>
                    <div class="col-pref">${formatWorkPref(applicant.work_preference)}</div>
                    <div class="col-status"><span class="application-status ${applicant.status}">${applicant.status}</span></div>
                    <div class="col-date">${formatDate(applicant.applied_at)}</div>
                    <div class="col-actions">
                        ${applicant.resume_path
                            ? `<a href="${API_BASE}/employer/download-resume/${applicant.application_id}" class="btn btn-small btn-secondary" target="_blank">Download</a>`
                            : '<span style="color:#a3a3a3;">No resume</span>'}
                    </div>
                </div>
            `).join('')}
        </div>
    `;
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
        navigate('employer');
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
        navigate('browse');
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

// Upload Modal
function showUploadModal(applicationId) {
    const modal = document.createElement('div');
    modal.className = 'modal-overlay';
    modal.innerHTML = `
        <div class="modal">
            <div class="modal-header">
                <h2>Upload Your Resume</h2>
                <button class="modal-close">&times;</button>
            </div>
            <div class="modal-body">
                <p style="margin-bottom:16px;color:#525252;">Upload your resume to complete your application.</p>
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
