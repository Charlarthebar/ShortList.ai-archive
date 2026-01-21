import React, { useState, useEffect, useRef } from 'react';

// API Configuration
const API_BASE = 'http://localhost:5001/api';

// API Helper functions
const api = {
  async fetch(endpoint, options = {}) {
    const token = localStorage.getItem('shortlist_token');
    const headers = {
      'Content-Type': 'application/json',
      ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
      ...options.headers,
    };

    const response = await fetch(`${API_BASE}${endpoint}`, {
      ...options,
      headers,
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: 'Request failed' }));
      throw new Error(error.error || 'Request failed');
    }

    return response.json();
  },

  get: (endpoint) => api.fetch(endpoint),
  post: (endpoint, data) => api.fetch(endpoint, { method: 'POST', body: JSON.stringify(data) }),
  put: (endpoint, data) => api.fetch(endpoint, { method: 'PUT', body: JSON.stringify(data) }),
  patch: (endpoint, data) => api.fetch(endpoint, { method: 'PATCH', body: JSON.stringify(data) }),
  delete: (endpoint) => api.fetch(endpoint, { method: 'DELETE' }),

  // Upload file (for resume)
  async upload(endpoint, file) {
    const token = localStorage.getItem('shortlist_token');
    const formData = new FormData();
    formData.append('file', file);

    const response = await fetch(`${API_BASE}${endpoint}`, {
      method: 'POST',
      headers: {
        ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
      },
      body: formData,
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: 'Upload failed' }));
      throw new Error(error.error || 'Upload failed');
    }

    return response.json();
  },
};

// Simple resume parser (extracts text and basic info)
const parseResume = async (file) => {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = async (e) => {
      try {
        const text = e.target.result;

        // Check if file is binary (PDF/DOC) - these will have lots of non-printable characters
        const printableRatio = (text.match(/[\x20-\x7E\n\r\t]/g) || []).length / text.length;
        if (printableRatio < 0.5) {
          // Binary file detected - can't parse client-side without special libraries
          reject(new Error('PDF_BINARY'));
          return;
        }

        // Basic extraction patterns
        const emailMatch = text.match(/[\w.-]+@[\w.-]+\.\w+/);
        const phoneMatch = text.match(/(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/);

        // Try to extract name from first lines (usually at top of resume)
        const lines = text.split('\n').filter(l => l.trim());
        let firstName = '', lastName = '';
        if (lines.length > 0) {
          const nameLine = lines[0].trim();
          const nameParts = nameLine.split(/\s+/);
          if (nameParts.length >= 2 && nameParts[0].length < 20) {
            firstName = nameParts[0];
            lastName = nameParts.slice(1).join(' ');
          }
        }

        // Extract skills by matching against common skill keywords
        const skillKeywords = ['JavaScript', 'TypeScript', 'React', 'Node.js', 'Python', 'Java', 'Go', 'Rust', 'AWS', 'GCP', 'Docker', 'Kubernetes', 'PostgreSQL', 'Machine Learning', 'Product Management', 'UX Design', 'Team Leadership', 'System Design', 'Agile', 'DevOps', 'SQL', 'MongoDB', 'Redis', 'GraphQL', 'REST', 'API', 'CI/CD', 'Git', 'Excel', 'Salesforce', 'Marketing', 'Sales', 'Finance', 'Analytics', 'Data Analysis', 'Project Management', 'Communication', 'Presentation', 'Strategy', 'Operations', 'HR', 'Recruiting', 'Customer Success'];
        const foundSkills = skillKeywords.filter(skill =>
          text.toLowerCase().includes(skill.toLowerCase())
        );

        // Try to find job titles
        const titlePatterns = [
          /(?:^|\n)([A-Za-z\s]+(?:Engineer|Developer|Designer|Manager|Director|Lead|Architect|Analyst|Consultant|Specialist|Coordinator|Executive|Officer|Associate|VP|President))/gi,
          /(?:Title|Position|Role):\s*([^\n]+)/i
        ];
        let currentTitle = '';
        for (const pattern of titlePatterns) {
          const match = text.match(pattern);
          if (match) {
            currentTitle = match[1]?.trim() || '';
            break;
          }
        }

        // Try to find company names (look for "at" or common patterns)
        const companyMatch = text.match(/(?:at|@)\s+([A-Z][A-Za-z\s&.]+?)(?:\s*[-–|,]|\n|$)/);
        const currentCompany = companyMatch ? companyMatch[1].trim() : '';

        resolve({
          firstName,
          lastName,
          email: emailMatch ? emailMatch[0] : '',
          phone: phoneMatch ? phoneMatch[1] : '',
          currentTitle,
          currentCompany,
          skills: foundSkills.slice(0, 10), // Limit to 10 skills
          rawText: text.substring(0, 5000) // Keep first 5000 chars for reference
        });
      } catch (err) {
        reject(err);
      }
    };
    reader.onerror = reject;

    // Read as text (works for .txt files)
    reader.readAsText(file);
  });
};

// Skill options for profile - SWE/Data/ML focused
const skillOptions = [
  // Languages
  'Python', 'JavaScript', 'TypeScript', 'Java', 'Go', 'Rust', 'C++', 'Scala',
  // Frontend
  'React', 'Vue.js', 'Angular', 'Next.js',
  // Backend
  'Node.js', 'Django', 'FastAPI', 'Spring',
  // Data/ML
  'Machine Learning', 'Deep Learning', 'PyTorch', 'TensorFlow', 'Data Science', 'NLP', 'Computer Vision', 'LLM/GPT',
  'Spark', 'Pandas', 'Data Engineering', 'ETL',
  // Infrastructure
  'AWS', 'GCP', 'Azure', 'Docker', 'Kubernetes', 'Terraform',
  // Databases
  'PostgreSQL', 'MongoDB', 'Redis', 'Elasticsearch',
  // Other
  'System Design', 'Distributed Systems', 'Microservices', 'GraphQL', 'Kafka'
];

// Experience level requirements for jobs
const EXPERIENCE_LEVELS = {
  'entry': { label: 'Entry Level', minYears: 0, maxYears: 2 },
  'mid': { label: 'Mid Level', minYears: 2, maxYears: 5 },
  'senior': { label: 'Senior', minYears: 5, maxYears: 8 },
  'staff': { label: 'Staff+', minYears: 8, maxYears: null }
};

export default function ShortList() {
  // Refs
  const fileInputRef = useRef(null);

  // Auth state
  const [token, setToken] = useState(() => localStorage.getItem('shortlist_token'));
  const [user, setUser] = useState(null);

  // UI state
  const [view, setView] = useState('landing');
  const [userType, setUserType] = useState('seeker');
  const [step, setStep] = useState(1);
  const [companyStep, setCompanyStep] = useState(1);
  const [tab, setTab] = useState('browse');
  const [search, setSearch] = useState('');
  const [selCand, setSelCand] = useState(null);
  const [showNotifs, setShowNotifs] = useState(false);
  const [uploadingResume, setUploadingResume] = useState(false);
  const [resumeUploaded, setResumeUploaded] = useState(false);
  const [resumeFileName, setResumeFileName] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [loginEmail, setLoginEmail] = useState('');
  const [loginPassword, setLoginPassword] = useState('');
  const [validationErrors, setValidationErrors] = useState({});

  // Data state
  const [jobs, setJobs] = useState([]);
  const [watched, setWatched] = useState([]);
  const [candidates, setCandidates] = useState([]);
  const [invited, setInvited] = useState([]);
  const [notifications, setNotifications] = useState([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [statusFilter, setStatusFilter] = useState('all'); // 'all', 'open', 'filled', 'closed'
  const [companyPositions, setCompanyPositions] = useState([]); // Positions for employer view
  const [editingPosition, setEditingPosition] = useState(null); // Position being edited
  // Shortlist state
  const [shortlistRoles, setShortlistRoles] = useState([]); // Employer's roles with shortlist stats
  const [selectedShortlistRole, setSelectedShortlistRole] = useState(null); // Currently viewing shortlist
  const [shortlistCandidates, setShortlistCandidates] = useState([]); // Candidates for selected role
  const [roleConfig, setRoleConfig] = useState(null); // Role configuration for editing
  const [showRoleConfig, setShowRoleConfig] = useState(false); // Show config modal
  const [companyProfile, setCompanyProfile] = useState({
    name: '', website: '', industry: '', size: '', description: '', logo: '',
    contactName: '', contactEmail: '', contactTitle: '', verified: false,
    locations: [], benefits: [], culture: '', password: ''
  });
  const [profile, setProfile] = useState({
    firstName: '', lastName: '', email: '', password: '', currentTitle: '', currentCompany: '',
    yearsExperience: '', searchStatus: '', skills: [], workStyle: [], salaryMin: '', salaryMax: '', idealRole: '',
    experiences: [], // Work history from resume
    techYearsExperience: 0, // Calculated tech-specific years
    expLevelFilter: [] // Filter by experience level: ['entry', 'mid', 'senior', 'staff']
  });


  // Check auth on mount
  useEffect(() => {
    if (token) {
      api.get('/auth/me')
        .then(data => {
          setUser(data.user);
          setUserType(data.user.user_type);
          setView('dashboard');
          setProfile(p => ({
            ...p,
            firstName: data.user.first_name || '',
            lastName: data.user.last_name || '',
            email: data.user.email || ''
          }));
        })
        .catch(() => {
          localStorage.removeItem('shortlist_token');
          setToken(null);
        });
    }
  }, [token]);

  // Load jobs when on browse tab
  useEffect(() => {
    if (view === 'dashboard' && (tab === 'browse' || tab === 'matches')) {
      loadJobs();
    }
  }, [view, tab, search, statusFilter]);

  // Load company positions when on positions tab
  useEffect(() => {
    if (view === 'dashboard' && userType === 'company' && tab === 'positions') {
      loadCompanyPositions();
    }
  }, [view, userType, tab]);

  // Load watchlist
  useEffect(() => {
    if (view === 'dashboard' && tab === 'watchlist' && token) {
      loadWatchlist();
    }
  }, [view, tab, token]);

  // Load notifications
  useEffect(() => {
    if (view === 'dashboard' && token) {
      loadNotifications();
    }
  }, [view, token]);

  // Load candidates for company view
  useEffect(() => {
    if (view === 'dashboard' && userType === 'company' && tab === 'talent') {
      loadCandidates();
    }
  }, [view, userType, tab, search]);

  // Load shortlist roles for company view
  useEffect(() => {
    if (view === 'dashboard' && userType === 'company' && tab === 'shortlist') {
      loadShortlistRoles();
    }
  }, [view, userType, tab]);

  // Format experience level for display
  const formatExpLevel = (level, minYears, maxYears) => {
    if (!level || level === 'any') return null;
    const labels = {
      'entry': 'Entry Level',
      'mid': 'Mid Level',
      'senior': 'Senior',
      'staff': 'Staff+'
    };
    let label = labels[level] || level;
    if (minYears !== null && minYears !== undefined) {
      if (maxYears) {
        label += ` (${minYears}-${maxYears} yrs)`;
      } else if (minYears > 0) {
        label += ` (${minYears}+ yrs)`;
      }
    }
    return label;
  };

  // API calls
  const loadJobs = async () => {
    try {
      setLoading(true);
      const params = new URLSearchParams();
      if (search) params.append('search', search);
      params.append('status', statusFilter); // Use status filter (all, open, filled, closed)
      params.append('limit', '50');

      const data = await api.get(`/positions?${params}`);
      setJobs(data.positions.map(p => ({
        id: p.id,
        title: p.title,
        company: p.company_name,
        location: p.location || 'Remote',
        salary: p.salary_range || 'Competitive',
        status: p.status,
        watchers: p.watcher_count || 0,
        matchScore: Math.floor(Math.random() * 30) + 70, // TODO: Real match scores
        dept: p.department || 'General',
        // Experience requirements from employer
        expLevel: p.experience_level,
        minYearsRequired: p.min_years_experience || 0,
        maxYearsRequired: p.max_years_experience,
        expLabel: formatExpLevel(p.experience_level, p.min_years_experience, p.max_years_experience),
        requiredSkills: p.required_skills || [],
        preferredSkills: p.preferred_skills || [],
        // Monitoring status - determines if we can notify on open
        isMonitored: p.is_monitored || false,
        dataSource: p.data_source || 'unknown',
        dataAsOfDate: p.data_as_of_date
      })));
    } catch (err) {
      console.error('Failed to load jobs:', err);
    } finally {
      setLoading(false);
    }
  };

  const loadWatchlist = async () => {
    try {
      const data = await api.get('/watches');
      setWatched(data.watches.map(w => w.position_id));
    } catch (err) {
      console.error('Failed to load watchlist:', err);
    }
  };

  const loadNotifications = async () => {
    try {
      const data = await api.get('/notifications');
      setNotifications(data.notifications.map(n => ({
        id: n.id,
        type: n.type,
        title: n.title,
        message: n.message,
        time: formatTime(n.created_at),
        read: n.read,
        jobId: n.position_id
      })));
      setUnreadCount(data.unread_count);
    } catch (err) {
      console.error('Failed to load notifications:', err);
    }
  };

  const loadCandidates = async () => {
    try {
      const params = new URLSearchParams();
      if (search) params.append('search', search);

      const data = await api.get(`/companies/candidates?${params}`);
      setCandidates(data.candidates.map(c => ({
        id: c.id,
        name: `${c.first_name || ''} ${c.last_name || ''}`.trim() || 'Anonymous',
        title: c.current_title || 'Professional',
        company: c.current_company || 'Company',
        exp: c.years_experience || '3-5',
        skills: c.skills || [],
        education: 'Not specified',
        matchScore: Math.floor(Math.random() * 30) + 70,
        status: c.search_status || 'open-to-offers',
        loc: c.preferred_locations?.[0] || 'Remote'
      })));
    } catch (err) {
      console.error('Failed to load candidates:', err);
    }
  };

  // Load company positions (for employer view)
  const loadCompanyPositions = async () => {
    try {
      setLoading(true);
      const data = await api.get('/companies/positions?status=all');
      setCompanyPositions(data.positions.map(p => ({
        id: p.id,
        title: p.title,
        department: p.department || '',
        location: p.location || 'Remote',
        salary: p.salary_range || '',
        status: p.status,
        watchers: p.watcher_count || 0,
        views: p.view_count || 0,
        applications: p.application_count || 0,
        postedAt: p.posted_at,
        workArrangement: p.work_arrangement || 'hybrid',
        description: p.description || '',
        expLevel: p.experience_level || 'any',
        minYears: p.min_years_experience || 0,
        maxYears: p.max_years_experience,
        requiredSkills: p.required_skills || [],
        preferredSkills: p.preferred_skills || [],
        fromAts: p.from_ats,
        applyUrl: p.apply_url
      })));
    } catch (err) {
      console.error('Failed to load company positions:', err);
    } finally {
      setLoading(false);
    }
  };

  // Update a position (for employer)
  const updatePosition = async (positionId, updates) => {
    try {
      await api.patch(`/companies/positions/${positionId}`, updates);
      // Reload positions after update
      loadCompanyPositions();
      setEditingPosition(null);
      return true;
    } catch (err) {
      console.error('Failed to update position:', err);
      setError(err.message);
      return false;
    }
  };

  // Load shortlist roles for employer dashboard
  const loadShortlistRoles = async () => {
    try {
      setLoading(true);
      const data = await api.get('/employer/roles');
      setShortlistRoles(data.roles.map(r => ({
        id: r.id,
        title: r.title,
        companyName: r.company_name,
        location: r.location,
        status: r.status,
        isMonitored: r.is_monitored,
        totalApplications: r.total_applications,
        passedScreening: r.passed_screening,
        meetsThreshold: r.meets_threshold,
        scoreThreshold: r.score_threshold || 70
      })));
    } catch (err) {
      console.error('Failed to load shortlist roles:', err);
    } finally {
      setLoading(false);
    }
  };

  // Load shortlist candidates for a specific role
  const loadShortlistCandidates = async (positionId, threshold = null) => {
    try {
      setLoading(true);
      let url = `/employer/roles/${positionId}/shortlist?qualified_only=true`;
      if (threshold) url += `&threshold=${threshold}`;
      const data = await api.get(url);
      setShortlistCandidates(data.candidates.map(c => ({
        id: c.id,
        userId: c.user_id,
        name: `${c.first_name || ''} ${c.last_name || ''}`.trim() || 'Anonymous',
        email: c.email,
        workAuthorization: c.work_authorization,
        gradYear: c.grad_year,
        experienceLevel: c.experience_level,
        startAvailability: c.start_availability,
        projectResponse: c.project_response,
        fitResponse: c.fit_response,
        resumeUrl: c.resume_url,
        linkedinUrl: c.linkedin_url,
        screeningPassed: c.screening_passed,
        screeningFailReason: c.screening_fail_reason,
        aiScore: c.ai_score,
        aiStrengths: c.ai_strengths || [],
        aiConcern: c.ai_concern,
        status: c.status,
        appliedAt: c.applied_at
      })));
      setSelectedShortlistRole(positionId);
    } catch (err) {
      console.error('Failed to load shortlist candidates:', err);
    } finally {
      setLoading(false);
    }
  };

  // Load role configuration
  const loadRoleConfig = async (positionId) => {
    try {
      const data = await api.get(`/roles/${positionId}`);
      setRoleConfig({
        positionId,
        requireWorkAuth: data.role.config?.require_work_auth || false,
        allowedWorkAuth: data.role.config?.allowed_work_auth || [],
        requireExperienceLevel: data.role.config?.require_experience_level || false,
        allowedExperienceLevels: data.role.config?.allowed_experience_levels || [],
        minGradYear: data.role.config?.min_grad_year || null,
        maxGradYear: data.role.config?.max_grad_year || null,
        requiredSkills: data.role.config?.required_skills || [],
        scoreThreshold: data.role.config?.score_threshold || 70,
        volumeCap: data.role.config?.volume_cap || null
      });
      setShowRoleConfig(true);
    } catch (err) {
      console.error('Failed to load role config:', err);
    }
  };

  // Save role configuration
  const saveRoleConfig = async () => {
    if (!roleConfig) return;
    try {
      await api.put(`/employer/roles/${roleConfig.positionId}/config`, {
        require_work_auth: roleConfig.requireWorkAuth,
        allowed_work_auth: roleConfig.allowedWorkAuth,
        require_experience_level: roleConfig.requireExperienceLevel,
        allowed_experience_levels: roleConfig.allowedExperienceLevels,
        min_grad_year: roleConfig.minGradYear,
        max_grad_year: roleConfig.maxGradYear,
        required_skills: roleConfig.requiredSkills,
        score_threshold: roleConfig.scoreThreshold,
        volume_cap: roleConfig.volumeCap
      });
      setShowRoleConfig(false);
      loadShortlistRoles();
      return true;
    } catch (err) {
      console.error('Failed to save role config:', err);
      setError(err.message);
      return false;
    }
  };

  // Export shortlist to CSV
  const exportShortlist = async (positionId) => {
    try {
      window.open(`${API_BASE}/employer/roles/${positionId}/export?token=${token}`, '_blank');
    } catch (err) {
      console.error('Failed to export shortlist:', err);
    }
  };

  // Mark candidate as reviewed
  const markCandidateReviewed = async (applicationId) => {
    try {
      await api.post(`/employer/applications/${applicationId}/review`);
      if (selectedShortlistRole) {
        loadShortlistCandidates(selectedShortlistRole);
      }
    } catch (err) {
      console.error('Failed to mark reviewed:', err);
    }
  };

  // Auth functions
  const handleSignup = async (email, password, type) => {
    try {
      setError(null);
      const data = await api.post('/auth/signup', {
        email,
        password,
        user_type: type,
        first_name: profile.firstName,
        last_name: profile.lastName
      });
      localStorage.setItem('shortlist_token', data.token);
      setToken(data.token);
      setUser(data.user);
      setUserType(type);
      return true;
    } catch (err) {
      setError(err.message);
      return false;
    }
  };

  const handleLogin = async (email, password) => {
    try {
      setError(null);
      const data = await api.post('/auth/login', { email, password });
      localStorage.setItem('shortlist_token', data.token);
      setToken(data.token);
      setUser(data.user);
      setUserType(data.user.user_type);
      setView('dashboard');
      return true;
    } catch (err) {
      setError(err.message);
      return false;
    }
  };

  const handleLogout = () => {
    localStorage.removeItem('shortlist_token');
    setToken(null);
    setUser(null);
    setView('landing');
  };

  // Watch functions
  const toggleWatch = async (jobId) => {
    if (!token) {
      setView('onboarding');
      return;
    }

    try {
      if (watched.includes(jobId)) {
        await api.delete(`/watches/position/${jobId}`);
        setWatched(w => w.filter(id => id !== jobId));
      } else {
        await api.post('/watches', { position_id: jobId });
        setWatched(w => [...w, jobId]);
      }
    } catch (err) {
      console.error('Failed to toggle watch:', err);
    }
  };

  // Invite function
  const inviteCandidate = async (candidateId, positionId = null) => {
    try {
      await api.post('/companies/invite', {
        candidate_user_id: candidateId,
        position_id: positionId,
        message: 'We think you would be a great fit for our team!'
      });
      setInvited(i => [...i, candidateId]);
    } catch (err) {
      console.error('Failed to invite:', err);
    }
  };

  // Notification functions
  const markAllRead = async () => {
    try {
      await api.post('/notifications/read-all');
      setNotifications(n => n.map(notif => ({ ...notif, read: true })));
      setUnreadCount(0);
    } catch (err) {
      console.error('Failed to mark all read:', err);
    }
  };

  const markRead = async (id) => {
    try {
      await api.post(`/notifications/${id}/read`);
      setNotifications(n => n.map(notif => notif.id === id ? { ...notif, read: true } : notif));
      setUnreadCount(c => Math.max(0, c - 1));
    } catch (err) {
      console.error('Failed to mark read:', err);
    }
  };

  // Profile update
  const saveProfile = async () => {
    try {
      await api.put('/users/profile', {
        current_title: profile.currentTitle,
        current_company: profile.currentCompany,
        years_experience: profile.yearsExperience,
        search_status: profile.searchStatus,
        skills: profile.skills,
        work_arrangement: profile.workStyle,
        salary_min: parseInt(profile.salaryMin?.replace(/\D/g, '')) || null,
        salary_max: parseInt(profile.salaryMax?.replace(/\D/g, '')) || null
      });

      await api.put('/users/profile/name', {
        first_name: profile.firstName,
        last_name: profile.lastName
      });
    } catch (err) {
      console.error('Failed to save profile:', err);
    }
  };

  // Helper functions
  const formatTime = (isoString) => {
    if (!isoString) return '';
    const date = new Date(isoString);
    const now = new Date();
    const diff = now - date;
    const hours = Math.floor(diff / (1000 * 60 * 60));
    if (hours < 1) return 'Just now';
    if (hours < 24) return `${hours} hours ago`;
    const days = Math.floor(hours / 24);
    return `${days} day${days > 1 ? 's' : ''} ago`;
  };

  const update = (f, v) => setProfile(p => ({ ...p, [f]: v }));
  const updateCompany = (f, v) => setCompanyProfile(p => ({ ...p, [f]: v }));
  const toggleSkill = (sk) => setProfile(p => ({ ...p, skills: p.skills.includes(sk) ? p.skills.filter(s => s !== sk) : [...p.skills, sk] }));

  // Resume upload and parsing
  const handleResumeUpload = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type
    const allowedTypes = ['application/pdf', 'text/plain', 'application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'];
    if (!allowedTypes.includes(file.type) && !file.name.match(/\.(pdf|txt|doc|docx)$/i)) {
      setError('Please upload a PDF, DOC, DOCX, or TXT file');
      return;
    }

    // Validate file size (max 5MB)
    if (file.size > 5 * 1024 * 1024) {
      setError('File size must be less than 5MB');
      return;
    }

    setUploadingResume(true);
    setResumeUploaded(false);
    setError(null);

    const isPdfOrDoc = file.name.match(/\.(pdf|doc|docx)$/i);

    try {
      let parsed;

      if (isPdfOrDoc) {
        // Send PDF/DOC to server for parsing
        const formData = new FormData();
        formData.append('file', file);

        const response = await fetch(`${API_BASE}/resume/parse`, {
          method: 'POST',
          body: formData,
        });

        const data = await response.json();

        if (!response.ok) {
          throw new Error(data.error || 'Failed to parse resume');
        }

        parsed = data.data;
      } else {
        // Parse text files locally
        parsed = await parseResume(file);
      }

      // Update profile with parsed data
      setProfile(p => ({
        ...p,
        firstName: parsed.firstName || p.firstName,
        lastName: parsed.lastName || p.lastName,
        email: parsed.email || p.email,
        currentTitle: parsed.currentTitle || p.currentTitle,
        currentCompany: parsed.currentCompany || p.currentCompany,
        skills: parsed.skills && parsed.skills.length > 0 ? [...new Set([...p.skills, ...parsed.skills])] : p.skills,
        experiences: parsed.experiences || p.experiences,
        yearsExperience: parsed.yearsExperience?.category || p.yearsExperience,
        techYearsExperience: parsed.yearsExperience?.tech || p.techYearsExperience
      }));

      // Set success state
      setResumeUploaded(true);
      setResumeFileName(file.name);
      setUploadingResume(false);
    } catch (err) {
      console.error('Resume parse error:', err);
      setError(err.message || 'Failed to parse resume. Please try again or enter your information manually.');
      setUploadingResume(false);
      setResumeUploaded(false);
    }

    // Reset file input
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  // Validation functions
  const validateStep1 = () => {
    const errors = {};
    if (!profile.firstName.trim()) errors.firstName = 'First name is required';
    if (!profile.lastName.trim()) errors.lastName = 'Last name is required';
    if (!profile.email.trim()) errors.email = 'Email is required';
    else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(profile.email)) errors.email = 'Invalid email format';
    if (!profile.password || profile.password.length < 6) errors.password = 'Password must be at least 6 characters';
    if (!profile.currentTitle.trim()) errors.currentTitle = 'Current job title is required';
    if (!profile.yearsExperience) errors.yearsExperience = 'Years of experience is required';
    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const validateStep2 = () => {
    const errors = {};
    if (!profile.searchStatus) errors.searchStatus = 'Please select your job search status';
    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const validateStep4 = () => {
    const errors = {};
    if (profile.workStyle.length === 0) errors.workStyle = 'Please select at least one work style preference';
    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const validateCompanyStep1 = () => {
    const errors = {};
    if (!companyProfile.name.trim()) errors.name = 'Company name is required';
    if (!companyProfile.contactEmail.trim()) errors.contactEmail = 'Email is required';
    else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(companyProfile.contactEmail)) errors.contactEmail = 'Invalid email format';
    if (!companyProfile.password || companyProfile.password.length < 6) errors.password = 'Password must be at least 6 characters';
    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const getNotifIcon = (type) => {
    switch(type) {
      case 'vacancy': return '🔔';
      case 'invite': return '✉️';
      case 'match': return '⭐';
      case 'application': return '📄';
      case 'profile': return '👁';
      case 'watcher': return '👀';
      case 'response': return '✅';
      case 'role_opened': return '🎉';  // New: role is now accepting applications
      case 'shortlist_ready': return '📋';  // New: employer shortlist notification
      default: return '📣';
    }
  };

  const userNotifs = notifications.filter(n => userType === 'seeker' ? !n.forCompany : n.forCompany);

  // Parse salary string to number (e.g., "$150k" -> 150000, "$150,000" -> 150000)
  const parseSalary = (salaryStr) => {
    if (!salaryStr) return null;
    const cleaned = salaryStr.replace(/[$,\s]/g, '').toLowerCase();
    const match = cleaned.match(/(\d+)(k)?/);
    if (match) {
      const num = parseInt(match[1]);
      return match[2] ? num * 1000 : num;
    }
    return null;
  };

  // Extract salary range from job salary string (e.g., "$120k - $150k")
  const extractSalaryRange = (salaryStr) => {
    if (!salaryStr) return { min: null, max: null };
    const numbers = salaryStr.match(/\$?\d+[k,\d]*/gi) || [];
    const parsed = numbers.map(parseSalary).filter(n => n !== null);
    return {
      min: parsed.length > 0 ? Math.min(...parsed) : null,
      max: parsed.length > 1 ? Math.max(...parsed) : (parsed[0] || null)
    };
  };

  // Map work style preference to location/arrangement keywords
  const workStyleMatches = (job, workStyles) => {
    if (!workStyles || workStyles.length === 0) return true;
    const location = (job.location || '').toLowerCase();
    const arrangement = (job.workArrangement || '').toLowerCase();

    for (const style of workStyles) {
      if (style.includes('Remote') && (location.includes('remote') || arrangement.includes('remote'))) return true;
      if (style.includes('Hybrid') && (location.includes('hybrid') || arrangement.includes('hybrid'))) return true;
      if (style.includes('On-site') && (location.includes('on-site') || location.includes('onsite') || arrangement.includes('on-site') || (!location.includes('remote') && !location.includes('hybrid')))) return true;
    }
    return false;
  };

  // Check if user has any active filters
  const hasActiveFilters = profile.workStyle.length > 0 || profile.salaryMin || profile.salaryMax || profile.idealRole || profile.expLevelFilter.length > 0;

  // Check if job experience level matches user's filter
  const expLevelMatches = (job, expLevelFilter) => {
    if (!expLevelFilter || expLevelFilter.length === 0) return true;
    if (!job.expLevel || job.expLevel === 'any') return true; // Jobs with no requirement match all
    return expLevelFilter.includes(job.expLevel);
  };

  // Check if job salary overlaps with user's target salary
  const salaryMatches = (job, userMin, userMax) => {
    if (!userMin && !userMax) return true;
    const jobSalary = extractSalaryRange(job.salary);
    if (!jobSalary.min && !jobSalary.max) return true; // If no salary info, include job

    const userMinNum = parseSalary(userMin);
    const userMaxNum = parseSalary(userMax);

    // Check for overlap
    if (userMinNum && jobSalary.max && jobSalary.max < userMinNum) return false;
    if (userMaxNum && jobSalary.min && jobSalary.min > userMaxNum) return false;
    return true;
  };

  // Simple keyword matching for ideal role
  const idealRoleMatches = (job, idealRole) => {
    if (!idealRole || idealRole.trim().length < 3) return true;
    const keywords = idealRole.toLowerCase().split(/\s+/).filter(w => w.length > 2);
    const jobText = `${job.title} ${job.company} ${job.dept} ${job.location}`.toLowerCase();
    // Match if at least one keyword is found
    return keywords.some(kw => jobText.includes(kw));
  };

  // Filter jobs by search and user preferences
  const filteredJobs = jobs.filter(j => {
    // Basic text search
    const searchMatch = !search ||
      j.title?.toLowerCase().includes(search.toLowerCase()) ||
      j.company?.toLowerCase().includes(search.toLowerCase()) ||
      j.location?.toLowerCase().includes(search.toLowerCase());
    if (!searchMatch) return false;

    // Apply user preference filters only if user has set preferences
    if (profile.workStyle.length > 0 && !workStyleMatches(j, profile.workStyle)) return false;
    if ((profile.salaryMin || profile.salaryMax) && !salaryMatches(j, profile.salaryMin, profile.salaryMax)) return false;
    if (profile.idealRole && !idealRoleMatches(j, profile.idealRole)) return false;
    if (profile.expLevelFilter.length > 0 && !expLevelMatches(j, profile.expLevelFilter)) return false;

    return true;
  });

  // LANDING PAGE
  if (view === 'landing') return (
    <div style={s.landing}>
      <style>{css}</style>
      <div style={s.landingBg}/>
      <header style={s.lHeader}>
        <div style={s.logo}><span style={s.logoIcon}>◈</span><span style={s.logoText}>ShortList</span></div>
        <nav style={s.lNav}>
          <a href="#" style={s.lLink}>How It Works</a>
          <a href="#" style={s.lLink}>For Companies</a>
          <button style={s.signIn} onClick={() => setView('login')}>Sign In</button>
        </nav>
      </header>
      <main style={s.lMain}>
        <div style={s.badge}><span style={s.dot}/> The intelligent career matchmaker</div>
        <h1 style={s.hero}>Get on the <span style={s.hl}>ShortList</span> for your dream role</h1>
        <p style={s.sub}>Watch positions you'd want. Get matched with opportunities you'd love. Let companies discover you — confidentially and on your terms.</p>
        <div style={s.ctas}>
          <button style={s.primary} onClick={() => { setUserType('seeker'); setView('onboarding'); }}>Get Started Free →</button>
          <button style={s.secondary} onClick={() => { setUserType('company'); setView('companyOnboarding'); }}>I'm Hiring</button>
        </div>
        <div style={s.proof}>
          <div style={s.avatars}>{['AC','MJ','SW','DP'].map((x,i) => <div key={i} style={{...s.sAvatar, marginLeft: i?'-10px':0, zIndex:4-i}}>{x}</div>)}<div style={{...s.sAvatar, marginLeft:'-10px', background:'#475569'}}>+2k</div></div>
          <span>Join 12,847 professionals on the ShortList</span>
        </div>
        <div style={s.feats}>
          <div style={s.feat}><div style={s.fIcon}>👀</div><h3>Watch & Wait</h3><p>Mark filled positions. Get alerts when they open.</p></div>
          <div style={s.feat}><div style={s.fIcon}>🎯</div><h3>Smart Matching</h3><p>AI surfaces opportunities that truly fit.</p></div>
          <div style={s.feat}><div style={s.fIcon}>🤝</div><h3>Get Discovered</h3><p>Companies find you. Your employer never knows.</p></div>
        </div>
      </main>
      <footer style={s.lFooter}>© 2026 ShortList.ai • Powered by {jobs.length > 0 ? `${jobs.length}+` : '8,000+'} real job postings</footer>
    </div>
  );

  // LOGIN PAGE
  if (view === 'login') {
    return (
      <div style={s.onboard}>
        <style>{css}</style>
        <header style={s.oHeader}>
          <div style={{...s.logo, cursor: 'pointer'}} onClick={() => setView('landing')}><span style={s.logoIcon}>◈</span><span style={s.logoText}>ShortList</span></div>
        </header>
        <main style={s.oMain}>
          <div style={s.step}>
            <h1 style={s.sTitle}>Welcome back</h1>
            <p style={s.sSub}>Sign in to your account</p>
            {error && <div style={{color: '#ef4444', marginBottom: '16px', padding: '12px', background: '#fef2f2', borderRadius: '8px'}}>{error}</div>}
            <div style={{display: 'flex', flexDirection: 'column', gap: '16px'}}>
              <input style={s.inp} type="email" placeholder="Email" value={loginEmail} onChange={e => setLoginEmail(e.target.value)} />
              <input style={s.inp} type="password" placeholder="Password" value={loginPassword} onChange={e => setLoginPassword(e.target.value)} />
              <button style={s.next} onClick={() => handleLogin(loginEmail, loginPassword)}>Sign In →</button>
            </div>
            <p style={{marginTop: '24px', textAlign: 'center', color: '#64748b'}}>
              Don't have an account? <button onClick={() => setView('onboarding')} style={{color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer'}}>Sign up</button>
            </p>
          </div>
        </main>
      </div>
    );
  }

  // ONBOARDING
  if (view === 'onboarding') return (
    <div style={s.onboard}>
      <style>{css}</style>
      <header style={s.oHeader}>
        <div style={s.logo}><span style={s.logoIcon}>◈</span><span style={s.logoText}>ShortList</span></div>
        <div style={s.prog}><span>Step {step}/4</span><div style={s.progBar}><div style={{...s.progFill, width:`${step*25}%`}}/></div></div>
        <button style={s.skip} onClick={() => setView('dashboard')}>Skip</button>
      </header>
      <main style={s.oMain}>
        {step === 1 && <div style={s.step}>
          <h1 style={s.sTitle}>Let's build your profile</h1>
          <p style={s.sSub}>Upload your resume to auto-fill or enter manually</p>
          {error && <div style={{color: '#ef4444', marginBottom: '16px', padding: '12px', background: '#fef2f2', borderRadius: '8px'}}>{error}</div>}
          <div style={s.resumeUploadArea}>
            <input type="file" ref={fileInputRef} style={{display: 'none'}} accept=".pdf,.doc,.docx,.txt" onChange={handleResumeUpload} />
            {resumeUploaded ? (
              <div style={s.resumeSuccess}>
                <div style={s.resumeSuccessIcon}>✓</div>
                <div style={s.resumeSuccessText}>
                  <span style={s.resumeSuccessTitle}>Resume uploaded!</span>
                  <span style={s.resumeFileName}>{resumeFileName}</span>
                </div>
                <button style={s.resumeChangeBtn} onClick={() => fileInputRef.current?.click()}>Change</button>
              </div>
            ) : (
              <button style={s.resumeUploadBtn} onClick={() => fileInputRef.current?.click()} disabled={uploadingResume}>
                {uploadingResume ? (
                  <>
                    <span style={s.uploadSpinner}>⏳</span>
                    <span>Parsing resume...</span>
                  </>
                ) : (
                  <>
                    <span style={s.uploadIcon}>📄</span>
                    <span style={s.uploadText}>Upload Resume</span>
                    <span style={s.uploadHint}>PDF, DOC, DOCX, or TXT (max 5MB)</span>
                  </>
                )}
              </button>
            )}
          </div>
          <div style={s.divider}><span>{resumeUploaded ? 'Review & edit your info' : 'or enter manually'}</span></div>
          {/* Work Experience Summary from Resume */}
          {profile.experiences && profile.experiences.length > 0 && (
            <div style={s.experienceSection}>
              <div style={s.expHeader}>
                <span style={s.expTitle}>Work Experience Detected</span>
                <span style={s.expYears}>{profile.techYearsExperience || 0} years in tech</span>
              </div>
              <div style={s.expList}>
                {profile.experiences.slice(0, 3).map((exp, i) => (
                  <div key={i} style={s.expItem}>
                    <div style={s.expDot}/>
                    <div style={s.expInfo}>
                      <span style={s.expRole}>{exp.title || 'Role'}</span>
                      <span style={s.expCompany}>{exp.company || 'Company'} • {exp.startYear}-{exp.endYear === new Date().getFullYear() ? 'Present' : exp.endYear}</span>
                    </div>
                    {exp.isTechRole && <span style={s.techBadge}>Tech</span>}
                  </div>
                ))}
                {profile.experiences.length > 3 && (
                  <div style={s.expMore}>+{profile.experiences.length - 3} more positions</div>
                )}
              </div>
            </div>
          )}
          <div style={s.grid}>
            <div style={{display: 'flex', flexDirection: 'column', gap: '4px'}}>
              <input style={{...s.inp, ...(validationErrors.firstName ? {borderColor: '#ef4444'} : {})}} placeholder="First Name *" value={profile.firstName} onChange={e => update('firstName', e.target.value)} />
              {validationErrors.firstName && <span style={s.fieldError}>{validationErrors.firstName}</span>}
            </div>
            <div style={{display: 'flex', flexDirection: 'column', gap: '4px'}}>
              <input style={{...s.inp, ...(validationErrors.lastName ? {borderColor: '#ef4444'} : {})}} placeholder="Last Name *" value={profile.lastName} onChange={e => update('lastName', e.target.value)} />
              {validationErrors.lastName && <span style={s.fieldError}>{validationErrors.lastName}</span>}
            </div>
            <div style={{display: 'flex', flexDirection: 'column', gap: '4px'}}>
              <input style={{...s.inp, ...(validationErrors.email ? {borderColor: '#ef4444'} : {})}} placeholder="Email *" value={profile.email} onChange={e => update('email', e.target.value)} />
              {validationErrors.email && <span style={s.fieldError}>{validationErrors.email}</span>}
            </div>
            <div style={{display: 'flex', flexDirection: 'column', gap: '4px'}}>
              <input style={{...s.inp, ...(validationErrors.password ? {borderColor: '#ef4444'} : {})}} type="password" placeholder="Password (min 6 chars) *" value={profile.password || ''} onChange={e => update('password', e.target.value)} />
              {validationErrors.password && <span style={s.fieldError}>{validationErrors.password}</span>}
            </div>
            <div style={{display: 'flex', flexDirection: 'column', gap: '4px'}}>
              <input style={{...s.inp, ...(validationErrors.currentTitle ? {borderColor: '#ef4444'} : {})}} placeholder="Current Job Title *" value={profile.currentTitle} onChange={e => update('currentTitle', e.target.value)} />
              {validationErrors.currentTitle && <span style={s.fieldError}>{validationErrors.currentTitle}</span>}
            </div>
            <input style={s.inp} placeholder="Current Company (optional)" value={profile.currentCompany} onChange={e => update('currentCompany', e.target.value)} />
            <div style={{display: 'flex', flexDirection: 'column', gap: '4px', gridColumn: '1/-1'}}>
              <select style={{...s.inp, ...(validationErrors.yearsExperience ? {borderColor: '#ef4444'} : {})}} value={profile.yearsExperience} onChange={e => update('yearsExperience', e.target.value)}>
                <option value="">Years of Experience *</option>
                <option value="0-2">0-2 years</option><option value="3-5">3-5 years</option><option value="6-10">6-10 years</option><option value="10+">10+ years</option>
              </select>
              {validationErrors.yearsExperience && <span style={s.fieldError}>{validationErrors.yearsExperience}</span>}
            </div>
          </div>
        </div>}
        {step === 2 && <div style={s.step}>
          <h1 style={s.sTitle}>How's your job search going?</h1>
          <p style={s.sSub}>This helps us prioritize the right opportunities</p>
          {validationErrors.searchStatus && <div style={{color: '#ef4444', marginBottom: '16px', padding: '12px', background: '#fef2f2', borderRadius: '8px'}}>{validationErrors.searchStatus}</div>}
          <div style={s.statuses}>
            {[{v:'actively-looking',i:'🚀',t:'Actively Looking',d:'Ready to make a move'},{v:'open-to-offers',i:'👋',t:'Open to Offers',d:'Happy but would consider the right role'},{v:'just-looking',i:'👀',t:'Just Looking',d:'Curious about the market'}].map(o => (
              <button key={o.v} style={{...s.stBtn, ...(profile.searchStatus===o.v?s.stBtnA:{}), ...(validationErrors.searchStatus && !profile.searchStatus ? {borderColor: '#ef4444'} : {})}} onClick={() => { update('searchStatus',o.v); setValidationErrors({}); }}>
                <span style={s.stIcon}>{o.i}</span><div><strong>{o.t}</strong><p style={s.stDesc}>{o.d}</p></div>{profile.searchStatus===o.v && <span style={s.check}>✓</span>}
              </button>
            ))}
          </div>
        </div>}
        {step === 3 && <div style={s.step}>
          <h1 style={s.sTitle}>What are your skills?</h1>
          <p style={s.sSub}>Select all that apply — these power your match score</p>
          <div style={s.skills}>{skillOptions.map(sk => <button key={sk} style={{...s.skBtn, ...(profile.skills.includes(sk)?s.skBtnA:{})}} onClick={() => toggleSkill(sk)}>{profile.skills.includes(sk) && '✓ '}{sk}</button>)}</div>
        </div>}
        {step === 4 && <div style={s.step}>
          <h1 style={s.sTitle}>Almost done! Preferences</h1>
          <p style={s.sSub}>Tell us what you're looking for</p>
          {validationErrors.workStyle && <div style={{color: '#ef4444', marginBottom: '16px', padding: '12px', background: '#fef2f2', borderRadius: '8px'}}>{validationErrors.workStyle}</div>}
          <div style={s.pref}>
            <label style={s.lbl}>Work Style *</label>
            <div style={s.wsRow}>{['🏠 Remote','🔄 Hybrid','🏢 On-site'].map(w => <button key={w} style={{...s.wsBtn, ...(profile.workStyle.includes(w)?s.wsBtnA:{}), ...(validationErrors.workStyle ? {borderColor: '#ef4444'} : {})}} onClick={() => { setProfile(p => ({...p, workStyle: p.workStyle.includes(w)?p.workStyle.filter(x=>x!==w):[...p.workStyle,w]})); setValidationErrors({}); }}>{w}</button>)}</div>
          </div>
          <div style={s.pref}>
            <label style={s.lbl}>Experience Level (filter jobs by level)</label>
            <div style={s.wsRow}>
              {[{v:'entry',l:'Entry (0-2 yrs)'},{v:'mid',l:'Mid (2-5 yrs)'},{v:'senior',l:'Senior (5-8 yrs)'},{v:'staff',l:'Staff+ (8+ yrs)'}].map(lvl => (
                <button key={lvl.v} style={{...s.wsBtn, ...(profile.expLevelFilter.includes(lvl.v)?s.wsBtnA:{})}} onClick={() => setProfile(p => ({...p, expLevelFilter: p.expLevelFilter.includes(lvl.v)?p.expLevelFilter.filter(x=>x!==lvl.v):[...p.expLevelFilter,lvl.v]}))}>
                  {lvl.l}
                </button>
              ))}
            </div>
          </div>
          <div style={s.pref}><label style={s.lbl}>Target Salary (optional)</label><div style={s.salRow}><input style={s.salInp} placeholder="$150,000" value={profile.salaryMin} onChange={e => update('salaryMin',e.target.value)}/><span>to</span><input style={s.salInp} placeholder="$200,000" value={profile.salaryMax} onChange={e => update('salaryMax',e.target.value)}/></div></div>
          <div style={s.pref}><label style={s.lbl}>🔒 Hide from (e.g., current employer)</label><input style={s.inp} placeholder="Company names, comma separated"/></div>
          <div style={s.pref}><label style={s.lbl}>Describe your ideal role (optional)</label><textarea style={s.ta} placeholder="I'm looking for a role where I can..." value={profile.idealRole} onChange={e => update('idealRole',e.target.value)} rows={3}/></div>
        </div>}
        <div style={s.sNav}>
          {step > 1 && <button style={s.back} onClick={() => { setStep(step-1); setValidationErrors({}); }}>← Back</button>}
          <button style={s.next} onClick={async () => {
            setError(null);
            if (step === 1) {
              if (!validateStep1()) return;
              setValidationErrors({});
              setStep(2);
            } else if (step === 2) {
              if (!validateStep2()) return;
              setValidationErrors({});
              setStep(3);
            } else if (step === 3) {
              // Skills are optional
              setStep(4);
            } else if (step === 4) {
              if (!validateStep4()) return;
              // Create account and go to dashboard
              const success = await handleSignup(profile.email, profile.password, 'seeker');
              if (success) {
                await saveProfile();
                setView('dashboard');
              }
            }
          }}>{step===4?'Complete Setup →':'Continue →'}</button>
        </div>
      </main>
    </div>
  );

  // COMPANY ONBOARDING (keeping similar structure)
  if (view === 'companyOnboarding') return (
    <div style={s.onboard}>
      <style>{css}</style>
      <header style={s.oHeader}>
        <div style={s.logo}><span style={s.logoIcon}>◈</span><span style={s.logoText}>ShortList</span></div>
        <div style={s.prog}><span>Step {companyStep}/4</span><div style={s.progBar}><div style={{...s.progFill, width:`${companyStep*25}%`}}/></div></div>
        <button style={s.skip} onClick={() => { setView('dashboard'); setTab('talent'); }}>Skip</button>
      </header>
      <main style={s.oMain}>
        {companyStep === 1 && <div style={s.step}>
          <h1 style={s.sTitle}>Let's set up your company</h1>
          <p style={s.sSub}>Create your company profile to start discovering talent</p>
          {error && <div style={{color: '#ef4444', marginBottom: '16px', padding: '12px', background: '#fef2f2', borderRadius: '8px'}}>{error}</div>}
          <div style={s.grid}>
            <div style={{display: 'flex', flexDirection: 'column', gap: '4px', gridColumn:'1/-1'}}>
              <input style={{...s.inp, ...(validationErrors.name ? {borderColor: '#ef4444'} : {})}} placeholder="Company Name *" value={companyProfile.name} onChange={e => updateCompany('name', e.target.value)} />
              {validationErrors.name && <span style={s.fieldError}>{validationErrors.name}</span>}
            </div>
            <div style={{display: 'flex', flexDirection: 'column', gap: '4px'}}>
              <input style={{...s.inp, ...(validationErrors.contactEmail ? {borderColor: '#ef4444'} : {})}} placeholder="Your Email *" value={companyProfile.contactEmail} onChange={e => updateCompany('contactEmail', e.target.value)} />
              {validationErrors.contactEmail && <span style={s.fieldError}>{validationErrors.contactEmail}</span>}
            </div>
            <div style={{display: 'flex', flexDirection: 'column', gap: '4px'}}>
              <input style={{...s.inp, ...(validationErrors.password ? {borderColor: '#ef4444'} : {})}} type="password" placeholder="Password (min 6 chars) *" value={companyProfile.password || ''} onChange={e => updateCompany('password', e.target.value)} />
              {validationErrors.password && <span style={s.fieldError}>{validationErrors.password}</span>}
            </div>
            <input style={s.inp} placeholder="Company Website (optional)" value={companyProfile.website} onChange={e => updateCompany('website', e.target.value)} />
            <select style={s.inp} value={companyProfile.industry} onChange={e => updateCompany('industry', e.target.value)}>
              <option value="">Industry (optional)</option>
              <option value="Technology">Technology</option><option value="Finance">Finance</option><option value="Healthcare">Healthcare</option><option value="E-commerce">E-commerce</option><option value="SaaS">SaaS</option><option value="AI/ML">AI/ML</option><option value="Other">Other</option>
            </select>
            <select style={s.inp} value={companyProfile.size} onChange={e => updateCompany('size', e.target.value)}>
              <option value="">Company Size (optional)</option>
              <option value="1-50">1-50 employees</option><option value="51-200">51-200 employees</option><option value="201-1000">201-1000 employees</option><option value="1001-5000">1001-5000 employees</option><option value="5000+">5000+ employees</option>
            </select>
          </div>
        </div>}
        {companyStep === 2 && <div style={s.step}>
          <h1 style={s.sTitle}>Company culture & benefits</h1>
          <p style={s.sSub}>Help candidates understand what makes your company great</p>
          <div style={s.pref}>
            <label style={s.lbl}>Work arrangements offered</label>
            <div style={s.wsRow}>
              {['🏠 Remote','🔄 Hybrid','🏢 On-site'].map(w => <button key={w} style={{...s.wsBtn, ...(companyProfile.locations?.includes(w)?s.wsBtnA:{})}} onClick={() => setCompanyProfile(p => ({...p, locations: p.locations?.includes(w)?p.locations.filter(x=>x!==w):[...(p.locations||[]),w]}))}>{w}</button>)}
            </div>
          </div>
          <div style={s.pref}>
            <label style={s.lbl}>Benefits & perks</label>
            <div style={s.benefitsGrid}>
              {['Health Insurance','401k Match','Unlimited PTO','Remote Work','Stock Options','Parental Leave'].map(b => (
                <button key={b} style={{...s.benefitBtn, ...(companyProfile.benefits?.includes(b)?s.benefitBtnA:{})}} onClick={() => setCompanyProfile(p => ({...p, benefits: p.benefits?.includes(b)?p.benefits.filter(x=>x!==b):[...(p.benefits||[]),b]}))}>
                  {companyProfile.benefits?.includes(b) && '✓ '}{b}
                </button>
              ))}
            </div>
          </div>
        </div>}
        {companyStep === 3 && <div style={s.step}>
          <h1 style={s.sTitle}>Almost there!</h1>
          <p style={s.sSub}>Tell candidates about your company</p>
          <textarea style={{...s.ta, minHeight: '150px'}} placeholder="Describe your company culture, mission, and what makes it a great place to work..." value={companyProfile.description} onChange={e => updateCompany('description', e.target.value)} />
        </div>}
        {companyStep === 4 && <div style={s.step}>
          <div style={s.verifySuccess}>
            <div style={s.successIcon}>✅</div>
            <h1 style={s.sTitle}>Almost there!</h1>
            <p style={s.sSub}>Your company profile is ready. After signup, our team will verify your company ownership within 24-48 hours.</p>
            <div style={{background: '#fef3c7', padding: '16px', borderRadius: '10px', marginTop: '20px', fontSize: '14px', color: '#92400e'}}>
              <strong>Verification required:</strong> To protect companies from impersonation, we'll send a verification email to your company domain or request proof of employment.
            </div>
          </div>
        </div>}
        {/* Error display */}
        {error && <div style={{background: '#fee2e2', color: '#991b1b', padding: '12px 16px', borderRadius: '10px', marginBottom: '16px', fontSize: '14px'}}>
          {error === 'Email already registered' ? 'This email is already registered. Please use a different email or log in.' : error}
        </div>}
        <div style={s.sNav}>
          {companyStep > 1 && <button style={s.back} onClick={() => { setCompanyStep(companyStep-1); setValidationErrors({}); }}>← Back</button>}
          <button style={s.next} onClick={async () => {
            setError(null);
            if (companyStep === 1) {
              if (!validateCompanyStep1()) return;
              setValidationErrors({});
              setCompanyStep(2);
            } else if (companyStep < 4) {
              setCompanyStep(companyStep + 1);
            } else {
              const success = await handleSignup(companyProfile.contactEmail, companyProfile.password, 'company');
              if (success) {
                setView('dashboard');
                setTab('shortlist');
              }
            }
          }}>{companyStep===4?'Go to Dashboard →':'Continue →'}</button>
        </div>
      </main>
    </div>
  );

  // DASHBOARD
  return (
    <div style={s.dash}>
      <style>{css}</style>
      <header style={s.header}>
        <div style={s.logo}><span style={s.logoIcon}>◈</span><span style={s.logoText}>ShortList</span></div>
        <div style={s.hRight}>
          <div style={s.toggle}>
            <button style={{...s.togBtn, ...(userType==='seeker'?s.togA:{})}} onClick={() => {setUserType('seeker');setTab('browse');setSearch('');}}>Job Seeker</button>
            <button style={{...s.togBtn, ...(userType==='company'?s.togA:{})}} onClick={() => {setUserType('company');setTab('shortlist');setSearch('');}}>Company</button>
          </div>
          <div style={s.notifWrap}>
            <div style={s.notif} onClick={() => setShowNotifs(!showNotifs)}>🔔{unreadCount > 0 && <span style={s.notifDot}>{unreadCount}</span>}</div>
            {showNotifs && <div style={s.notifDrop}>
              <div style={s.notifHeader}><span style={s.notifTitle}>Notifications</span><button style={s.markRead} onClick={markAllRead}>Mark all read</button></div>
              <div style={s.notifList}>
                {userNotifs.length === 0 ? <div style={s.notifEmpty}>No notifications yet</div> :
                  userNotifs.map(n => (
                    <div key={n.id} style={{...s.notifItem, background: n.read ? 'transparent' : 'rgba(99,102,241,0.05)'}} onClick={() => markRead(n.id)}>
                      <div style={{...s.notifIcon, background: n.type==='vacancy'?'#dcfce7':n.type==='invite'?'#e9d5ff':n.type==='match'?'#fef3c7':n.type==='application'?'#dbeafe':n.type==='response'?'#dcfce7':n.type==='role_opened'?'#dcfce7':n.type==='shortlist_ready'?'#dbeafe':'#f1f5f9'}}>{getNotifIcon(n.type)}</div>
                      <div style={s.notifContent}>
                        <div style={s.notifMsgTitle}>{n.title}</div>
                        <div style={s.notifMsg}>{n.message}</div>
                        <div style={s.notifTime}>{n.time}</div>
                      </div>
                      {!n.read && <div style={s.unreadDot}/>}
                    </div>
                  ))
                }
              </div>
            </div>}
          </div>
          <div style={s.avatar} onClick={handleLogout} title="Click to logout">{profile.firstName?.[0]||user?.first_name?.[0]||'U'}{profile.lastName?.[0]||user?.last_name?.[0]||''}</div>
        </div>
      </header>
      <main style={s.main}>
        <nav style={s.nav}>
          {userType==='seeker' ? <>
            <button style={{...s.navBtn, ...(tab==='browse'?s.navA:{})}} onClick={() => setTab('browse')}>🔍 Discover</button>
            <button style={{...s.navBtn, ...(tab==='matches'?s.navA:{})}} onClick={() => setTab('matches')}>⭐ Matches</button>
            <button style={{...s.navBtn, ...(tab==='watchlist'?s.navA:{})}} onClick={() => setTab('watchlist')}>📌 Watchlist ({watched.length})</button>
            <button style={{...s.navBtn, ...(tab==='profile'?s.navA:{})}} onClick={() => setTab('profile')}>👤 Profile</button>
          </> : <>
            <button style={{...s.navBtn, ...(tab==='shortlist'?s.navA:{})}} onClick={() => setTab('shortlist')}>📋 Shortlist</button>
            <button style={{...s.navBtn, ...(tab==='talent'?s.navA:{})}} onClick={() => setTab('talent')}>🔍 Discover Talent</button>
            <button style={{...s.navBtn, ...(tab==='invited'?s.navA:{})}} onClick={() => setTab('invited')}>✉️ Invited ({invited.length})</button>
            <button style={{...s.navBtn, ...(tab==='positions'?s.navA:{})}} onClick={() => setTab('positions')}>💼 Positions</button>
          </>}
        </nav>

        {/* SEEKER BROWSE/MATCHES */}
        {userType==='seeker' && (tab==='browse'||tab==='matches') && <div>
          <h1 style={s.secTitle}>{tab==='browse'?'Discover Opportunities':'Your Top Matches'}</h1>
          <p style={s.secSub}>{tab==='browse'?'Watch positions and get notified when they open':'Roles that match your profile'}</p>
          <div style={s.filterRow}>
            <div style={s.searchBar}><span style={s.searchIco}>🔍</span><input style={s.searchInp} placeholder="Search jobs, companies, locations..." value={search} onChange={e => setSearch(e.target.value)}/></div>
            <div style={s.statusFilter}>
              <select style={s.statusSelect} value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
                <option value="all">All Positions</option>
                <option value="open">Open Only</option>
                <option value="filled">Filled</option>
                <option value="closed">Closed</option>
              </select>
            </div>
          </div>
          {hasActiveFilters && <div style={s.activeFilters}>
            <span style={s.filterLabel}>Active filters:</span>
            {profile.workStyle.map(ws => <span key={ws} style={s.filterTag}>{ws} <button style={s.filterX} onClick={() => setProfile(p => ({...p, workStyle: p.workStyle.filter(w => w !== ws)}))}>×</button></span>)}
            {profile.expLevelFilter.map(lvl => <span key={lvl} style={s.filterTag}>{lvl} level <button style={s.filterX} onClick={() => setProfile(p => ({...p, expLevelFilter: p.expLevelFilter.filter(l => l !== lvl)}))}>×</button></span>)}
            {(profile.salaryMin || profile.salaryMax) && <span style={s.filterTag}>{profile.salaryMin || '$0'} - {profile.salaryMax || 'Any'} <button style={s.filterX} onClick={() => setProfile(p => ({...p, salaryMin: '', salaryMax: ''}))}>×</button></span>}
            {profile.idealRole && <span style={s.filterTag}>"{profile.idealRole.substring(0, 20)}..." <button style={s.filterX} onClick={() => setProfile(p => ({...p, idealRole: ''}))}>×</button></span>}
            <button style={s.clearFilters} onClick={() => setProfile(p => ({...p, workStyle: [], salaryMin: '', salaryMax: '', idealRole: '', expLevelFilter: []}))}>Clear all</button>
          </div>}
          <div style={s.info}>{loading ? 'Loading...' : `${filteredJobs.length} positions found${hasActiveFilters ? ' (filtered)' : ''}`}</div>
          <div style={s.jobGrid}>
            {(tab==='matches'?[...filteredJobs].sort((a,b)=>b.matchScore-a.matchScore):filteredJobs).map(j => (
              <div key={j.id} style={{...s.jobCard, borderLeft: j.matchScore>=90?'4px solid #6366f1':'none'}}>
                <div style={s.jHeader}><div style={s.jLogo}>{j.company?.[0] || '?'}</div><div style={s.jMeta}><strong>{j.company}</strong><span style={s.jLoc}>{j.location}</span></div><div style={{...s.match, background:j.matchScore>=90?'#6366f1':j.matchScore>=80?'#8b5cf6':'#a5b4fc'}}>{j.matchScore}%</div></div>
                <h3 style={s.jTitle}>{j.title}</h3>
                <p style={s.jSal}>{j.salary}</p>
                <div style={s.jTags}>
                  <span style={s.dept}>{j.dept}</span>
                  {j.expLabel && <span style={s.expLevel}>{j.expLabel}</span>}
                  <span style={{...s.status,
                    background: j.status==='open'?'#dcfce7':j.status==='filled'?'#fef3c7':j.status==='paused'?'#e9d5ff':'#f1f5f9',
                    color: j.status==='open'?'#166534':j.status==='filled'?'#92400e':j.status==='paused'?'#7c3aed':'#64748b'
                  }}>{j.status==='open'?'● Open':j.status==='filled'?'✓ Filled':j.status==='paused'?'⏸ Paused':'○ Closed'}</span>
                  {!j.isMonitored && j.dataSource === 'historical' && <span style={{padding: '4px 8px', background: '#f1f5f9', borderRadius: '6px', fontSize: '11px', color: '#64748b'}}>Historical</span>}
                </div>
                {j.requiredSkills && j.requiredSkills.length > 0 && (
                  <div style={s.reqSkills}>{j.requiredSkills.slice(0,3).map(sk => <span key={sk} style={s.reqSkill}>{sk}</span>)}{j.requiredSkills.length > 3 && <span style={s.moreSkills}>+{j.requiredSkills.length - 3}</span>}</div>
                )}
                <div style={s.jFoot}><span style={s.watchers}>👁 {j.watchers}</span><button style={{...s.watchBtn, background:watched.includes(j.id)?'#6366f1':'transparent', color:watched.includes(j.id)?'#fff':'#6366f1'}} onClick={() => toggleWatch(j.id)}>{watched.includes(j.id)?'✓ Watching':'+ Watch'}</button></div>
              </div>
            ))}
          </div>
        </div>}

        {/* WATCHLIST */}
        {userType==='seeker' && tab==='watchlist' && <div>
          <h1 style={s.secTitle}>Your Watchlist</h1>
          <p style={s.secSub}>Track positions you're interested in. We'll notify you when monitored roles open up.</p>
          <div style={s.stats}><div style={s.stat}><span style={s.statNum}>{watched.length}</span><span style={s.statLbl}>Watching</span></div><div style={s.stat}><span style={s.statNum}>{jobs.filter(j=>watched.includes(j.id)&&j.status==='open').length}</span><span style={s.statLbl}>Now Open</span></div><div style={s.stat}><span style={s.statNum}>{notifications.filter(n=>n.type==='invite').length}</span><span style={s.statLbl}>Invites</span></div></div>
          {jobs.filter(j=>watched.includes(j.id)).length === 0 ? (
            <div style={s.empty}><div style={s.emptyIco}>📌</div><h3>No positions watched yet</h3><p>Browse jobs and click "Watch" to add them here</p><button style={s.emptyBtn} onClick={() => setTab('browse')}>Discover Jobs</button></div>
          ) : jobs.filter(j=>watched.includes(j.id)).map(j => (
            <div key={j.id} style={{...s.wItem, borderLeft:j.status==='open'?'4px solid #10b981':j.isMonitored?'4px solid #6366f1':'4px solid #e2e8f0'}}>
              <div style={s.jLogo}>{j.company?.[0] || '?'}</div>
              <div style={s.wInfo}>
                <strong>{j.title}</strong>
                <span style={s.wMeta}>{j.company} • {j.location}</span>
                <span style={s.wSal}>{j.salary}</span>
                {j.isMonitored ? (
                  <span style={{fontSize: '11px', color: '#6366f1', marginTop: '4px', display: 'block'}}>🔔 We'll notify you when this opens</span>
                ) : j.dataSource === 'historical' ? (
                  <span style={{fontSize: '11px', color: '#94a3b8', marginTop: '4px', display: 'block'}}>Historical data • No alerts available</span>
                ) : null}
              </div>
              <div style={s.wRight}>{j.status==='open'?<button style={s.applyBtn}>Apply Now →</button>:<span style={s.filledTag}>{j.status === 'filled' ? 'Filled' : 'Closed'}</span>}<button style={s.rmBtn} onClick={() => toggleWatch(j.id)}>✕</button></div>
            </div>
          ))}
        </div>}

        {/* PROFILE */}
        {userType==='seeker' && tab==='profile' && <div>
          <div style={s.profCard}>
            <div style={s.profHeader}><div style={s.profAvatar}>{profile.firstName?.[0]||'J'}{profile.lastName?.[0]||'D'}</div><div style={s.profInfo}><h2>{profile.firstName||'Jordan'} {profile.lastName||'Davis'}</h2><p>{profile.currentTitle||'Senior Product Designer'} at {profile.currentCompany||'Acme Corp'}</p><p style={s.exp}>{profile.yearsExperience||'6-10'} years experience</p></div><button style={s.editBtn} onClick={() => {setStep(1);setView('onboarding');}}>Edit Profile</button></div>
            <div style={s.profSec}><h3>Search Status</h3><span style={{...s.sBadge, background:profile.searchStatus==='actively-looking'?'#dcfce7':profile.searchStatus==='open-to-offers'?'#e9d5ff':'#f1f5f9', color:profile.searchStatus==='actively-looking'?'#166534':profile.searchStatus==='open-to-offers'?'#7c3aed':'#64748b'}}>{profile.searchStatus==='actively-looking'?'🚀 Actively Looking':profile.searchStatus==='open-to-offers'?'👋 Open to Offers':'👀 Just Looking'}</span></div>
            {profile.skills.length>0 && <div style={s.profSec}><h3>Skills</h3><div style={s.profSkills}>{profile.skills.map(sk => <span key={sk} style={s.profSkill}>{sk}</span>)}</div></div>}
            <div style={s.profStats}><div style={s.profStat}><strong>{watched.length}</strong><span>Watching</span></div><div style={s.profStat}><strong>{notifications.filter(n=>n.type==='invite').length}</strong><span>Invites</span></div></div>
          </div>
        </div>}

        {/* COMPANY: TALENT */}
        {userType==='company' && tab==='talent' && <div>
          <h1 style={s.secTitle}>Discover Talent</h1>
          <p style={s.secSub}>Confidentially search and invite candidates</p>
          <div style={s.searchBar}><span style={s.searchIco}>🔍</span><input style={s.searchInp} placeholder="Search by name, title, skills..." value={search} onChange={e => setSearch(e.target.value)}/></div>
          <div style={s.confid}>🔒 Your searches are confidential. Candidates only see you if you invite them.</div>
          <div style={s.info}>{candidates.length} candidates found</div>
          <div style={s.candGrid}>
            {candidates.map(c => (
              <div key={c.id} style={s.candCard} onClick={() => setSelCand(c)}>
                <div style={s.cHeader}><div style={s.cAvatar}>{c.name.split(' ').map(n=>n[0]).join('')}</div><span style={{...s.cMatch, background:c.matchScore>=90?'#6366f1':'#dcfce7', color:c.matchScore>=90?'#fff':'#166534'}}>{c.matchScore}%</span></div>
                <h3 style={s.cName}>{c.name}</h3>
                <p style={s.cTitle}>{c.title}</p>
                <p style={s.cComp}>{c.company} • {c.exp} yrs • {c.loc}</p>
                <div style={s.cSkills}>{c.skills.slice(0,3).map(sk => <span key={sk} style={s.cSkill}>{sk}</span>)}{c.skills.length>3 && <span style={s.cMore}>+{c.skills.length-3}</span>}</div>
                <div style={s.cFoot}>
                  <span style={{...s.sBadge, fontSize:'11px', padding:'4px 8px', background:c.status==='actively-looking'?'#dcfce7':c.status==='open-to-offers'?'#e9d5ff':'#f1f5f9', color:c.status==='actively-looking'?'#166534':c.status==='open-to-offers'?'#7c3aed':'#64748b'}}>{c.status==='actively-looking'?'🚀 Active':c.status==='open-to-offers'?'👋 Open':'👀 Looking'}</span>
                  <button style={{...s.invBtn, background:invited.includes(c.id)?'#e2e8f0':'#6366f1', color:invited.includes(c.id)?'#64748b':'#fff'}} onClick={e => {e.stopPropagation(); inviteCandidate(c.id);}}>{invited.includes(c.id)?'✓ Invited':'Invite'}</button>
                </div>
              </div>
            ))}
          </div>
        </div>}

        {/* COMPANY: INVITED */}
        {userType==='company' && tab==='invited' && <div>
          <h1 style={s.secTitle}>Invited Candidates</h1>
          {invited.length===0 ? <div style={s.empty}><div style={s.emptyIco}>✉️</div><h3>No invitations yet</h3><p>Start discovering talent!</p><button style={s.emptyBtn} onClick={() => setTab('talent')}>Discover Talent</button></div>
          : <div>{candidates.filter(c=>invited.includes(c.id)).map(c => <div key={c.id} style={s.invItem}><div style={s.cAvatar}>{c.name.split(' ').map(n=>n[0]).join('')}</div><div style={s.invInfo}><strong>{c.name}</strong><span>{c.title} at {c.company}</span></div><span style={s.pending}>⏳ Pending</span></div>)}</div>}
        </div>}

        {/* COMPANY: SHORTLIST */}
        {userType==='company' && tab==='shortlist' && <div>
          {!selectedShortlistRole ? (
            // Role list view
            <div>
              <h1 style={s.secTitle}>Shortlist Dashboard</h1>
              <p style={s.secSub}>View and manage candidates who have joined the shortlist for your roles</p>

              {/* Stats summary */}
              {shortlistRoles.length > 0 && <div style={s.posStats}>
                <div style={s.posStat}><span style={s.posStatNum}>{shortlistRoles.length}</span><span style={s.posStatLbl}>Roles</span></div>
                <div style={s.posStat}><span style={s.posStatNum}>{shortlistRoles.reduce((sum, r) => sum + (r.totalApplications || 0), 0)}</span><span style={s.posStatLbl}>Total Candidates</span></div>
                <div style={s.posStat}><span style={s.posStatNum}>{shortlistRoles.reduce((sum, r) => sum + (r.meetsThreshold || 0), 0)}</span><span style={s.posStatLbl}>Qualified</span></div>
              </div>}

              {loading ? <div style={s.info}>Loading roles...</div> :
               shortlistRoles.length === 0 ? (
                <div style={s.empty}>
                  <div style={s.emptyIco}>📋</div>
                  <h3>No shortlist candidates yet</h3>
                  <p>When candidates join the shortlist for your roles, they'll appear here.</p>
                </div>
              ) : (
                <div style={s.posGrid}>
                  {shortlistRoles.map(role => (
                    <div key={role.id} style={{...s.posCard, cursor: 'pointer', borderLeft: role.meetsThreshold > 0 ? '4px solid #10b981' : '4px solid #e2e8f0'}} onClick={() => loadShortlistCandidates(role.id)}>
                      <div style={s.posCardHead}>
                        <div>
                          <h3 style={s.posTitle}>{role.title}</h3>
                          <p style={s.posMeta}>{role.companyName} • {role.location}</p>
                        </div>
                        {role.isMonitored && <span style={{...s.atsTag, background: '#dcfce7', color: '#166534'}}>Monitored</span>}
                      </div>

                      <div style={{display: 'flex', gap: '16px', marginTop: '16px'}}>
                        <div style={{textAlign: 'center', flex: 1}}>
                          <div style={{fontSize: '24px', fontWeight: 700, color: '#0f172a'}}>{role.totalApplications || 0}</div>
                          <div style={{fontSize: '12px', color: '#64748b'}}>Total</div>
                        </div>
                        <div style={{textAlign: 'center', flex: 1}}>
                          <div style={{fontSize: '24px', fontWeight: 700, color: '#10b981'}}>{role.passedScreening || 0}</div>
                          <div style={{fontSize: '12px', color: '#64748b'}}>Passed</div>
                        </div>
                        <div style={{textAlign: 'center', flex: 1}}>
                          <div style={{fontSize: '24px', fontWeight: 700, color: '#6366f1'}}>{role.meetsThreshold || 0}</div>
                          <div style={{fontSize: '12px', color: '#64748b'}}>Qualified</div>
                        </div>
                      </div>

                      <div style={{display: 'flex', gap: '8px', marginTop: '16px'}}>
                        <button style={{...s.editBtn, flex: 1}} onClick={(e) => { e.stopPropagation(); loadShortlistCandidates(role.id); }}>View Candidates</button>
                        <button style={{...s.editBtn, background: '#f1f5f9'}} onClick={(e) => { e.stopPropagation(); loadRoleConfig(role.id); }}>Configure</button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ) : (
            // Candidate list view for selected role
            <div>
              <div style={{display: 'flex', alignItems: 'center', gap: '16px', marginBottom: '24px'}}>
                <button style={{...s.back, padding: '8px 16px'}} onClick={() => { setSelectedShortlistRole(null); setShortlistCandidates([]); }}>← Back</button>
                <div style={{flex: 1}}>
                  <h1 style={s.secTitle}>Shortlist Candidates</h1>
                  <p style={s.secSub}>{shortlistRoles.find(r => r.id === selectedShortlistRole)?.title || 'Role'}</p>
                </div>
                <button style={{...s.editBtn, background: '#6366f1', color: '#fff'}} onClick={() => exportShortlist(selectedShortlistRole)}>Export CSV</button>
              </div>

              {loading ? <div style={s.info}>Loading candidates...</div> :
               shortlistCandidates.length === 0 ? (
                <div style={s.empty}>
                  <div style={s.emptyIco}>👥</div>
                  <h3>No qualified candidates yet</h3>
                  <p>Candidates who pass screening and meet your threshold will appear here.</p>
                </div>
              ) : (
                <div style={{display: 'flex', flexDirection: 'column', gap: '16px'}}>
                  {shortlistCandidates.map((c, idx) => (
                    <div key={c.id} style={{background: '#fff', borderRadius: '14px', padding: '20px', boxShadow: '0 1px 3px rgba(0,0,0,0.05)'}}>
                      <div style={{display: 'flex', alignItems: 'flex-start', gap: '16px'}}>
                        {/* Rank badge */}
                        <div style={{width: '40px', height: '40px', borderRadius: '50%', background: idx < 3 ? '#fef3c7' : '#f1f5f9', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 700, color: idx < 3 ? '#92400e' : '#64748b'}}>
                          #{idx + 1}
                        </div>

                        {/* Main info */}
                        <div style={{flex: 1}}>
                          <div style={{display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '8px'}}>
                            <h3 style={{margin: 0, fontSize: '16px', fontWeight: 600}}>{c.name}</h3>
                            {c.aiScore && <span style={{padding: '4px 10px', borderRadius: '20px', background: c.aiScore >= 80 ? '#dcfce7' : c.aiScore >= 70 ? '#fef3c7' : '#fee2e2', color: c.aiScore >= 80 ? '#166534' : c.aiScore >= 70 ? '#92400e' : '#991b1b', fontSize: '13px', fontWeight: 600}}>
                              {c.aiScore} / 100
                            </span>}
                            <span style={{fontSize: '13px', color: '#64748b'}}>{c.email}</span>
                          </div>

                          <div style={{display: 'flex', gap: '16px', fontSize: '13px', color: '#64748b', marginBottom: '12px'}}>
                            <span>Work Auth: {c.workAuthorization?.replace('_', ' ')}</span>
                            {c.experienceLevel && <span>Level: {c.experienceLevel}</span>}
                            {c.gradYear && <span>Grad: {c.gradYear}</span>}
                            {c.startAvailability && <span>Available: {c.startAvailability}</span>}
                          </div>

                          {/* AI Insights */}
                          {c.aiStrengths && c.aiStrengths.length > 0 && (
                            <div style={{background: '#f8fafc', borderRadius: '8px', padding: '12px', marginBottom: '12px'}}>
                              <div style={{fontSize: '12px', fontWeight: 600, color: '#64748b', marginBottom: '8px'}}>AI Insights</div>
                              <div style={{display: 'flex', flexWrap: 'wrap', gap: '8px', marginBottom: '8px'}}>
                                {c.aiStrengths.map((s, i) => (
                                  <span key={i} style={{padding: '4px 10px', background: '#dcfce7', borderRadius: '6px', fontSize: '12px', color: '#166534'}}>✓ {s}</span>
                                ))}
                              </div>
                              {c.aiConcern && <div style={{fontSize: '12px', color: '#92400e'}}>⚠ {c.aiConcern}</div>}
                            </div>
                          )}

                          {/* Responses preview */}
                          <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px'}}>
                            <div>
                              <div style={{fontSize: '12px', fontWeight: 600, color: '#64748b', marginBottom: '4px'}}>Project Response</div>
                              <div style={{fontSize: '13px', color: '#334155', lineHeight: 1.5}}>{c.projectResponse?.substring(0, 150)}{c.projectResponse?.length > 150 ? '...' : ''}</div>
                            </div>
                            <div>
                              <div style={{fontSize: '12px', fontWeight: 600, color: '#64748b', marginBottom: '4px'}}>Why They Fit</div>
                              <div style={{fontSize: '13px', color: '#334155', lineHeight: 1.5}}>{c.fitResponse?.substring(0, 150)}{c.fitResponse?.length > 150 ? '...' : ''}</div>
                            </div>
                          </div>
                        </div>

                        {/* Actions */}
                        <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
                          {c.resumeUrl && <a href={c.resumeUrl} target="_blank" rel="noopener noreferrer" style={{...s.editBtn, textDecoration: 'none', textAlign: 'center'}}>Resume</a>}
                          {c.linkedinUrl && <a href={c.linkedinUrl} target="_blank" rel="noopener noreferrer" style={{...s.editBtn, textDecoration: 'none', textAlign: 'center'}}>LinkedIn</a>}
                          {c.status !== 'reviewed' && <button style={{...s.editBtn, background: '#6366f1', color: '#fff'}} onClick={() => markCandidateReviewed(c.id)}>Mark Reviewed</button>}
                          {c.status === 'reviewed' && <span style={{padding: '8px 16px', background: '#dcfce7', borderRadius: '8px', fontSize: '12px', color: '#166534', textAlign: 'center'}}>✓ Reviewed</span>}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>}

        {/* ROLE CONFIGURATION MODAL */}
        {showRoleConfig && roleConfig && <div style={s.overlay} onClick={() => setShowRoleConfig(false)}>
          <div style={{...s.modal, maxWidth: '600px'}} onClick={e => e.stopPropagation()}>
            <button style={s.modalX} onClick={() => setShowRoleConfig(false)}>✕</button>
            <h2 style={{marginBottom: '8px'}}>Role Configuration</h2>
            <p style={{color: '#64748b', marginBottom: '24px'}}>Define must-have requirements and scoring thresholds</p>

            <div style={s.editForm}>
              {/* Work Authorization */}
              <div style={{...s.formGroup, marginBottom: '20px'}}>
                <div style={{display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '12px'}}>
                  <input type="checkbox" checked={roleConfig.requireWorkAuth} onChange={e => setRoleConfig({...roleConfig, requireWorkAuth: e.target.checked})} />
                  <label style={{...s.formLabel, margin: 0}}>Require specific work authorization</label>
                </div>
                {roleConfig.requireWorkAuth && (
                  <div style={{display: 'flex', flexWrap: 'wrap', gap: '8px', marginLeft: '28px'}}>
                    {['us_citizen', 'permanent_resident', 'f1_opt', 'f1_cpt', 'h1b'].map(auth => (
                      <label key={auth} style={{display: 'flex', alignItems: 'center', gap: '6px', padding: '6px 12px', background: roleConfig.allowedWorkAuth?.includes(auth) ? '#dbeafe' : '#f1f5f9', borderRadius: '6px', cursor: 'pointer', fontSize: '13px'}}>
                        <input type="checkbox" checked={roleConfig.allowedWorkAuth?.includes(auth)} onChange={e => {
                          const newAuth = e.target.checked
                            ? [...(roleConfig.allowedWorkAuth || []), auth]
                            : (roleConfig.allowedWorkAuth || []).filter(a => a !== auth);
                          setRoleConfig({...roleConfig, allowedWorkAuth: newAuth});
                        }} />
                        {auth.replace('_', ' ')}
                      </label>
                    ))}
                  </div>
                )}
              </div>

              {/* Experience Level */}
              <div style={{...s.formGroup, marginBottom: '20px'}}>
                <div style={{display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '12px'}}>
                  <input type="checkbox" checked={roleConfig.requireExperienceLevel} onChange={e => setRoleConfig({...roleConfig, requireExperienceLevel: e.target.checked})} />
                  <label style={{...s.formLabel, margin: 0}}>Require specific experience level</label>
                </div>
                {roleConfig.requireExperienceLevel && (
                  <div style={{display: 'flex', flexWrap: 'wrap', gap: '8px', marginLeft: '28px'}}>
                    {['intern', 'new_grad', 'entry', 'mid', 'senior', 'staff'].map(level => (
                      <label key={level} style={{display: 'flex', alignItems: 'center', gap: '6px', padding: '6px 12px', background: roleConfig.allowedExperienceLevels?.includes(level) ? '#dbeafe' : '#f1f5f9', borderRadius: '6px', cursor: 'pointer', fontSize: '13px'}}>
                        <input type="checkbox" checked={roleConfig.allowedExperienceLevels?.includes(level)} onChange={e => {
                          const newLevels = e.target.checked
                            ? [...(roleConfig.allowedExperienceLevels || []), level]
                            : (roleConfig.allowedExperienceLevels || []).filter(l => l !== level);
                          setRoleConfig({...roleConfig, allowedExperienceLevels: newLevels});
                        }} />
                        {level.replace('_', ' ')}
                      </label>
                    ))}
                  </div>
                )}
              </div>

              {/* Graduation Year */}
              <div style={s.formRow}>
                <div style={s.formGroup}>
                  <label style={s.formLabel}>Min Grad Year</label>
                  <input style={s.formInput} type="number" placeholder="e.g., 2024" value={roleConfig.minGradYear || ''} onChange={e => setRoleConfig({...roleConfig, minGradYear: e.target.value ? parseInt(e.target.value) : null})} />
                </div>
                <div style={s.formGroup}>
                  <label style={s.formLabel}>Max Grad Year</label>
                  <input style={s.formInput} type="number" placeholder="e.g., 2026" value={roleConfig.maxGradYear || ''} onChange={e => setRoleConfig({...roleConfig, maxGradYear: e.target.value ? parseInt(e.target.value) : null})} />
                </div>
              </div>

              {/* Score Threshold */}
              <div style={s.formRow}>
                <div style={s.formGroup}>
                  <label style={s.formLabel}>AI Score Threshold</label>
                  <div style={{display: 'flex', alignItems: 'center', gap: '12px'}}>
                    <input style={{...s.formInput, width: '80px'}} type="number" min="0" max="100" value={roleConfig.scoreThreshold} onChange={e => setRoleConfig({...roleConfig, scoreThreshold: parseInt(e.target.value) || 70})} />
                    <span style={{color: '#64748b', fontSize: '13px'}}>Candidates scoring above this are shown by default</span>
                  </div>
                </div>
              </div>

              {/* Volume Cap */}
              <div style={s.formGroup}>
                <label style={s.formLabel}>Volume Cap (optional)</label>
                <div style={{display: 'flex', alignItems: 'center', gap: '12px'}}>
                  <input style={{...s.formInput, width: '80px'}} type="number" min="0" placeholder="e.g., 50" value={roleConfig.volumeCap || ''} onChange={e => setRoleConfig({...roleConfig, volumeCap: e.target.value ? parseInt(e.target.value) : null})} />
                  <span style={{color: '#64748b', fontSize: '13px'}}>Max candidates to show at a time (leave empty for unlimited)</span>
                </div>
              </div>

              <div style={s.formActions}>
                <button style={s.cancelBtn} onClick={() => setShowRoleConfig(false)}>Cancel</button>
                <button style={s.saveBtn} onClick={saveRoleConfig}>Save Configuration</button>
              </div>
            </div>
          </div>
        </div>}

        {/* COMPANY: POSITIONS */}
        {userType==='company' && tab==='positions' && <div>
          <div style={s.posHead}>
            <div>
              <h1 style={s.secTitle}>Your Positions</h1>
              <p style={s.secSub}>Manage job listings synced from your ATS and track candidate interest</p>
            </div>
            <button style={s.newPos}>+ New Position</button>
          </div>

          {/* Position stats */}
          {companyPositions.length > 0 && <div style={s.posStats}>
            <div style={s.posStat}><span style={s.posStatNum}>{companyPositions.filter(p => p.status === 'open').length}</span><span style={s.posStatLbl}>Open</span></div>
            <div style={s.posStat}><span style={s.posStatNum}>{companyPositions.filter(p => p.status === 'filled').length}</span><span style={s.posStatLbl}>Filled</span></div>
            <div style={s.posStat}><span style={s.posStatNum}>{companyPositions.reduce((sum, p) => sum + (p.watchers || 0), 0)}</span><span style={s.posStatLbl}>Watching</span></div>
          </div>}

          {loading ? <div style={s.info}>Loading positions...</div> :
           companyPositions.length === 0 ? <div style={s.empty}><div style={s.emptyIco}>💼</div><h3>No positions yet</h3><p>Positions from your ATS will appear here automatically, or create one manually.</p></div> :
           <div style={s.posGrid}>
            {companyPositions.map(p => (
              <div key={p.id} style={{...s.posCard, borderLeft: p.status==='open'?'4px solid #10b981':p.status==='filled'?'4px solid #f59e0b':'4px solid #94a3b8'}}>
                <div style={s.posCardHead}>
                  <div>
                    <h3 style={s.posTitle}>{p.title}</h3>
                    <p style={s.posMeta}>{p.location} • {p.workArrangement}</p>
                  </div>
                  <span style={{...s.posStatus,
                    background: p.status==='open'?'#dcfce7':p.status==='filled'?'#fef3c7':p.status==='paused'?'#e9d5ff':'#f1f5f9',
                    color: p.status==='open'?'#166534':p.status==='filled'?'#92400e':p.status==='paused'?'#7c3aed':'#64748b'
                  }}>{p.status==='open'?'● Open':p.status==='filled'?'✓ Filled':p.status==='paused'?'⏸ Paused':'○ Closed'}</span>
                </div>

                {p.fromAts && <span style={s.atsTag}>📡 From ATS</span>}

                <div style={s.posMetrics}>
                  <span style={s.posMetric}>👁 {p.watchers} watching</span>
                  {p.salary && <span style={s.posMetric}>💰 {p.salary}</span>}
                </div>

                {/* Experience requirements */}
                {p.expLevel && p.expLevel !== 'any' && (
                  <div style={s.posExp}>
                    <span style={s.expBadge}>{formatExpLevel(p.expLevel, p.minYears, p.maxYears) || p.expLevel}</span>
                  </div>
                )}

                {/* Actions */}
                <div style={s.posActions}>
                  <select
                    style={s.statusDropdown}
                    value={p.status}
                    onChange={e => updatePosition(p.id, { status: e.target.value })}
                  >
                    <option value="open">Open</option>
                    <option value="filled">Filled</option>
                    <option value="paused">Paused</option>
                    <option value="closed">Closed</option>
                  </select>
                  <button style={s.editBtn} onClick={() => setEditingPosition(p)}>Edit</button>
                </div>
              </div>
            ))}
          </div>}
        </div>}

        {/* POSITION EDIT MODAL */}
        {editingPosition && <div style={s.overlay} onClick={() => setEditingPosition(null)}>
          <div style={{...s.modal, maxWidth: '600px'}} onClick={e => e.stopPropagation()}>
            <button style={s.modalX} onClick={() => setEditingPosition(null)}>✕</button>
            <h2 style={{marginBottom: '20px'}}>Edit Position</h2>
            <div style={s.editForm}>
              <div style={s.formGroup}>
                <label style={s.formLabel}>Title</label>
                <input style={s.formInput} value={editingPosition.title} onChange={e => setEditingPosition({...editingPosition, title: e.target.value})} />
              </div>
              <div style={s.formRow}>
                <div style={s.formGroup}>
                  <label style={s.formLabel}>Location</label>
                  <input style={s.formInput} value={editingPosition.location} onChange={e => setEditingPosition({...editingPosition, location: e.target.value})} />
                </div>
                <div style={s.formGroup}>
                  <label style={s.formLabel}>Work Style</label>
                  <select style={s.formSelect} value={editingPosition.workArrangement} onChange={e => setEditingPosition({...editingPosition, workArrangement: e.target.value})}>
                    <option value="remote">Remote</option>
                    <option value="hybrid">Hybrid</option>
                    <option value="on-site">On-site</option>
                  </select>
                </div>
              </div>
              <div style={s.formGroup}>
                <label style={s.formLabel}>Salary Range</label>
                <input style={s.formInput} value={editingPosition.salary} onChange={e => setEditingPosition({...editingPosition, salary: e.target.value})} placeholder="e.g., $120k - $150k" />
              </div>
              <div style={s.formRow}>
                <div style={s.formGroup}>
                  <label style={s.formLabel}>Experience Level</label>
                  <select style={s.formSelect} value={editingPosition.expLevel} onChange={e => setEditingPosition({...editingPosition, expLevel: e.target.value})}>
                    <option value="any">Any Level</option>
                    <option value="entry">Entry Level</option>
                    <option value="mid">Mid Level</option>
                    <option value="senior">Senior</option>
                    <option value="staff">Staff+</option>
                  </select>
                </div>
                <div style={s.formGroup}>
                  <label style={s.formLabel}>Min Years</label>
                  <input style={s.formInput} type="number" min="0" value={editingPosition.minYears || ''} onChange={e => setEditingPosition({...editingPosition, minYears: parseInt(e.target.value) || 0})} />
                </div>
              </div>
              <div style={s.formGroup}>
                <label style={s.formLabel}>Status</label>
                <select style={s.formSelect} value={editingPosition.status} onChange={e => setEditingPosition({...editingPosition, status: e.target.value})}>
                  <option value="open">Open - Accepting applications</option>
                  <option value="filled">Filled - Position taken</option>
                  <option value="paused">Paused - Temporarily not hiring</option>
                  <option value="closed">Closed - No longer available</option>
                </select>
              </div>
              <div style={s.formActions}>
                <button style={s.cancelBtn} onClick={() => setEditingPosition(null)}>Cancel</button>
                <button style={s.saveBtn} onClick={() => updatePosition(editingPosition.id, {
                  title: editingPosition.title,
                  location: editingPosition.location,
                  work_arrangement: editingPosition.workArrangement,
                  salary_range: editingPosition.salary,
                  experience_level: editingPosition.expLevel,
                  min_years_experience: editingPosition.minYears,
                  status: editingPosition.status
                })}>Save Changes</button>
              </div>
            </div>
          </div>
        </div>}
      </main>

      {/* CANDIDATE MODAL */}
      {selCand && <div style={s.overlay} onClick={() => setSelCand(null)}>
        <div style={s.modal} onClick={e => e.stopPropagation()}>
          <button style={s.modalX} onClick={() => setSelCand(null)}>✕</button>
          <div style={s.mHeader}><div style={s.mAvatar}>{selCand.name.split(' ').map(n=>n[0]).join('')}</div><div style={s.mInfo}><h2>{selCand.name}</h2><p>{selCand.title} at {selCand.company}</p><p style={s.mMeta}>{selCand.loc} • {selCand.exp} years</p></div><div style={s.mMatch}><span style={s.mMatchNum}>{selCand.matchScore}%</span><span>Match</span></div></div>
          <div style={s.mSec}><h4>Status</h4><span style={{...s.sBadge, background:selCand.status==='actively-looking'?'#dcfce7':selCand.status==='open-to-offers'?'#e9d5ff':'#f1f5f9'}}>{selCand.status==='actively-looking'?'🚀 Actively Looking':selCand.status==='open-to-offers'?'👋 Open to Offers':'👀 Just Looking'}</span></div>
          <div style={s.mSec}><h4>Education</h4><p>{selCand.education}</p></div>
          <div style={s.mSec}><h4>Skills</h4><div style={s.mSkills}>{selCand.skills.map(sk => <span key={sk} style={s.mSkill}>{sk}</span>)}</div></div>
          <div style={s.mActions}><button style={{...s.mInvBtn, background:invited.includes(selCand.id)?'#e2e8f0':'#6366f1', color:invited.includes(selCand.id)?'#64748b':'#fff'}} onClick={() => {inviteCandidate(selCand.id); setSelCand(null);}}>{invited.includes(selCand.id)?'✓ Already Invited':'Invite to Apply'}</button><button style={s.mSecBtn}>Save for Later</button></div>
        </div>
      </div>}
    </div>
  );
}

// CSS
const css = `@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Fraunces:wght@500;600;700&display=swap');*{box-sizing:border-box;margin:0;padding:0}body{font-family:'DM Sans',sans-serif}`;

// Styles (keeping the same as original)
const s = {
  landing:{minHeight:'100vh',background:'#0f0f1a',color:'#fff',fontFamily:'"DM Sans",sans-serif',position:'relative'},
  landingBg:{position:'absolute',inset:0,background:'radial-gradient(circle at 30% 20%,rgba(99,102,241,0.15),transparent 50%),radial-gradient(circle at 70% 60%,rgba(139,92,246,0.1),transparent 50%)',pointerEvents:'none'},
  lHeader:{position:'relative',zIndex:10,padding:'24px 48px',display:'flex',justifyContent:'space-between',alignItems:'center'},
  logo:{display:'flex',alignItems:'center',gap:'10px',cursor:'pointer'},logoIcon:{fontSize:'28px',color:'#818cf8'},logoText:{fontFamily:'"Fraunces",serif',fontSize:'24px',fontWeight:600},
  lNav:{display:'flex',alignItems:'center',gap:'32px'},lLink:{color:'#94a3b8',textDecoration:'none',fontSize:'14px'},
  signIn:{padding:'10px 20px',background:'rgba(255,255,255,0.1)',border:'1px solid rgba(255,255,255,0.2)',borderRadius:'8px',color:'#fff',cursor:'pointer'},
  lMain:{position:'relative',zIndex:10,maxWidth:'900px',margin:'0 auto',padding:'60px 24px',textAlign:'center'},
  badge:{display:'inline-flex',alignItems:'center',gap:'8px',padding:'8px 16px',background:'rgba(99,102,241,0.2)',borderRadius:'20px',fontSize:'13px',marginBottom:'24px'},
  dot:{width:'6px',height:'6px',borderRadius:'50%',background:'#818cf8'},
  hero:{fontFamily:'"Fraunces",serif',fontSize:'48px',fontWeight:700,lineHeight:1.1,marginBottom:'24px'},
  hl:{background:'linear-gradient(135deg,#818cf8,#a78bfa)',WebkitBackgroundClip:'text',WebkitTextFillColor:'transparent'},
  sub:{fontSize:'18px',color:'#94a3b8',lineHeight:1.6,marginBottom:'40px'},
  ctas:{display:'flex',justifyContent:'center',gap:'16px',marginBottom:'32px'},
  primary:{padding:'16px 32px',background:'#6366f1',border:'none',borderRadius:'12px',color:'#fff',fontSize:'16px',fontWeight:600,cursor:'pointer'},
  secondary:{padding:'16px 32px',background:'transparent',border:'1px solid rgba(255,255,255,0.3)',borderRadius:'12px',color:'#fff',fontSize:'16px',cursor:'pointer'},
  proof:{display:'flex',alignItems:'center',justifyContent:'center',gap:'12px',marginBottom:'48px',fontSize:'14px',color:'#94a3b8'},
  avatars:{display:'flex'},sAvatar:{width:'32px',height:'32px',borderRadius:'50%',background:'#6366f1',border:'2px solid #0f0f1a',display:'flex',alignItems:'center',justifyContent:'center',fontSize:'11px',fontWeight:600},
  feats:{display:'grid',gridTemplateColumns:'repeat(3,1fr)',gap:'24px',marginTop:'48px'},
  feat:{background:'rgba(255,255,255,0.05)',borderRadius:'16px',padding:'28px 24px',textAlign:'left'},
  fIcon:{fontSize:'28px',marginBottom:'12px'},
  lFooter:{position:'absolute',bottom:'24px',left:0,right:0,textAlign:'center',color:'#64748b',fontSize:'13px'},
  onboard:{minHeight:'100vh',background:'#f8fafc',fontFamily:'"DM Sans",sans-serif'},
  oHeader:{padding:'20px 40px',display:'flex',justifyContent:'space-between',alignItems:'center',borderBottom:'1px solid #e2e8f0',background:'#fff'},
  prog:{display:'flex',alignItems:'center',gap:'12px',fontSize:'14px',color:'#64748b'},
  progBar:{width:'120px',height:'6px',background:'#e2e8f0',borderRadius:'3px',overflow:'hidden'},
  progFill:{height:'100%',background:'#6366f1',borderRadius:'3px',transition:'width 0.3s'},
  skip:{padding:'8px 16px',background:'transparent',border:'none',color:'#64748b',cursor:'pointer'},
  oMain:{maxWidth:'640px',margin:'0 auto',padding:'48px 24px'},
  step:{background:'#fff',borderRadius:'16px',padding:'40px',boxShadow:'0 4px 20px rgba(0,0,0,0.05)'},
  sTitle:{fontFamily:'"Fraunces",serif',fontSize:'28px',fontWeight:600,marginBottom:'8px',color:'#0f172a'},
  sSub:{color:'#64748b',marginBottom:'32px'},
  imports:{display:'flex',gap:'12px',marginBottom:'24px'},
  impBtn:{flex:1,padding:'16px',background:'#f8fafc',border:'1px solid #e2e8f0',borderRadius:'12px',fontSize:'14px',cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center',gap:'8px'},
  divider:{display:'flex',alignItems:'center',justifyContent:'center',margin:'24px 0',color:'#94a3b8',fontSize:'13px'},
  grid:{display:'grid',gridTemplateColumns:'1fr 1fr',gap:'16px'},
  inp:{width:'100%',padding:'14px 16px',border:'1px solid #e2e8f0',borderRadius:'10px',fontSize:'14px',outline:'none'},
  statuses:{display:'flex',flexDirection:'column',gap:'12px'},
  stBtn:{display:'flex',alignItems:'center',gap:'16px',padding:'20px',background:'#f8fafc',border:'2px solid #e2e8f0',borderRadius:'14px',textAlign:'left',cursor:'pointer'},
  stBtnA:{borderColor:'#6366f1',background:'#f0f0ff'},
  stIcon:{fontSize:'28px'},stDesc:{fontSize:'13px',color:'#64748b',marginTop:'4px',fontWeight:'normal'},
  check:{marginLeft:'auto',color:'#6366f1',fontSize:'20px',fontWeight:700},
  skills:{display:'flex',flexWrap:'wrap',gap:'10px'},
  skBtn:{padding:'10px 16px',background:'#f1f5f9',border:'none',borderRadius:'8px',fontSize:'13px',cursor:'pointer'},
  skBtnA:{background:'#6366f1',color:'#fff'},
  pref:{marginBottom:'24px'},lbl:{display:'block',fontSize:'14px',fontWeight:600,marginBottom:'10px',color:'#334155'},
  wsRow:{display:'flex',gap:'10px'},wsBtn:{padding:'12px 20px',background:'#f1f5f9',border:'none',borderRadius:'10px',fontSize:'14px',cursor:'pointer'},wsBtnA:{background:'#6366f1',color:'#fff'},
  salRow:{display:'flex',alignItems:'center',gap:'12px'},salInp:{flex:1,padding:'14px 16px',border:'1px solid #e2e8f0',borderRadius:'10px',fontSize:'14px'},
  ta:{width:'100%',padding:'14px 16px',border:'1px solid #e2e8f0',borderRadius:'10px',fontSize:'14px',resize:'vertical',fontFamily:'inherit'},
  sNav:{display:'flex',justifyContent:'space-between',marginTop:'32px'},
  back:{padding:'14px 24px',background:'transparent',border:'1px solid #e2e8f0',borderRadius:'10px',fontSize:'14px',cursor:'pointer'},
  next:{padding:'14px 32px',background:'#6366f1',border:'none',borderRadius:'10px',color:'#fff',fontSize:'14px',fontWeight:600,cursor:'pointer',marginLeft:'auto'},
  dash:{minHeight:'100vh',background:'#f8fafc',fontFamily:'"DM Sans",sans-serif'},
  header:{padding:'16px 32px',background:'#fff',borderBottom:'1px solid #e2e8f0',display:'flex',justifyContent:'space-between',alignItems:'center',position:'sticky',top:0,zIndex:100},
  hRight:{display:'flex',alignItems:'center',gap:'16px'},
  toggle:{display:'flex',background:'#f1f5f9',borderRadius:'10px',padding:'4px'},
  togBtn:{padding:'10px 20px',border:'none',background:'transparent',borderRadius:'8px',fontSize:'14px',cursor:'pointer',color:'#64748b'},
  togA:{background:'#fff',color:'#0f172a',boxShadow:'0 1px 3px rgba(0,0,0,0.1)'},
  notifWrap:{position:'relative'},
  notif:{position:'relative',padding:'10px',fontSize:'20px',cursor:'pointer',background:'#f1f5f9',borderRadius:'10px',display:'flex',alignItems:'center',justifyContent:'center'},
  notifDot:{position:'absolute',top:'4px',right:'4px',minWidth:'18px',height:'18px',background:'#ef4444',borderRadius:'9px',fontSize:'11px',fontWeight:600,color:'#fff',display:'flex',alignItems:'center',justifyContent:'center',padding:'0 5px'},
  notifDrop:{position:'absolute',top:'52px',right:0,width:'380px',background:'#fff',borderRadius:'16px',boxShadow:'0 20px 40px rgba(0,0,0,0.15)',border:'1px solid #e2e8f0',zIndex:1000,overflow:'hidden'},
  notifHeader:{display:'flex',justifyContent:'space-between',alignItems:'center',padding:'16px 20px',borderBottom:'1px solid #e2e8f0'},
  notifTitle:{fontWeight:600,fontSize:'16px',color:'#0f172a'},
  markRead:{background:'none',border:'none',color:'#6366f1',fontSize:'13px',cursor:'pointer',fontWeight:500},
  notifList:{maxHeight:'400px',overflowY:'auto'},
  notifEmpty:{padding:'40px 20px',textAlign:'center',color:'#64748b'},
  notifItem:{display:'flex',gap:'12px',padding:'14px 20px',borderBottom:'1px solid #f1f5f9',cursor:'pointer',alignItems:'flex-start'},
  notifIcon:{width:'40px',height:'40px',borderRadius:'10px',display:'flex',alignItems:'center',justifyContent:'center',fontSize:'18px',flexShrink:0},
  notifContent:{flex:1,minWidth:0},
  notifMsgTitle:{fontWeight:600,fontSize:'13px',color:'#0f172a',marginBottom:'2px'},
  notifMsg:{fontSize:'13px',color:'#475569',lineHeight:1.4},
  notifTime:{fontSize:'12px',color:'#94a3b8',marginTop:'4px'},
  unreadDot:{width:'8px',height:'8px',borderRadius:'50%',background:'#6366f1',flexShrink:0,marginTop:'6px'},
  avatar:{width:'40px',height:'40px',borderRadius:'10px',background:'#6366f1',color:'#fff',display:'flex',alignItems:'center',justifyContent:'center',fontWeight:600,fontSize:'14px',cursor:'pointer'},
  main:{maxWidth:'1200px',margin:'0 auto',padding:'32px'},
  nav:{display:'flex',gap:'8px',background:'#fff',padding:'8px',borderRadius:'12px',marginBottom:'32px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)'},
  navBtn:{padding:'12px 20px',border:'none',background:'transparent',borderRadius:'8px',fontSize:'14px',cursor:'pointer',color:'#64748b'},
  navA:{background:'#6366f1',color:'#fff'},
  secTitle:{fontFamily:'"Fraunces",serif',fontSize:'28px',fontWeight:600,color:'#0f172a',marginBottom:'8px'},
  secSub:{color:'#64748b',marginBottom:'24px'},
  searchBar:{position:'relative',marginBottom:'16px'},searchIco:{position:'absolute',left:'16px',top:'50%',transform:'translateY(-50%)',fontSize:'16px'},
  searchInp:{width:'100%',padding:'14px 16px 14px 44px',border:'1px solid #e2e8f0',borderRadius:'12px',fontSize:'14px',background:'#fff'},
  info:{fontSize:'13px',color:'#64748b',marginBottom:'16px'},
  jobGrid:{display:'grid',gridTemplateColumns:'repeat(auto-fill,minmax(300px,1fr))',gap:'16px'},
  jobCard:{background:'#fff',borderRadius:'14px',padding:'20px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)'},
  jHeader:{display:'flex',alignItems:'center',gap:'12px',marginBottom:'12px'},
  jLogo:{width:'40px',height:'40px',borderRadius:'10px',background:'#f1f5f9',display:'flex',alignItems:'center',justifyContent:'center',fontSize:'16px',fontWeight:600,color:'#64748b'},
  jMeta:{flex:1,display:'flex',flexDirection:'column',gap:'2px'},jLoc:{fontSize:'13px',color:'#64748b'},
  match:{padding:'5px 10px',borderRadius:'20px',fontSize:'12px',fontWeight:600,color:'#fff'},
  jTitle:{fontSize:'16px',fontWeight:600,color:'#0f172a',marginBottom:'4px'},jSal:{color:'#6366f1',fontWeight:500,fontSize:'14px',marginBottom:'12px'},
  jTags:{display:'flex',gap:'8px',marginBottom:'16px'},dept:{padding:'4px 10px',background:'#f1f5f9',borderRadius:'6px',fontSize:'12px',color:'#64748b'},
  status:{padding:'4px 10px',borderRadius:'6px',fontSize:'12px',fontWeight:500},
  jFoot:{display:'flex',justifyContent:'space-between',alignItems:'center',paddingTop:'12px',borderTop:'1px solid #f1f5f9'},
  watchers:{fontSize:'13px',color:'#64748b'},
  watchBtn:{padding:'8px 14px',border:'2px solid #6366f1',borderRadius:'8px',fontSize:'12px',fontWeight:600,cursor:'pointer'},
  stats:{display:'flex',gap:'16px',marginBottom:'24px'},stat:{background:'#fff',padding:'20px 28px',borderRadius:'12px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)'},
  statNum:{display:'block',fontSize:'28px',fontWeight:700,color:'#0f172a'},statLbl:{fontSize:'13px',color:'#64748b'},
  wItem:{display:'flex',alignItems:'center',gap:'16px',background:'#fff',padding:'16px 20px',borderRadius:'12px',marginBottom:'12px'},
  wInfo:{flex:1,display:'flex',flexDirection:'column',gap:'4px'},wMeta:{fontSize:'13px',color:'#64748b'},wSal:{fontSize:'14px',color:'#6366f1',fontWeight:500},
  wRight:{display:'flex',alignItems:'center',gap:'12px'},
  applyBtn:{padding:'10px 20px',background:'#10b981',border:'none',borderRadius:'8px',color:'#fff',fontWeight:600,cursor:'pointer'},
  filledTag:{padding:'6px 12px',background:'#f1f5f9',borderRadius:'6px',fontSize:'13px',color:'#64748b'},
  rmBtn:{padding:'8px 12px',background:'transparent',border:'none',color:'#94a3b8',cursor:'pointer',fontSize:'16px'},
  profCard:{background:'#fff',borderRadius:'16px',padding:'32px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)',maxWidth:'700px'},
  profHeader:{display:'flex',alignItems:'center',gap:'20px',paddingBottom:'24px',borderBottom:'1px solid #f1f5f9',marginBottom:'24px'},
  profAvatar:{width:'72px',height:'72px',borderRadius:'16px',background:'#6366f1',color:'#fff',display:'flex',alignItems:'center',justifyContent:'center',fontSize:'24px',fontWeight:600},
  profInfo:{flex:1},exp:{fontSize:'14px',color:'#64748b',marginTop:'4px'},
  editBtn:{padding:'10px 20px',background:'#f1f5f9',border:'none',borderRadius:'10px',fontSize:'14px',cursor:'pointer'},
  profSec:{marginBottom:'24px'},sBadge:{display:'inline-block',padding:'8px 14px',borderRadius:'8px',fontSize:'13px',fontWeight:500},
  profSkills:{display:'flex',flexWrap:'wrap',gap:'8px',marginTop:'12px'},profSkill:{padding:'6px 12px',background:'#f1f5f9',borderRadius:'6px',fontSize:'13px'},
  profStats:{display:'flex',gap:'32px',paddingTop:'24px',borderTop:'1px solid #f1f5f9'},profStat:{textAlign:'center'},
  confid:{padding:'14px 20px',background:'#fef3c7',borderRadius:'10px',fontSize:'13px',color:'#92400e',marginBottom:'16px'},
  candGrid:{display:'grid',gridTemplateColumns:'repeat(auto-fill,minmax(280px,1fr))',gap:'16px'},
  candCard:{background:'#fff',borderRadius:'14px',padding:'20px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)',cursor:'pointer'},
  cHeader:{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:'12px'},
  cAvatar:{width:'48px',height:'48px',borderRadius:'12px',background:'linear-gradient(135deg,#6366f1,#8b5cf6)',color:'#fff',display:'flex',alignItems:'center',justifyContent:'center',fontWeight:600,fontSize:'14px'},
  cMatch:{padding:'5px 10px',borderRadius:'20px',fontSize:'11px',fontWeight:600},
  cName:{fontSize:'16px',fontWeight:600,marginBottom:'4px'},cTitle:{fontSize:'14px',color:'#334155'},cComp:{fontSize:'13px',color:'#64748b',marginBottom:'12px'},
  cSkills:{display:'flex',gap:'6px',flexWrap:'wrap',marginBottom:'16px'},cSkill:{padding:'4px 8px',background:'#f1f5f9',borderRadius:'4px',fontSize:'11px'},cMore:{padding:'4px 8px',background:'#f1f5f9',borderRadius:'4px',fontSize:'11px',color:'#94a3b8'},
  cFoot:{display:'flex',justifyContent:'space-between',alignItems:'center',paddingTop:'12px',borderTop:'1px solid #f1f5f9'},
  invBtn:{padding:'8px 14px',border:'none',borderRadius:'8px',fontSize:'12px',fontWeight:600,cursor:'pointer'},
  empty:{textAlign:'center',padding:'64px',background:'#fff',borderRadius:'16px'},emptyIco:{fontSize:'48px',marginBottom:'16px'},
  emptyBtn:{marginTop:'16px',padding:'12px 24px',background:'#6366f1',border:'none',borderRadius:'10px',color:'#fff',fontWeight:600,cursor:'pointer'},
  invItem:{display:'flex',alignItems:'center',gap:'16px',background:'#fff',padding:'20px',borderRadius:'12px',marginBottom:'12px'},
  invInfo:{flex:1,display:'flex',flexDirection:'column',gap:'4px'},pending:{padding:'6px 12px',background:'#fef3c7',borderRadius:'20px',fontSize:'12px',color:'#92400e',fontWeight:500},
  posHead:{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:'24px'},
  newPos:{padding:'12px 20px',background:'#6366f1',border:'none',borderRadius:'10px',color:'#fff',fontWeight:600,cursor:'pointer'},
  overlay:{position:'fixed',inset:0,background:'rgba(0,0,0,0.5)',display:'flex',alignItems:'center',justifyContent:'center',zIndex:1000,padding:'24px'},
  modal:{background:'#fff',borderRadius:'20px',padding:'32px',maxWidth:'500px',width:'100%',position:'relative',maxHeight:'90vh',overflow:'auto'},
  modalX:{position:'absolute',top:'16px',right:'16px',width:'36px',height:'36px',borderRadius:'50%',border:'none',background:'#f1f5f9',fontSize:'18px',cursor:'pointer'},
  mHeader:{display:'flex',gap:'16px',marginBottom:'24px'},
  mAvatar:{width:'64px',height:'64px',borderRadius:'16px',background:'linear-gradient(135deg,#6366f1,#8b5cf6)',color:'#fff',display:'flex',alignItems:'center',justifyContent:'center',fontWeight:600,fontSize:'20px'},
  mInfo:{flex:1},mMeta:{fontSize:'14px',color:'#64748b',marginTop:'4px'},
  mMatch:{textAlign:'center'},mMatchNum:{display:'block',fontSize:'28px',fontWeight:700,color:'#10b981'},
  mSec:{marginBottom:'20px'},
  mSkills:{display:'flex',flexWrap:'wrap',gap:'8px',marginTop:'8px'},mSkill:{padding:'6px 12px',background:'#f1f5f9',borderRadius:'6px',fontSize:'13px'},
  mActions:{display:'flex',gap:'12px',paddingTop:'20px',borderTop:'1px solid #f1f5f9'},
  mInvBtn:{flex:1,padding:'14px',border:'none',borderRadius:'10px',fontSize:'14px',fontWeight:600,cursor:'pointer'},
  mSecBtn:{padding:'14px 20px',background:'#f1f5f9',border:'none',borderRadius:'10px',fontSize:'14px',cursor:'pointer'},
  benefitsGrid:{display:'flex',flexWrap:'wrap',gap:'10px'},
  benefitBtn:{padding:'10px 16px',background:'#f1f5f9',border:'none',borderRadius:'8px',fontSize:'13px',cursor:'pointer'},
  benefitBtnA:{background:'#6366f1',color:'#fff'},
  verifySuccess:{textAlign:'center',marginBottom:'32px'},
  successIcon:{fontSize:'64px',marginBottom:'16px'},
  fieldError:{fontSize:'12px',color:'#ef4444',marginTop:'2px'},
  resumeUploadArea:{marginBottom:'24px'},
  resumeUploadBtn:{width:'100%',padding:'32px 24px',background:'#f8fafc',border:'2px dashed #e2e8f0',borderRadius:'12px',cursor:'pointer',display:'flex',flexDirection:'column',alignItems:'center',gap:'8px',transition:'all 0.2s'},
  uploadIcon:{fontSize:'32px'},
  uploadText:{fontSize:'16px',fontWeight:600,color:'#334155'},
  uploadHint:{fontSize:'13px',color:'#94a3b8'},
  uploadSpinner:{fontSize:'24px',animation:'spin 1s linear infinite'},
  resumeSuccess:{display:'flex',alignItems:'center',gap:'16px',padding:'20px 24px',background:'#dcfce7',border:'2px solid #86efac',borderRadius:'12px'},
  resumeSuccessIcon:{width:'40px',height:'40px',borderRadius:'50%',background:'#22c55e',color:'#fff',display:'flex',alignItems:'center',justifyContent:'center',fontSize:'20px',fontWeight:700},
  resumeSuccessText:{flex:1,display:'flex',flexDirection:'column',gap:'2px'},
  resumeSuccessTitle:{fontSize:'15px',fontWeight:600,color:'#166534'},
  resumeFileName:{fontSize:'13px',color:'#15803d'},
  resumeChangeBtn:{padding:'8px 16px',background:'#fff',border:'1px solid #86efac',borderRadius:'8px',fontSize:'13px',color:'#166534',cursor:'pointer'},
  activeFilters:{display:'flex',flexWrap:'wrap',alignItems:'center',gap:'8px',padding:'12px 16px',background:'#f0f0ff',borderRadius:'10px',marginBottom:'16px'},
  filterLabel:{fontSize:'13px',color:'#64748b',fontWeight:500},
  filterTag:{display:'inline-flex',alignItems:'center',gap:'6px',padding:'6px 10px',background:'#fff',borderRadius:'6px',fontSize:'12px',color:'#6366f1',border:'1px solid #e2e8f0'},
  filterX:{background:'none',border:'none',color:'#94a3b8',cursor:'pointer',fontSize:'14px',padding:'0 2px',lineHeight:1},
  clearFilters:{background:'none',border:'none',color:'#ef4444',cursor:'pointer',fontSize:'12px',fontWeight:500,marginLeft:'8px'},
  // Work experience styles
  experienceSection:{marginBottom:'24px',padding:'20px',background:'#f0fdf4',border:'1px solid #bbf7d0',borderRadius:'12px'},
  expHeader:{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:'12px'},
  expTitle:{fontSize:'14px',fontWeight:600,color:'#166534'},
  expYears:{fontSize:'13px',fontWeight:600,color:'#15803d',background:'#dcfce7',padding:'4px 10px',borderRadius:'12px'},
  expList:{display:'flex',flexDirection:'column',gap:'10px'},
  expItem:{display:'flex',alignItems:'center',gap:'12px',padding:'10px 12px',background:'#fff',borderRadius:'8px',border:'1px solid #e2e8f0'},
  expDot:{width:'8px',height:'8px',borderRadius:'50%',background:'#22c55e',flexShrink:0},
  expInfo:{flex:1,display:'flex',flexDirection:'column',gap:'2px'},
  expRole:{fontSize:'14px',fontWeight:500,color:'#0f172a'},
  expCompany:{fontSize:'12px',color:'#64748b'},
  techBadge:{fontSize:'11px',fontWeight:500,color:'#6366f1',background:'#e0e7ff',padding:'3px 8px',borderRadius:'4px'},
  expMore:{fontSize:'12px',color:'#64748b',textAlign:'center',paddingTop:'4px'},
  // Experience level badge on job cards
  expLevel:{fontSize:'11px',fontWeight:500,padding:'4px 8px',borderRadius:'4px',background:'#fef3c7',color:'#92400e'},
  // Required skills on job cards
  reqSkills:{display:'flex',flexWrap:'wrap',gap:'6px',marginTop:'8px'},
  reqSkill:{fontSize:'11px',padding:'3px 8px',background:'#e0e7ff',color:'#4338ca',borderRadius:'4px'},
  moreSkills:{fontSize:'11px',padding:'3px 8px',color:'#64748b'},
  // Filter row for search + status filter
  filterRow:{display:'flex',gap:'12px',marginBottom:'16px',alignItems:'stretch'},
  statusFilter:{flexShrink:0},
  statusSelect:{padding:'14px 16px',border:'1px solid #e2e8f0',borderRadius:'12px',fontSize:'14px',background:'#fff',cursor:'pointer',minWidth:'140px'},
  // Company position management styles
  posStats:{display:'flex',gap:'16px',marginBottom:'24px'},
  posStat:{background:'#fff',padding:'20px 28px',borderRadius:'12px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)',textAlign:'center'},
  posStatNum:{display:'block',fontSize:'28px',fontWeight:700,color:'#0f172a'},
  posStatLbl:{fontSize:'13px',color:'#64748b'},
  posGrid:{display:'grid',gridTemplateColumns:'repeat(auto-fill,minmax(340px,1fr))',gap:'16px'},
  posCard:{background:'#fff',borderRadius:'14px',padding:'20px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)'},
  posCardHead:{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:'12px'},
  posTitle:{fontSize:'16px',fontWeight:600,color:'#0f172a',marginBottom:'4px'},
  posMeta:{fontSize:'13px',color:'#64748b'},
  posStatus:{padding:'4px 10px',borderRadius:'6px',fontSize:'12px',fontWeight:500},
  atsTag:{display:'inline-block',fontSize:'11px',padding:'4px 8px',background:'#dbeafe',color:'#1d4ed8',borderRadius:'4px',marginBottom:'8px'},
  posMetrics:{display:'flex',gap:'16px',marginBottom:'12px'},
  posMetric:{fontSize:'13px',color:'#64748b'},
  posExp:{marginBottom:'12px'},
  expBadge:{fontSize:'12px',fontWeight:500,padding:'4px 10px',borderRadius:'6px',background:'#fef3c7',color:'#92400e'},
  posActions:{display:'flex',gap:'10px',paddingTop:'12px',borderTop:'1px solid #f1f5f9'},
  statusDropdown:{flex:1,padding:'8px 12px',border:'1px solid #e2e8f0',borderRadius:'8px',fontSize:'13px',background:'#fff',cursor:'pointer'},
  // Form styles for position editing
  editForm:{display:'flex',flexDirection:'column',gap:'16px'},
  formGroup:{display:'flex',flexDirection:'column',gap:'6px'},
  formRow:{display:'grid',gridTemplateColumns:'1fr 1fr',gap:'16px'},
  formLabel:{fontSize:'13px',fontWeight:600,color:'#334155'},
  formInput:{padding:'12px 14px',border:'1px solid #e2e8f0',borderRadius:'8px',fontSize:'14px',outline:'none'},
  formSelect:{padding:'12px 14px',border:'1px solid #e2e8f0',borderRadius:'8px',fontSize:'14px',background:'#fff',cursor:'pointer'},
  formActions:{display:'flex',gap:'12px',justifyContent:'flex-end',paddingTop:'16px',borderTop:'1px solid #f1f5f9'},
  cancelBtn:{padding:'12px 20px',background:'#f1f5f9',border:'none',borderRadius:'8px',fontSize:'14px',cursor:'pointer'},
  saveBtn:{padding:'12px 24px',background:'#6366f1',border:'none',borderRadius:'8px',color:'#fff',fontSize:'14px',fontWeight:600,cursor:'pointer'},
};
