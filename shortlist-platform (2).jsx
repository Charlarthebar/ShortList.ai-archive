import React, { useState } from 'react';

const mockJobs = [
  // Original sample jobs (keeping these)
  { id: 1, title: 'Senior Software Engineer', company: 'Nexus Technologies', location: 'San Francisco, CA', salary: '$180k - $220k', status: 'filled', watchers: 234, matchScore: 94, dept: 'Engineering' },
  { id: 2, title: 'Product Design Lead', company: 'Meridian Labs', location: 'New York, NY', salary: '$160k - $190k', status: 'filled', watchers: 187, matchScore: 78, dept: 'Design' },
  { id: 3, title: 'Data Science Manager', company: 'Quantum Analytics', location: 'Austin, TX', salary: '$170k - $210k', status: 'open', watchers: 312, matchScore: 85, dept: 'Data' },
  { id: 4, title: 'Staff Frontend Engineer', company: 'Stripe', location: 'San Francisco, CA', salary: '$220k - $280k', status: 'filled', watchers: 567, matchScore: 96, dept: 'Engineering' },
  { id: 5, title: 'Head of Product', company: 'Figma', location: 'San Francisco, CA', salary: '$250k - $320k', status: 'open', watchers: 423, matchScore: 88, dept: 'Product' },
  // Woodlands, TX jobs from uploaded data
  { id: 6, title: 'Senior Administrative Assistant', company: 'JPMorgan Chase Bank', location: 'The Woodlands, TX', salary: '$55k - $70k', status: 'open', watchers: 89, matchScore: 72, dept: 'Administration' },
  { id: 7, title: 'RN II NICU', company: 'Houston Methodist', location: 'The Woodlands, TX', salary: '$75k - $95k', status: 'open', watchers: 156, matchScore: 81, dept: 'Healthcare' },
  { id: 8, title: 'Payroll Manager', company: 'WHC Energy Services', location: 'The Woodlands, TX', salary: '$85k - $110k', status: 'open', watchers: 67, matchScore: 76, dept: 'Finance' },
  { id: 9, title: 'Procurement Specialist', company: 'Cordialsa USA', location: 'The Woodlands, TX', salary: '$65k - $85k', status: 'filled', watchers: 45, matchScore: 69, dept: 'Operations' },
  { id: 10, title: 'Junior Mobile Engineer', company: 'Clarity Innovations', location: 'The Woodlands, TX', salary: '$70k - $90k', status: 'open', watchers: 234, matchScore: 89, dept: 'Engineering' },
  { id: 11, title: 'Dental Hygienist', company: 'Bear Branch Family Dentistry', location: 'The Woodlands, TX', salary: '$60k - $80k', status: 'open', watchers: 78, matchScore: 65, dept: 'Healthcare' },
  { id: 12, title: 'Nurse Practitioner - CV Surgery', company: 'Houston Methodist', location: 'The Woodlands, TX', salary: '$120k - $150k', status: 'open', watchers: 189, matchScore: 83, dept: 'Healthcare' },
  { id: 13, title: 'Strategy Analyst', company: 'Lantern', location: 'The Woodlands, TX', salary: '$80k - $100k', status: 'open', watchers: 112, matchScore: 87, dept: 'Strategy' },
  { id: 14, title: 'Automotive Technician', company: 'Christian Brothers Automotive', location: 'The Woodlands, TX', salary: '$50k - $75k', status: 'open', watchers: 56, matchScore: 58, dept: 'Automotive' },
  { id: 15, title: 'Remote Benefits Coordinator', company: 'McQuade Organization', location: 'Remote (The Woodlands, TX)', salary: '$45k - $60k', status: 'open', watchers: 198, matchScore: 74, dept: 'HR' },
  { id: 16, title: 'Tax Associate', company: 'Focus Partners Wealth', location: 'The Woodlands, TX', salary: '$60k - $80k', status: 'open', watchers: 92, matchScore: 71, dept: 'Finance' },
  { id: 17, title: 'Lead Audiologist-ENT', company: 'Memorial Hermann Health System', location: 'The Woodlands, TX', salary: '$90k - $120k', status: 'filled', watchers: 134, matchScore: 77, dept: 'Healthcare' },
  { id: 18, title: 'EMT Paramedic', company: 'Memorial Hermann Health System', location: 'The Woodlands, TX', salary: '$45k - $60k', status: 'open', watchers: 87, matchScore: 62, dept: 'Healthcare' },
  { id: 19, title: 'Radiologic Technologist I', company: 'Houston Methodist', location: 'The Woodlands, TX', salary: '$55k - $70k', status: 'open', watchers: 98, matchScore: 68, dept: 'Healthcare' },
  { id: 20, title: 'Remote Account Executive', company: 'Zuzick & Associates', location: 'Remote (The Woodlands, TX)', salary: '$60k - $100k', status: 'open', watchers: 156, matchScore: 79, dept: 'Sales' },
  { id: 21, title: 'Gastroenterologist', company: 'Baylor College of Medicine', location: 'The Woodlands, TX', salary: '$300k - $450k', status: 'filled', watchers: 234, matchScore: 91, dept: 'Medical' },
  { id: 22, title: 'Corporate Communications Intern', company: 'Sage', location: 'The Woodlands, TX', salary: '$18 - $22/hr', status: 'open', watchers: 45, matchScore: 55, dept: 'Marketing' },
  { id: 23, title: 'Digital Commerce Intern', company: 'EverCommerce', location: 'The Woodlands, TX', salary: '$20 - $25/hr', status: 'open', watchers: 67, matchScore: 64, dept: 'E-commerce' },
  { id: 24, title: 'IT Technician', company: 'METRO Construction Ltd.', location: 'The Woodlands, TX', salary: '$50k - $65k', status: 'open', watchers: 78, matchScore: 73, dept: 'IT' },
];

const mockCandidates = [
  { id: 1, name: 'Alexandra Chen', title: 'Senior Software Engineer', company: 'Google', exp: 8, skills: ['React', 'Node.js', 'Python', 'AWS', 'System Design'], education: 'MS Computer Science, Stanford', matchScore: 94, status: 'actively-looking', loc: 'San Francisco, CA' },
  { id: 2, name: 'Marcus Johnson', title: 'Engineering Manager', company: 'Meta', exp: 12, skills: ['Team Leadership', 'System Design', 'Java', 'Kubernetes'], education: 'BS Computer Engineering, MIT', matchScore: 89, status: 'open-to-offers', loc: 'Menlo Park, CA' },
  { id: 3, name: 'Sarah Williams', title: 'Full Stack Developer', company: 'Stripe', exp: 5, skills: ['TypeScript', 'GraphQL', 'PostgreSQL', 'React'], education: 'BS Software Engineering, CMU', matchScore: 86, status: 'just-looking', loc: 'Seattle, WA' },
  { id: 4, name: 'David Park', title: 'Principal Engineer', company: 'Netflix', exp: 15, skills: ['Architecture', 'Microservices', 'Go', 'AWS'], education: 'PhD Computer Science, Berkeley', matchScore: 97, status: 'open-to-offers', loc: 'Los Gatos, CA' },
  { id: 5, name: 'Emily Rodriguez', title: 'Product Manager', company: 'Airbnb', exp: 7, skills: ['Product Strategy', 'User Research', 'SQL', 'A/B Testing'], education: 'MBA, Harvard Business School', matchScore: 91, status: 'actively-looking', loc: 'San Francisco, CA' },
  { id: 6, name: 'James Liu', title: 'Staff ML Engineer', company: 'OpenAI', exp: 9, skills: ['PyTorch', 'TensorFlow', 'NLP', 'Python'], education: 'PhD Machine Learning, Stanford', matchScore: 93, status: 'open-to-offers', loc: 'San Francisco, CA' },
  { id: 7, name: 'Rachel Kim', title: 'Design Director', company: 'Figma', exp: 11, skills: ['Design Systems', 'Figma', 'User Research', 'Leadership'], education: 'MFA Design, RISD', matchScore: 88, status: 'just-looking', loc: 'New York, NY' },
  { id: 8, name: 'Michael Thompson', title: 'VP of Engineering', company: 'Plaid', exp: 16, skills: ['Engineering Leadership', 'Scaling Teams', 'Architecture'], education: 'BS Computer Science, MIT', matchScore: 95, status: 'open-to-offers', loc: 'San Francisco, CA' },
  { id: 9, name: 'Jennifer Martinez', title: 'Senior Data Scientist', company: 'Databricks', exp: 6, skills: ['Python', 'SQL', 'Machine Learning', 'Spark'], education: 'MS Statistics, UC Berkeley', matchScore: 87, status: 'actively-looking', loc: 'San Francisco, CA' },
  { id: 10, name: 'Robert Wilson', title: 'Backend Engineer', company: 'Coinbase', exp: 4, skills: ['Go', 'Rust', 'PostgreSQL', 'Redis'], education: 'BS Computer Science, Georgia Tech', matchScore: 82, status: 'actively-looking', loc: 'Remote' },
  { id: 11, name: 'Amanda Foster', title: 'Head of Product Design', company: 'Notion', exp: 13, skills: ['Product Design', 'Design Strategy', 'Leadership', 'Figma'], education: 'BA Design, Parsons', matchScore: 90, status: 'open-to-offers', loc: 'New York, NY' },
  { id: 12, name: 'Kevin Zhang', title: 'Site Reliability Engineer', company: 'Cloudflare', exp: 7, skills: ['Kubernetes', 'Terraform', 'Go', 'Linux'], education: 'BS CS, University of Washington', matchScore: 85, status: 'actively-looking', loc: 'Austin, TX' },
  { id: 13, name: 'Lisa Anderson', title: 'Growth Lead', company: 'DoorDash', exp: 8, skills: ['Growth Strategy', 'Analytics', 'SQL', 'Marketing'], education: 'MBA, Wharton', matchScore: 79, status: 'open-to-offers', loc: 'San Francisco, CA' },
  { id: 14, name: 'Christopher Lee', title: 'iOS Tech Lead', company: 'Apple', exp: 10, skills: ['Swift', 'iOS Architecture', 'UIKit', 'SwiftUI'], education: 'MS Computer Science, Stanford', matchScore: 92, status: 'just-looking', loc: 'Cupertino, CA' },
  { id: 15, name: 'Natalie Brown', title: 'Security Architect', company: 'CrowdStrike', exp: 12, skills: ['Security Architecture', 'Cloud Security', 'Python'], education: 'MS Cybersecurity, Georgia Tech', matchScore: 84, status: 'actively-looking', loc: 'Austin, TX' },
  { id: 16, name: 'Daniel Garcia', title: 'Frontend Architect', company: 'Vercel', exp: 9, skills: ['React', 'Next.js', 'TypeScript', 'Performance'], education: 'BS Computer Science, UCLA', matchScore: 96, status: 'open-to-offers', loc: 'Remote' },
  { id: 17, name: 'Michelle Wang', title: 'Data Engineering Manager', company: 'Snowflake', exp: 11, skills: ['Data Pipelines', 'Spark', 'Python', 'AWS'], education: 'MS Data Science, Columbia', matchScore: 88, status: 'actively-looking', loc: 'San Mateo, CA' },
  { id: 18, name: 'Brian Taylor', title: 'Technical PM', company: 'Slack', exp: 6, skills: ['Technical PM', 'API Design', 'Agile', 'SQL'], education: 'BS Computer Science, Cornell', matchScore: 83, status: 'open-to-offers', loc: 'San Francisco, CA' },
];

const skillOptions = ['JavaScript', 'TypeScript', 'React', 'Node.js', 'Python', 'Java', 'Go', 'Rust', 'AWS', 'GCP', 'Docker', 'Kubernetes', 'PostgreSQL', 'Machine Learning', 'Product Management', 'UX Design', 'Team Leadership', 'System Design', 'Agile', 'DevOps'];

export default function ShortList() {
  const [view, setView] = useState('landing');
  const [userType, setUserType] = useState('seeker');
  const [step, setStep] = useState(1);
  const [companyStep, setCompanyStep] = useState(1);
  const [tab, setTab] = useState('browse');
  const [watched, setWatched] = useState([1, 5, 6, 8]);
  const [invited, setInvited] = useState([]);
  const [importing, setImporting] = useState(false);
  const [search, setSearch] = useState('');
  const [selCand, setSelCand] = useState(null);
  const [selJob, setSelJob] = useState(null);
  const [showNotifs, setShowNotifs] = useState(false);
  const [companyVerified, setCompanyVerified] = useState(false);
  const [companyProfile, setCompanyProfile] = useState({
    name: '', website: '', industry: '', size: '', description: '', logo: '',
    contactName: '', contactEmail: '', contactTitle: '', verified: false,
    locations: [], benefits: [], culture: ''
  });
  const [companyJobs, setCompanyJobs] = useState([
    { id: 101, title: 'Senior Administrative Assistant', dept: 'Administration', location: 'The Woodlands, TX', salary: '$55k - $70k', status: 'open', type: 'Full-time', remote: 'On-site', description: 'Supporting executive leadership with administrative tasks...', requirements: ['3+ years experience', 'MS Office proficiency', 'Strong communication'], watchers: 89, applications: 15, invited: 4, views: 1245, posted: '2024-12-15' },
    { id: 102, title: 'RN II NICU', dept: 'Nursing', location: 'The Woodlands, TX', salary: '$75k - $95k', status: 'open', type: 'Full-time', remote: 'On-site', description: 'Providing critical care for newborns in the NICU...', requirements: ['BSN required', 'NICU experience', 'BLS/NRP certified'], watchers: 156, applications: 22, invited: 8, views: 2341, posted: '2024-12-10' },
    { id: 103, title: 'Payroll Manager', dept: 'Finance', location: 'The Woodlands, TX', salary: '$85k - $110k', status: 'open', type: 'Full-time', remote: 'Hybrid', description: 'Managing payroll operations for 500+ employees...', requirements: ['5+ years payroll experience', 'ADP/Workday', 'CPA preferred'], watchers: 67, applications: 12, invited: 3, views: 987, posted: '2024-12-20' },
    { id: 104, title: 'Procurement Specialist', dept: 'Operations', location: 'The Woodlands, TX', salary: '$65k - $85k', status: 'filled', type: 'Full-time', remote: 'Hybrid', description: 'Sourcing and negotiating vendor contracts...', requirements: ['Supply chain background', 'Negotiation skills', 'SAP experience'], watchers: 45, applications: 18, invited: 5, views: 756, posted: '2024-11-01', filledDate: '2024-12-05' },
    { id: 105, title: 'Junior Mobile Engineer', dept: 'Engineering', location: 'The Woodlands, TX', salary: '$70k - $90k', status: 'open', type: 'Full-time', remote: 'Remote', description: 'Developing iOS and Android applications...', requirements: ['React Native/Flutter', '1-3 years experience', 'CS degree'], watchers: 234, applications: 45, invited: 12, views: 3456, posted: '2024-12-18' },
    { id: 106, title: 'Dental Hygienist', dept: 'Healthcare', location: 'The Woodlands, TX', salary: '$60k - $80k', status: 'open', type: 'Full-time', remote: 'On-site', description: 'Providing dental hygiene services and patient education...', requirements: ['RDH license', '2+ years experience', 'Patient focused'], watchers: 78, applications: 14, invited: 6, views: 1123, posted: '2024-12-12' },
    { id: 107, title: 'Nurse Practitioner - CV Surgery', dept: 'Medical', location: 'The Woodlands, TX', salary: '$120k - $150k', status: 'open', type: 'Full-time', remote: 'On-site', description: 'First assist in cardiovascular surgical procedures...', requirements: ['NP certification', 'CV surgery experience', 'ACLS required'], watchers: 189, applications: 8, invited: 3, views: 2890, posted: '2024-12-08' },
    { id: 108, title: 'Strategy Analyst', dept: 'Strategy', location: 'The Woodlands, TX', salary: '$80k - $100k', status: 'open', type: 'Full-time', remote: 'Hybrid', description: 'Analyzing market trends and developing strategic recommendations...', requirements: ['MBA preferred', 'SQL/Tableau', 'Consulting background'], watchers: 112, applications: 28, invited: 7, views: 1678, posted: '2024-12-14' },
    { id: 109, title: 'Automotive Technician', dept: 'Service', location: 'The Woodlands, TX', salary: '$50k - $75k', status: 'open', type: 'Full-time', remote: 'On-site', description: 'Diagnosing and repairing vehicle mechanical issues...', requirements: ['ASE certified', '3+ years experience', 'Own tools'], watchers: 56, applications: 19, invited: 4, views: 890, posted: '2024-12-16' },
    { id: 110, title: 'Remote Benefits Coordinator', dept: 'HR', location: 'Remote (The Woodlands, TX)', salary: '$45k - $60k', status: 'open', type: 'Full-time', remote: 'Remote', description: 'Coordinating employee benefits enrollment and inquiries...', requirements: ['Benefits administration', 'HRIS systems', 'Customer service'], watchers: 198, applications: 67, invited: 15, views: 4521, posted: '2024-12-19' },
  ]);

  // Woodlands-area companies from the uploaded data
  const woodlandsCompanies = [
    { id: 'c1', name: 'Houston Methodist', industry: 'Healthcare', size: '5000+', logo: 'HM', description: 'Leading healthcare system in Texas', verified: true },
    { id: 'c2', name: 'Memorial Hermann Health System', industry: 'Healthcare', size: '5000+', logo: 'MH', description: 'Comprehensive healthcare network', verified: true },
    { id: 'c3', name: 'JPMorgan Chase Bank', industry: 'Finance', size: '5000+', logo: 'JP', description: 'Global financial services firm', verified: true },
    { id: 'c4', name: 'WHC Energy Services', industry: 'Energy', size: '201-1000', logo: 'WE', description: 'Oil & gas services company', verified: true },
    { id: 'c5', name: 'Baylor College of Medicine', industry: 'Healthcare', size: '1001-5000', logo: 'BC', description: 'Premier medical education institution', verified: true },
    { id: 'c6', name: 'EverCommerce', industry: 'Technology', size: '1001-5000', logo: 'EC', description: 'Business software solutions', verified: true },
    { id: 'c7', name: 'Clarity Innovations', industry: 'Technology', size: '51-200', logo: 'CI', description: 'Mobile app development studio', verified: true },
    { id: 'c8', name: 'Bear Branch Family Dentistry', industry: 'Healthcare', size: '1-50', logo: 'BB', description: 'Family dental practice', verified: true },
    { id: 'c9', name: 'Christian Brothers Automotive', industry: 'Automotive', size: '51-200', logo: 'CB', description: 'Automotive repair services', verified: true },
    { id: 'c10', name: 'McQuade Organization', industry: 'Insurance', size: '51-200', logo: 'MQ', description: 'Insurance and benefits consulting', verified: true },
  ];

  const [jobWatchers, setJobWatchers] = useState({
    101: [1, 4, 11, 18], // Senior Administrative Assistant
    102: [5, 9, 15, 17, 19], // RN II NICU  
    103: [2, 8, 13], // Payroll Manager
    104: [3, 6, 12], // Procurement Specialist
    105: [1, 3, 6, 10, 12, 14, 16], // Junior Mobile Engineer
    106: [7, 11, 19], // Dental Hygienist
    107: [5, 9, 15, 17], // Nurse Practitioner
    108: [2, 4, 8, 13, 18], // Strategy Analyst
    109: [10, 12, 20], // Automotive Technician
    110: [1, 3, 5, 7, 9, 11, 13, 15, 17, 19], // Remote Benefits Coordinator
  });
  const [notifications, setNotifications] = useState([
    { id: 1, type: 'vacancy', title: 'Position Now Open', message: 'Data Science Manager at Quantum Analytics is now open!', time: '2 hours ago', read: false, jobId: 3 },
    { id: 2, type: 'invite', title: 'Company Invitation', message: 'Stripe invited you to apply for Staff Frontend Engineer', time: '5 hours ago', read: false, company: 'Stripe' },
    { id: 3, type: 'invite', title: 'Company Invitation', message: 'Databricks invited you to apply for Director of Engineering', time: '1 day ago', read: false, company: 'Databricks' },
    { id: 4, type: 'match', title: 'New Match', message: 'New 96% match: Staff Frontend Engineer at Stripe', time: '1 day ago', read: true, jobId: 6 },
    { id: 5, type: 'application', title: 'Application Viewed', message: 'Netflix viewed your application for Engineering Manager', time: '2 days ago', read: true, company: 'Netflix' },
    { id: 6, type: 'vacancy', title: 'Position Now Open', message: 'Head of Product at Figma is now open!', time: '3 days ago', read: true, jobId: 7 },
    { id: 7, type: 'profile', title: 'Profile Views', message: '12 companies viewed your profile this week', time: '3 days ago', read: true },
    { id: 8, type: 'watcher', title: 'Watcher Alert', message: '50+ new watchers on Senior Software Engineer position', time: '1 day ago', read: false, forCompany: true },
    { id: 9, type: 'application', title: 'New Application', message: 'Alexandra Chen applied for Senior Software Engineer', time: '4 hours ago', read: false, forCompany: true, candidateId: 1 },
    { id: 10, type: 'application', title: 'New Application', message: 'Marcus Johnson applied for Senior Software Engineer', time: '6 hours ago', read: false, forCompany: true, candidateId: 2 },
    { id: 11, type: 'response', title: 'Invitation Accepted', message: 'David Park accepted your invitation to apply', time: '1 day ago', read: true, forCompany: true, candidateId: 4 },
  ]);
  const [profile, setProfile] = useState({
    firstName: '', lastName: '', email: '', currentTitle: '', currentCompany: '',
    yearsExperience: '', searchStatus: '', skills: [], workStyle: [], salaryMin: '', salaryMax: '', idealRole: ''
  });

  const update = (f, v) => setProfile(p => ({ ...p, [f]: v }));
  const updateCompany = (f, v) => setCompanyProfile(p => ({ ...p, [f]: v }));
  const toggleSkill = (sk) => setProfile(p => ({ ...p, skills: p.skills.includes(sk) ? p.skills.filter(s => s !== sk) : [...p.skills, sk] }));

  const importLinkedIn = () => {
    setImporting(true);
    setTimeout(() => {
      setProfile(p => ({ ...p, firstName: 'Jordan', lastName: 'Davis', currentTitle: 'Senior Product Designer', currentCompany: 'Acme Corp', yearsExperience: '6-10', skills: ['UX Design', 'Product Management', 'Team Leadership', 'Agile'] }));
      setImporting(false);
    }, 1500);
  };

  const userNotifs = notifications.filter(n => userType === 'seeker' ? !n.forCompany : n.forCompany);
  const unreadCount = userNotifs.filter(n => !n.read).length;

  const markAllRead = () => setNotifications(notifications.map(n => ({ ...n, read: true })));
  const markRead = (id) => setNotifications(notifications.map(n => n.id === id ? { ...n, read: true } : n));

  const getNotifIcon = (type) => {
    switch(type) {
      case 'vacancy': return '🔔';
      case 'invite': return '✉️';
      case 'match': return '⭐';
      case 'application': return '📄';
      case 'profile': return '👁';
      case 'watcher': return '👀';
      case 'response': return '✅';
      default: return '📣';
    }
  };

  const jobs = mockJobs.filter(j => j.title.toLowerCase().includes(search.toLowerCase()) || j.company.toLowerCase().includes(search.toLowerCase()) || j.location.toLowerCase().includes(search.toLowerCase()));
  const candidates = mockCandidates.filter(c => c.name.toLowerCase().includes(search.toLowerCase()) || c.title.toLowerCase().includes(search.toLowerCase()) || c.skills.some(s => s.toLowerCase().includes(search.toLowerCase())));

  // LANDING
  if (view === 'landing') return (
    <div style={s.landing}>
      <style>{css}</style>
      <div style={s.landingBg}/>
      <header style={s.lHeader}>
        <div style={s.logo}><span style={s.logoIcon}>◈</span><span style={s.logoText}>ShortList</span></div>
        <nav style={s.lNav}>
          <a href="#" style={s.lLink}>How It Works</a>
          <a href="#" style={s.lLink}>For Companies</a>
          <button style={s.signIn} onClick={() => { setView('dashboard'); setUserType('seeker'); }}>Sign In</button>
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
      <footer style={s.lFooter}>© 2024 ShortList.ai</footer>
    </div>
  );

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
          <p style={s.sSub}>Import from LinkedIn or fill in manually</p>
          <div style={s.imports}>
            <button style={s.impBtn} onClick={importLinkedIn} disabled={importing}>{importing ? '⏳ Importing...' : '🔗 Import from LinkedIn'}</button>
            <button style={s.impBtn}>📄 Upload Resume</button>
          </div>
          <div style={s.divider}><span>or enter manually</span></div>
          <div style={s.grid}>
            <input style={s.inp} placeholder="First Name" value={profile.firstName} onChange={e => update('firstName', e.target.value)} />
            <input style={s.inp} placeholder="Last Name" value={profile.lastName} onChange={e => update('lastName', e.target.value)} />
            <input style={s.inp} placeholder="Email" value={profile.email} onChange={e => update('email', e.target.value)} />
            <input style={s.inp} placeholder="Current Job Title" value={profile.currentTitle} onChange={e => update('currentTitle', e.target.value)} />
            <input style={s.inp} placeholder="Current Company" value={profile.currentCompany} onChange={e => update('currentCompany', e.target.value)} />
            <select style={s.inp} value={profile.yearsExperience} onChange={e => update('yearsExperience', e.target.value)}>
              <option value="">Years of Experience</option>
              <option value="0-2">0-2 years</option><option value="3-5">3-5 years</option><option value="6-10">6-10 years</option><option value="10+">10+ years</option>
            </select>
          </div>
        </div>}
        {step === 2 && <div style={s.step}>
          <h1 style={s.sTitle}>How's your job search going?</h1>
          <p style={s.sSub}>This helps us prioritize the right opportunities</p>
          <div style={s.statuses}>
            {[{v:'actively-looking',i:'🚀',t:'Actively Looking',d:'Ready to make a move'},{v:'open-to-offers',i:'👋',t:'Open to Offers',d:'Happy but would consider the right role'},{v:'just-looking',i:'👀',t:'Just Looking',d:'Curious about the market'}].map(o => (
              <button key={o.v} style={{...s.stBtn, ...(profile.searchStatus===o.v?s.stBtnA:{})}} onClick={() => update('searchStatus',o.v)}>
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
          <div style={s.pref}><label style={s.lbl}>Work Style</label><div style={s.wsRow}>{['🏠 Remote','🔄 Hybrid','🏢 On-site'].map(w => <button key={w} style={{...s.wsBtn, ...(profile.workStyle.includes(w)?s.wsBtnA:{})}} onClick={() => setProfile(p => ({...p, workStyle: p.workStyle.includes(w)?p.workStyle.filter(x=>x!==w):[...p.workStyle,w]}))}>{w}</button>)}</div></div>
          <div style={s.pref}><label style={s.lbl}>Target Salary</label><div style={s.salRow}><input style={s.salInp} placeholder="$150,000" value={profile.salaryMin} onChange={e => update('salaryMin',e.target.value)}/><span>to</span><input style={s.salInp} placeholder="$200,000" value={profile.salaryMax} onChange={e => update('salaryMax',e.target.value)}/></div></div>
          <div style={s.pref}><label style={s.lbl}>🔒 Hide from (e.g., current employer)</label><input style={s.inp} placeholder="Company names, comma separated"/></div>
          <div style={s.pref}><label style={s.lbl}>Describe your ideal role</label><textarea style={s.ta} placeholder="I'm looking for..." value={profile.idealRole} onChange={e => update('idealRole',e.target.value)} rows={3}/></div>
        </div>}
        <div style={s.sNav}>{step > 1 && <button style={s.back} onClick={() => setStep(step-1)}>← Back</button>}<button style={s.next} onClick={() => step < 4 ? setStep(step+1) : setView('dashboard')}>{step===4?'Complete Setup →':'Continue →'}</button></div>
      </main>
    </div>
  );

  // COMPANY ONBOARDING
  if (view === 'companyOnboarding') return (
    <div style={s.onboard}>
      <style>{css}</style>
      <header style={s.oHeader}>
        <div style={s.logo}><span style={s.logoIcon}>◈</span><span style={s.logoText}>ShortList</span></div>
        <div style={s.prog}><span>Step {companyStep}/4</span><div style={s.progBar}><div style={{...s.progFill, width:`${companyStep*25}%`}}/></div></div>
        <button style={s.skip} onClick={() => { setCompanyVerified(true); setView('dashboard'); }}>Skip</button>
      </header>
      <main style={s.oMain}>
        {companyStep === 1 && <div style={s.step}>
          <h1 style={s.sTitle}>Let's verify your company</h1>
          <p style={s.sSub}>This helps candidates trust that your opportunities are legitimate</p>
          <div style={s.verifyBox}>
            <div style={s.verifyIcon}>🏢</div>
            <h3 style={s.verifyTitle}>Company Verification</h3>
            <p style={s.verifyDesc}>We'll verify your company through your work email domain or LinkedIn company page.</p>
          </div>
          <div style={s.grid}>
            <input style={{...s.inp, gridColumn:'1/-1'}} placeholder="Company Name" value={companyProfile.name} onChange={e => updateCompany('name', e.target.value)} />
            <input style={s.inp} placeholder="Company Website" value={companyProfile.website} onChange={e => updateCompany('website', e.target.value)} />
            <select style={s.inp} value={companyProfile.industry} onChange={e => updateCompany('industry', e.target.value)}>
              <option value="">Industry</option>
              <option value="Technology">Technology</option><option value="Finance">Finance</option><option value="Healthcare">Healthcare</option><option value="E-commerce">E-commerce</option><option value="SaaS">SaaS</option><option value="AI/ML">AI/ML</option><option value="Other">Other</option>
            </select>
            <select style={s.inp} value={companyProfile.size} onChange={e => updateCompany('size', e.target.value)}>
              <option value="">Company Size</option>
              <option value="1-50">1-50 employees</option><option value="51-200">51-200 employees</option><option value="201-1000">201-1000 employees</option><option value="1001-5000">1001-5000 employees</option><option value="5000+">5000+ employees</option>
            </select>
            <textarea style={{...s.ta, gridColumn:'1/-1'}} placeholder="Brief company description..." value={companyProfile.description} onChange={e => updateCompany('description', e.target.value)} rows={3}/>
          </div>
        </div>}
        {companyStep === 2 && <div style={s.step}>
          <h1 style={s.sTitle}>Your contact information</h1>
          <p style={s.sSub}>This will be used for verification and candidate communications</p>
          <div style={s.grid}>
            <input style={s.inp} placeholder="Your Full Name" value={companyProfile.contactName} onChange={e => updateCompany('contactName', e.target.value)} />
            <input style={s.inp} placeholder="Your Job Title" value={companyProfile.contactTitle} onChange={e => updateCompany('contactTitle', e.target.value)} />
            <input style={{...s.inp, gridColumn:'1/-1'}} placeholder="Work Email (must match company domain)" value={companyProfile.contactEmail} onChange={e => updateCompany('contactEmail', e.target.value)} />
          </div>
          <div style={s.emailVerify}>
            <div style={s.emailVerifyIcon}>📧</div>
            <div style={s.emailVerifyText}>
              <strong>Email Verification</strong>
              <p>We'll send a verification link to your work email to confirm your company affiliation.</p>
            </div>
          </div>
        </div>}
        {companyStep === 3 && <div style={s.step}>
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
              {['Health Insurance','401k Match','Unlimited PTO','Remote Work','Stock Options','Parental Leave','Learning Budget','Gym Membership','Free Meals','Mental Health'].map(b => (
                <button key={b} style={{...s.benefitBtn, ...(companyProfile.benefits?.includes(b)?s.benefitBtnA:{})}} onClick={() => setCompanyProfile(p => ({...p, benefits: p.benefits?.includes(b)?p.benefits.filter(x=>x!==b):[...(p.benefits||[]),b]}))}>
                  {companyProfile.benefits?.includes(b) && '✓ '}{b}
                </button>
              ))}
            </div>
          </div>
          <div style={s.pref}>
            <label style={s.lbl}>Describe your company culture</label>
            <textarea style={s.ta} placeholder="What's it like to work at your company? What values drive your team?" value={companyProfile.culture} onChange={e => updateCompany('culture', e.target.value)} rows={4}/>
          </div>
        </div>}
        {companyStep === 4 && <div style={s.step}>
          <div style={s.verifySuccess}>
            <div style={s.successIcon}>✅</div>
            <h1 style={s.sTitle}>You're all set!</h1>
            <p style={s.sSub}>Your company profile is ready. Here's what you can do now:</p>
          </div>
          <div style={s.featureList}>
            <div style={s.featureItem}><span style={s.featureIcon}>📋</span><div><strong>Manage Positions</strong><p>Create, edit, and track your job listings</p></div></div>
            <div style={s.featureItem}><span style={s.featureIcon}>🔍</span><div><strong>Search Talent</strong><p>Confidentially browse our candidate database</p></div></div>
            <div style={s.featureItem}><span style={s.featureIcon}>👀</span><div><strong>See Who's Watching</strong><p>View candidates interested in your positions</p></div></div>
            <div style={s.featureItem}><span style={s.featureIcon}>✉️</span><div><strong>Invite to Apply</strong><p>Reach out to top candidates directly</p></div></div>
          </div>
        </div>}
        <div style={s.sNav}>
          {companyStep > 1 && <button style={s.back} onClick={() => setCompanyStep(companyStep-1)}>← Back</button>}
          <button style={s.next} onClick={() => { 
            if (companyStep < 4) setCompanyStep(companyStep+1); 
            else { setCompanyVerified(true); setView('dashboard'); setTab('positions'); }
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
            <button style={{...s.togBtn, ...(userType==='company'?s.togA:{})}} onClick={() => {setUserType('company');setTab('talent');setSearch('');}}>Company</button>
          </div>
          <div style={s.notifWrap}>
            <div style={s.notif} onClick={() => setShowNotifs(!showNotifs)}>🔔{unreadCount > 0 && <span style={s.notifDot}>{unreadCount}</span>}</div>
            {showNotifs && <div style={s.notifDrop}>
              <div style={s.notifHeader}><span style={s.notifTitle}>Notifications</span><button style={s.markRead} onClick={markAllRead}>Mark all read</button></div>
              <div style={s.notifList}>
                {userNotifs.length === 0 ? <div style={s.notifEmpty}>No notifications yet</div> :
                  userNotifs.map(n => (
                    <div key={n.id} style={{...s.notifItem, background: n.read ? 'transparent' : 'rgba(99,102,241,0.05)'}} onClick={() => markRead(n.id)}>
                      <div style={{...s.notifIcon, background: n.type==='vacancy'?'#dcfce7':n.type==='invite'?'#e9d5ff':n.type==='match'?'#fef3c7':n.type==='application'?'#dbeafe':n.type==='response'?'#dcfce7':'#f1f5f9'}}>{getNotifIcon(n.type)}</div>
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
          <div style={s.avatar}>{profile.firstName?.[0]||'J'}{profile.lastName?.[0]||'D'}</div>
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
            <button style={{...s.navBtn, ...(tab==='talent'?s.navA:{})}} onClick={() => setTab('talent')}>🔍 Discover Talent</button>
            <button style={{...s.navBtn, ...(tab==='invited'?s.navA:{})}} onClick={() => setTab('invited')}>✉️ Invited ({invited.length})</button>
            <button style={{...s.navBtn, ...(tab==='positions'?s.navA:{})}} onClick={() => setTab('positions')}>💼 Positions</button>
          </>}
        </nav>

        {/* SEEKER BROWSE/MATCHES */}
        {userType==='seeker' && (tab==='browse'||tab==='matches') && <div>
          <h1 style={s.secTitle}>{tab==='browse'?'Discover Opportunities':'Your Top Matches'}</h1>
          <p style={s.secSub}>{tab==='browse'?'Watch positions and get notified when they open':'Roles that match your profile'}</p>
          <div style={s.searchBar}><span style={s.searchIco}>🔍</span><input style={s.searchInp} placeholder="Search jobs, companies, locations..." value={search} onChange={e => setSearch(e.target.value)}/></div>
          <div style={s.info}>{jobs.length} positions found</div>
          <div style={s.jobGrid}>
            {(tab==='matches'?[...jobs].sort((a,b)=>b.matchScore-a.matchScore):jobs).map(j => (
              <div key={j.id} style={{...s.jobCard, borderLeft: j.matchScore>=90?'4px solid #6366f1':'none'}}>
                <div style={s.jHeader}><div style={s.jLogo}>{j.company[0]}</div><div style={s.jMeta}><strong>{j.company}</strong><span style={s.jLoc}>{j.location}</span></div><div style={{...s.match, background:j.matchScore>=90?'#6366f1':j.matchScore>=80?'#8b5cf6':'#a5b4fc'}}>{j.matchScore}%</div></div>
                <h3 style={s.jTitle}>{j.title}</h3>
                <p style={s.jSal}>{j.salary}</p>
                <div style={s.jTags}><span style={s.dept}>{j.dept}</span><span style={{...s.status, background:j.status==='open'?'#dcfce7':'#f1f5f9', color:j.status==='open'?'#166534':'#64748b'}}>{j.status==='open'?'● Open':'○ Filled'}</span></div>
                <div style={s.jFoot}><span style={s.watchers}>👁 {j.watchers}</span><button style={{...s.watchBtn, background:watched.includes(j.id)?'#6366f1':'transparent', color:watched.includes(j.id)?'#fff':'#6366f1'}} onClick={() => setWatched(w => w.includes(j.id)?w.filter(x=>x!==j.id):[...w,j.id])}>{watched.includes(j.id)?'✓ Watching':'+ Watch'}</button></div>
              </div>
            ))}
          </div>
        </div>}

        {/* WATCHLIST */}
        {userType==='seeker' && tab==='watchlist' && <div>
          <h1 style={s.secTitle}>Your Watchlist</h1>
          <p style={s.secSub}>We'll notify you when these positions open up</p>
          <div style={s.stats}><div style={s.stat}><span style={s.statNum}>{watched.length}</span><span style={s.statLbl}>Watching</span></div><div style={s.stat}><span style={s.statNum}>{mockJobs.filter(j=>watched.includes(j.id)&&j.status==='open').length}</span><span style={s.statLbl}>Now Open</span></div><div style={s.stat}><span style={s.statNum}>3</span><span style={s.statLbl}>Invites</span></div></div>
          {mockJobs.filter(j=>watched.includes(j.id)).map(j => (
            <div key={j.id} style={{...s.wItem, borderLeft:j.status==='open'?'4px solid #10b981':'4px solid transparent'}}>
              <div style={s.jLogo}>{j.company[0]}</div>
              <div style={s.wInfo}><strong>{j.title}</strong><span style={s.wMeta}>{j.company} • {j.location}</span><span style={s.wSal}>{j.salary}</span></div>
              <div style={s.wRight}>{j.status==='open'?<button style={s.applyBtn}>Apply Now →</button>:<span style={s.filledTag}>Filled</span>}<button style={s.rmBtn} onClick={() => setWatched(w=>w.filter(x=>x!==j.id))}>✕</button></div>
            </div>
          ))}
        </div>}

        {/* PROFILE */}
        {userType==='seeker' && tab==='profile' && <div>
          <div style={s.profCard}>
            <div style={s.profHeader}><div style={s.profAvatar}>{profile.firstName?.[0]||'J'}{profile.lastName?.[0]||'D'}</div><div style={s.profInfo}><h2>{profile.firstName||'Jordan'} {profile.lastName||'Davis'}</h2><p>{profile.currentTitle||'Senior Product Designer'} at {profile.currentCompany||'Acme Corp'}</p><p style={s.exp}>{profile.yearsExperience||'6-10'} years experience</p></div><button style={s.editBtn} onClick={() => {setStep(1);setView('onboarding');}}>Edit Profile</button></div>
            <div style={s.profSec}><h3>Search Status</h3><span style={{...s.sBadge, background:profile.searchStatus==='actively-looking'?'#dcfce7':profile.searchStatus==='open-to-offers'?'#e9d5ff':'#f1f5f9', color:profile.searchStatus==='actively-looking'?'#166534':profile.searchStatus==='open-to-offers'?'#7c3aed':'#64748b'}}>{profile.searchStatus==='actively-looking'?'🚀 Actively Looking':profile.searchStatus==='open-to-offers'?'👋 Open to Offers':'👀 Just Looking'}</span></div>
            {profile.skills.length>0 && <div style={s.profSec}><h3>Skills</h3><div style={s.profSkills}>{profile.skills.map(sk => <span key={sk} style={s.profSkill}>{sk}</span>)}</div></div>}
            <div style={s.profStats}><div style={s.profStat}><strong>147</strong><span>Profile Views</span></div><div style={s.profStat}><strong>23</strong><span>Companies Interested</span></div><div style={s.profStat}><strong>5</strong><span>Invites</span></div></div>
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
                  <button style={{...s.invBtn, background:invited.includes(c.id)?'#e2e8f0':'#6366f1', color:invited.includes(c.id)?'#64748b':'#fff'}} onClick={e => {e.stopPropagation(); !invited.includes(c.id) && setInvited(i=>[...i,c.id]);}}>{invited.includes(c.id)?'✓ Invited':'Invite'}</button>
                </div>
              </div>
            ))}
          </div>
        </div>}

        {/* COMPANY: INVITED */}
        {userType==='company' && tab==='invited' && <div>
          <h1 style={s.secTitle}>Invited Candidates</h1>
          {invited.length===0 ? <div style={s.empty}><div style={s.emptyIco}>✉️</div><h3>No invitations yet</h3><p>Start discovering talent!</p><button style={s.emptyBtn} onClick={() => setTab('talent')}>Discover Talent</button></div>
          : <div>{mockCandidates.filter(c=>invited.includes(c.id)).map(c => <div key={c.id} style={s.invItem}><div style={s.cAvatar}>{c.name.split(' ').map(n=>n[0]).join('')}</div><div style={s.invInfo}><strong>{c.name}</strong><span>{c.title} at {c.company}</span></div><span style={s.pending}>⏳ Pending</span></div>)}</div>}
        </div>}

        {/* COMPANY: POSITIONS */}
        {userType==='company' && tab==='positions' && <div>
          <div style={s.posHead}>
            <div>
              <h1 style={s.secTitle}>Your Positions</h1>
              <p style={s.secSub}>Manage your job listings and track candidate interest</p>
            </div>
            <button style={s.newPos} onClick={() => setSelJob({isNew: true, title:'', dept:'', location:'', salary:'', status:'open', type:'Full-time', remote:'Hybrid', description:'', requirements:[], watchers:0, applications:0, invited:0, views:0})}>+ New Position</button>
          </div>
          <div style={s.posStats}>
            <div style={s.posStat2}><span style={s.posStat2Num}>{companyJobs.filter(j=>j.status==='open').length}</span><span style={s.posStat2Lbl}>Open Positions</span></div>
            <div style={s.posStat2}><span style={s.posStat2Num}>{Object.values(jobWatchers).reduce((a,w)=>a+w.length,0)}</span><span style={s.posStat2Lbl}>Total Watchers</span></div>
            <div style={s.posStat2}><span style={s.posStat2Num}>{companyJobs.reduce((a,j)=>a+j.applications,0)}</span><span style={s.posStat2Lbl}>Applications</span></div>
            <div style={s.posStat2}><span style={s.posStat2Num}>{companyJobs.reduce((a,j)=>a+j.views,0).toLocaleString()}</span><span style={s.posStat2Lbl}>Total Views</span></div>
          </div>
          {companyJobs.map(job => (
            <div key={job.id} style={s.posCard2}>
              <div style={s.posCard2Header}>
                <div style={s.posCard2Info}>
                  <h3 style={s.posCard2Title}>{job.title}</h3>
                  <p style={s.posCard2Meta}>{job.dept} • {job.location} • {job.salary} • {job.remote}</p>
                </div>
                <div style={s.posCard2Actions}>
                  <span style={{...s.posSt, background:job.status==='open'?'#dcfce7':'#f1f5f9', color:job.status==='open'?'#166534':'#64748b'}}>{job.status==='open'?'● Open':'○ Filled'}</span>
                  <button style={s.posEditBtn} onClick={() => setSelJob(job)}>Edit</button>
                </div>
              </div>
              <div style={s.posCard2Stats}>
                <div style={s.posCard2Stat}><span style={s.posCard2StatIcon}>👁</span><strong>{job.views.toLocaleString()}</strong><span>views</span></div>
                <div style={s.posCard2Stat}><span style={s.posCard2StatIcon}>👀</span><strong>{jobWatchers[job.id]?.length || 0}</strong><span>watching</span></div>
                <div style={s.posCard2Stat}><span style={s.posCard2StatIcon}>📄</span><strong>{job.applications}</strong><span>applied</span></div>
                <div style={s.posCard2Stat}><span style={s.posCard2StatIcon}>✉️</span><strong>{job.invited}</strong><span>invited</span></div>
              </div>
              <div style={s.posCard2Footer}>
                <span style={s.posCard2Date}>Posted {job.posted}</span>
                <div style={s.posCard2Btns}>
                  <button style={s.viewWatchersBtn} onClick={() => { setTab('watchers'); setSelJob({...job, viewOnly: true}); }}>View Watchers ({jobWatchers[job.id]?.length || 0}) →</button>
                </div>
              </div>
            </div>
          ))}
        </div>}

        {/* COMPANY: WATCHERS */}
        {userType==='company' && tab==='watchers' && <div>
          <button style={s.backLink} onClick={() => { setTab('positions'); setSelJob(null); }}>← Back to Positions</button>
          <h1 style={s.secTitle}>Candidates Watching: {selJob?.title}</h1>
          <p style={s.secSub}>These candidates are interested in this position and will be notified if it opens</p>
          <div style={s.watcherFilters}>
            <button style={{...s.filterBtn, ...(true?s.filterBtnA:{})}}>All ({jobWatchers[selJob?.id]?.length || 0})</button>
            <button style={s.filterBtn}>Actively Looking ({mockCandidates.filter(c => jobWatchers[selJob?.id]?.includes(c.id) && c.status==='actively-looking').length})</button>
            <button style={s.filterBtn}>Open to Offers ({mockCandidates.filter(c => jobWatchers[selJob?.id]?.includes(c.id) && c.status==='open-to-offers').length})</button>
          </div>
          <div style={s.watchersList}>
            {mockCandidates.filter(c => jobWatchers[selJob?.id]?.includes(c.id)).map(c => (
              <div key={c.id} style={s.watcherCard}>
                <div style={s.watcherMain}>
                  <div style={s.cAvatar}>{c.name.split(' ').map(n=>n[0]).join('')}</div>
                  <div style={s.watcherInfo}>
                    <div style={s.watcherName}>{c.name}</div>
                    <div style={s.watcherTitle}>{c.title} at {c.company}</div>
                    <div style={s.watcherMeta}>{c.loc} • {c.exp} years exp</div>
                  </div>
                  <div style={s.watcherRight}>
                    <span style={{...s.cMatch, background:c.matchScore>=90?'#6366f1':'#dcfce7', color:c.matchScore>=90?'#fff':'#166534'}}>{c.matchScore}% match</span>
                    <span style={{...s.sBadge, fontSize:'11px', padding:'4px 8px', marginTop:'8px', background:c.status==='actively-looking'?'#dcfce7':c.status==='open-to-offers'?'#e9d5ff':'#f1f5f9', color:c.status==='actively-looking'?'#166534':c.status==='open-to-offers'?'#7c3aed':'#64748b'}}>{c.status==='actively-looking'?'🚀 Active':c.status==='open-to-offers'?'👋 Open':'👀 Looking'}</span>
                  </div>
                </div>
                <div style={s.watcherSkills}>{c.skills.slice(0,4).map(sk => <span key={sk} style={s.cSkill}>{sk}</span>)}</div>
                <div style={s.watcherActions}>
                  <button style={s.viewProfileBtn} onClick={() => setSelCand(c)}>View Profile</button>
                  <button style={{...s.invBtn, background:invited.includes(c.id)?'#e2e8f0':'#6366f1', color:invited.includes(c.id)?'#64748b':'#fff'}} onClick={() => !invited.includes(c.id) && setInvited(i=>[...i,c.id])}>{invited.includes(c.id)?'✓ Invited':'Invite to Apply'}</button>
                </div>
              </div>
            ))}
            {(!jobWatchers[selJob?.id] || jobWatchers[selJob?.id].length === 0) && <div style={s.empty}><div style={s.emptyIco}>👀</div><h3>No watchers yet</h3><p>Candidates will appear here when they start watching this position</p></div>}
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
          <div style={s.mActions}><button style={{...s.mInvBtn, background:invited.includes(selCand.id)?'#e2e8f0':'#6366f1', color:invited.includes(selCand.id)?'#64748b':'#fff'}} onClick={() => {!invited.includes(selCand.id) && setInvited(i=>[...i,selCand.id]); setSelCand(null);}}>{invited.includes(selCand.id)?'✓ Already Invited':'Invite to Apply'}</button><button style={s.mSecBtn}>Save for Later</button></div>
        </div>
      </div>}

      {/* JOB EDIT MODAL */}
      {selJob && !selJob.viewOnly && <div style={s.overlay} onClick={() => setSelJob(null)}>
        <div style={{...s.modal, maxWidth:'600px'}} onClick={e => e.stopPropagation()}>
          <button style={s.modalX} onClick={() => setSelJob(null)}>✕</button>
          <h2 style={{marginBottom:'24px'}}>{selJob.isNew ? 'Create New Position' : 'Edit Position'}</h2>
          <div style={s.grid}>
            <input style={{...s.inp, gridColumn:'1/-1'}} placeholder="Job Title" value={selJob.title} onChange={e => setSelJob({...selJob, title: e.target.value})} />
            <select style={s.inp} value={selJob.dept} onChange={e => setSelJob({...selJob, dept: e.target.value})}>
              <option value="">Department</option>
              <option value="Engineering">Engineering</option><option value="Design">Design</option><option value="Product">Product</option><option value="Data">Data</option><option value="Marketing">Marketing</option><option value="Sales">Sales</option><option value="HR">HR</option>
            </select>
            <input style={s.inp} placeholder="Location" value={selJob.location} onChange={e => setSelJob({...selJob, location: e.target.value})} />
            <input style={s.inp} placeholder="Salary Range (e.g., $150k - $200k)" value={selJob.salary} onChange={e => setSelJob({...selJob, salary: e.target.value})} />
            <select style={s.inp} value={selJob.remote} onChange={e => setSelJob({...selJob, remote: e.target.value})}>
              <option value="Remote">Remote</option><option value="Hybrid">Hybrid</option><option value="On-site">On-site</option>
            </select>
            <select style={s.inp} value={selJob.type} onChange={e => setSelJob({...selJob, type: e.target.value})}>
              <option value="Full-time">Full-time</option><option value="Part-time">Part-time</option><option value="Contract">Contract</option>
            </select>
            <select style={s.inp} value={selJob.status} onChange={e => setSelJob({...selJob, status: e.target.value})}>
              <option value="open">Open</option><option value="filled">Filled</option><option value="paused">Paused</option>
            </select>
            <textarea style={{...s.ta, gridColumn:'1/-1'}} placeholder="Job description..." value={selJob.description} onChange={e => setSelJob({...selJob, description: e.target.value})} rows={4}/>
          </div>
          <div style={s.mActions}>
            <button style={s.mInvBtn} onClick={() => {
              if (selJob.isNew) {
                setCompanyJobs([...companyJobs, {...selJob, id: Date.now(), isNew: undefined, watchers: 0, applications: 0, invited: 0, views: 0, posted: new Date().toISOString().split('T')[0]}]);
              } else {
                setCompanyJobs(companyJobs.map(j => j.id === selJob.id ? selJob : j));
              }
              setSelJob(null);
            }}>{selJob.isNew ? 'Create Position' : 'Save Changes'}</button>
            {!selJob.isNew && <button style={{...s.mSecBtn, color:'#ef4444'}} onClick={() => { setCompanyJobs(companyJobs.filter(j => j.id !== selJob.id)); setSelJob(null); }}>Delete</button>}
            <button style={s.mSecBtn} onClick={() => setSelJob(null)}>Cancel</button>
          </div>
        </div>
      </div>}
    </div>
  );
}

const css = `@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Fraunces:wght@500;600;700&display=swap');*{box-sizing:border-box;margin:0;padding:0}body{font-family:'DM Sans',sans-serif}`;

const s = {
  landing:{minHeight:'100vh',background:'#0f0f1a',color:'#fff',fontFamily:'"DM Sans",sans-serif',position:'relative'},
  landingBg:{position:'absolute',inset:0,background:'radial-gradient(circle at 30% 20%,rgba(99,102,241,0.15),transparent 50%),radial-gradient(circle at 70% 60%,rgba(139,92,246,0.1),transparent 50%)',pointerEvents:'none'},
  lHeader:{position:'relative',zIndex:10,padding:'24px 48px',display:'flex',justifyContent:'space-between',alignItems:'center'},
  logo:{display:'flex',alignItems:'center',gap:'10px'},logoIcon:{fontSize:'28px',color:'#818cf8'},logoText:{fontFamily:'"Fraunces",serif',fontSize:'24px',fontWeight:600},
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
  avatar:{width:'40px',height:'40px',borderRadius:'10px',background:'#6366f1',color:'#fff',display:'flex',alignItems:'center',justifyContent:'center',fontWeight:600,fontSize:'14px'},
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
  posCard:{background:'#fff',borderRadius:'14px',padding:'24px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)',marginBottom:'16px'},
  posMain:{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:'16px'},
  posTitle:{fontSize:'17px',fontWeight:600,marginBottom:'4px'},posMeta:{color:'#64748b',fontSize:'14px'},
  posSt:{padding:'6px 14px',borderRadius:'20px',fontSize:'13px',fontWeight:600},
  posStats:{display:'flex',gap:'32px',paddingTop:'16px',borderTop:'1px solid #f1f5f9',textAlign:'center'},
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
  verifyBox:{background:'linear-gradient(135deg,#f0f0ff,#e9d5ff)',borderRadius:'16px',padding:'32px',textAlign:'center',marginBottom:'32px'},
  verifyIcon:{fontSize:'48px',marginBottom:'16px'},
  verifyTitle:{fontSize:'20px',fontWeight:600,marginBottom:'8px'},
  verifyDesc:{color:'#64748b',fontSize:'14px'},
  emailVerify:{display:'flex',gap:'16px',alignItems:'flex-start',background:'#f8fafc',borderRadius:'12px',padding:'20px',marginTop:'24px'},
  emailVerifyIcon:{fontSize:'32px'},
  emailVerifyText:{flex:1},
  benefitsGrid:{display:'flex',flexWrap:'wrap',gap:'10px'},
  benefitBtn:{padding:'10px 16px',background:'#f1f5f9',border:'none',borderRadius:'8px',fontSize:'13px',cursor:'pointer'},
  benefitBtnA:{background:'#6366f1',color:'#fff'},
  verifySuccess:{textAlign:'center',marginBottom:'32px'},
  successIcon:{fontSize:'64px',marginBottom:'16px'},
  featureList:{display:'flex',flexDirection:'column',gap:'16px'},
  featureItem:{display:'flex',gap:'16px',alignItems:'flex-start',background:'#f8fafc',borderRadius:'12px',padding:'20px'},
  featureIcon:{fontSize:'24px'},
  posStats:{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:'16px',marginBottom:'24px'},
  posStat2:{background:'#fff',padding:'20px',borderRadius:'12px',textAlign:'center',boxShadow:'0 1px 3px rgba(0,0,0,0.05)'},
  posStat2Num:{display:'block',fontSize:'28px',fontWeight:700,color:'#0f172a'},
  posStat2Lbl:{fontSize:'13px',color:'#64748b'},
  posCard2:{background:'#fff',borderRadius:'16px',padding:'24px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)',marginBottom:'16px'},
  posCard2Header:{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:'20px'},
  posCard2Info:{flex:1},
  posCard2Title:{fontSize:'18px',fontWeight:600,marginBottom:'6px'},
  posCard2Meta:{fontSize:'14px',color:'#64748b'},
  posCard2Actions:{display:'flex',gap:'12px',alignItems:'center'},
  posEditBtn:{padding:'8px 16px',background:'#f1f5f9',border:'none',borderRadius:'8px',fontSize:'13px',cursor:'pointer'},
  posCard2Stats:{display:'flex',gap:'32px',paddingBottom:'20px',borderBottom:'1px solid #f1f5f9',marginBottom:'16px'},
  posCard2Stat:{display:'flex',alignItems:'center',gap:'8px',fontSize:'14px',color:'#64748b'},
  posCard2StatIcon:{fontSize:'16px'},
  posCard2Footer:{display:'flex',justifyContent:'space-between',alignItems:'center'},
  posCard2Date:{fontSize:'13px',color:'#94a3b8'},
  posCard2Btns:{display:'flex',gap:'12px'},
  viewWatchersBtn:{padding:'10px 20px',background:'#6366f1',border:'none',borderRadius:'8px',color:'#fff',fontSize:'13px',fontWeight:600,cursor:'pointer'},
  backLink:{background:'none',border:'none',color:'#6366f1',fontSize:'14px',cursor:'pointer',marginBottom:'16px',padding:0},
  watcherFilters:{display:'flex',gap:'8px',marginBottom:'24px'},
  filterBtn:{padding:'10px 20px',background:'#f1f5f9',border:'none',borderRadius:'8px',fontSize:'13px',cursor:'pointer',color:'#64748b'},
  filterBtnA:{background:'#6366f1',color:'#fff'},
  watchersList:{display:'flex',flexDirection:'column',gap:'16px'},
  watcherCard:{background:'#fff',borderRadius:'16px',padding:'24px',boxShadow:'0 1px 3px rgba(0,0,0,0.05)'},
  watcherMain:{display:'flex',gap:'16px',alignItems:'flex-start',marginBottom:'16px'},
  watcherInfo:{flex:1},
  watcherName:{fontSize:'16px',fontWeight:600,marginBottom:'4px'},
  watcherTitle:{fontSize:'14px',color:'#334155'},
  watcherMeta:{fontSize:'13px',color:'#64748b',marginTop:'4px'},
  watcherRight:{display:'flex',flexDirection:'column',alignItems:'flex-end'},
  watcherSkills:{display:'flex',gap:'8px',flexWrap:'wrap',marginBottom:'16px'},
  watcherActions:{display:'flex',gap:'12px',paddingTop:'16px',borderTop:'1px solid #f1f5f9'},
  viewProfileBtn:{padding:'10px 20px',background:'#f1f5f9',border:'none',borderRadius:'8px',fontSize:'13px',cursor:'pointer'},
};
