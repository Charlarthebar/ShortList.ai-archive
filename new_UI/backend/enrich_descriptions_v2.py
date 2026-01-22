#!/usr/bin/env python3
"""
Enhanced job description enrichment with structured, detailed content.
Creates professional descriptions with bullet points, sections, and role-specific details.
"""

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def generate_structured_description(title, company, location, salary_range=None):
    """Generate a well-structured, detailed job description."""
    title_lower = title.lower()

    # Determine the role category and specific variant
    role_info = categorize_role(title_lower)

    # Build the description with proper structure
    desc = build_description(title, company, location, salary_range, role_info)

    return desc


def categorize_role(title_lower):
    """Categorize the role and extract specific details."""

    # Software/Tech roles
    if any(kw in title_lower for kw in ['software engineer', 'software developer', 'swe ', 'backend engineer', 'frontend engineer', 'full stack', 'fullstack']):
        seniority = get_seniority(title_lower)
        specialty = 'backend' if 'backend' in title_lower else 'frontend' if 'frontend' in title_lower else 'full stack' if 'full stack' in title_lower or 'fullstack' in title_lower else 'general'
        return {'category': 'software_engineer', 'seniority': seniority, 'specialty': specialty}

    if 'data scientist' in title_lower or 'machine learning' in title_lower or 'ml engineer' in title_lower:
        return {'category': 'data_scientist', 'seniority': get_seniority(title_lower)}

    if 'data analyst' in title_lower or 'business analyst' in title_lower or 'analytics' in title_lower:
        return {'category': 'data_analyst', 'seniority': get_seniority(title_lower)}

    if 'data engineer' in title_lower:
        return {'category': 'data_engineer', 'seniority': get_seniority(title_lower)}

    if 'devops' in title_lower or 'site reliability' in title_lower or 'sre ' in title_lower or 'platform engineer' in title_lower:
        return {'category': 'devops', 'seniority': get_seniority(title_lower)}

    if 'security' in title_lower and ('engineer' in title_lower or 'analyst' in title_lower):
        return {'category': 'security_engineer', 'seniority': get_seniority(title_lower)}

    if 'network engineer' in title_lower or 'network admin' in title_lower:
        return {'category': 'network_engineer', 'seniority': get_seniority(title_lower)}

    if 'it support' in title_lower or 'help desk' in title_lower or 'service desk' in title_lower or 'it technician' in title_lower or 'support technician' in title_lower:
        return {'category': 'it_support', 'seniority': get_seniority(title_lower)}

    # Product/Design
    if 'product manager' in title_lower or 'product owner' in title_lower:
        return {'category': 'product_manager', 'seniority': get_seniority(title_lower)}

    if 'ux' in title_lower or 'ui ' in title_lower or 'product design' in title_lower or 'user experience' in title_lower:
        return {'category': 'ux_designer', 'seniority': get_seniority(title_lower)}

    if 'graphic design' in title_lower or 'visual design' in title_lower:
        return {'category': 'graphic_designer', 'seniority': get_seniority(title_lower)}

    # Business roles
    if 'account executive' in title_lower or 'sales representative' in title_lower or 'sales ' in title_lower:
        return {'category': 'sales', 'seniority': get_seniority(title_lower)}

    if 'marketing' in title_lower:
        specialty = 'digital' if 'digital' in title_lower else 'content' if 'content' in title_lower else 'product' if 'product' in title_lower else 'general'
        return {'category': 'marketing', 'seniority': get_seniority(title_lower), 'specialty': specialty}

    if 'recruiter' in title_lower or 'talent acquisition' in title_lower:
        return {'category': 'recruiter', 'seniority': get_seniority(title_lower)}

    if 'human resources' in title_lower or 'hr ' in title_lower or ' hr' in title_lower:
        return {'category': 'hr', 'seniority': get_seniority(title_lower)}

    # Finance/Accounting
    if 'accountant' in title_lower or 'accounting' in title_lower:
        return {'category': 'accountant', 'seniority': get_seniority(title_lower)}

    if 'financial analyst' in title_lower or 'finance analyst' in title_lower:
        return {'category': 'financial_analyst', 'seniority': get_seniority(title_lower)}

    if 'controller' in title_lower:
        return {'category': 'controller', 'seniority': get_seniority(title_lower)}

    # Healthcare
    if 'physician' in title_lower or 'doctor' in title_lower or 'medical director' in title_lower:
        specialty = extract_medical_specialty(title_lower)
        return {'category': 'physician', 'specialty': specialty}

    if 'nurse practitioner' in title_lower or ' np ' in title_lower:
        return {'category': 'nurse_practitioner'}

    if 'registered nurse' in title_lower or ' rn ' in title_lower or 'nurse' in title_lower:
        return {'category': 'nurse', 'specialty': extract_nursing_specialty(title_lower)}

    if 'physical therapist' in title_lower or 'physical therapy' in title_lower:
        return {'category': 'physical_therapist'}

    if 'pharmacist' in title_lower:
        return {'category': 'pharmacist'}

    if 'medical assistant' in title_lower:
        return {'category': 'medical_assistant'}

    # Engineering (non-software)
    if 'mechanical engineer' in title_lower:
        return {'category': 'mechanical_engineer', 'seniority': get_seniority(title_lower)}

    if 'electrical engineer' in title_lower or 'electronic engineer' in title_lower:
        return {'category': 'electrical_engineer', 'seniority': get_seniority(title_lower)}

    if 'civil engineer' in title_lower:
        return {'category': 'civil_engineer', 'seniority': get_seniority(title_lower)}

    if 'chemical engineer' in title_lower:
        return {'category': 'chemical_engineer', 'seniority': get_seniority(title_lower)}

    if 'biomedical engineer' in title_lower or 'biopharma' in title_lower or 'bioprocess' in title_lower:
        return {'category': 'biomedical_engineer', 'seniority': get_seniority(title_lower)}

    if 'process engineer' in title_lower:
        return {'category': 'process_engineer', 'seniority': get_seniority(title_lower)}

    if 'quality engineer' in title_lower or 'qa engineer' in title_lower:
        return {'category': 'quality_engineer', 'seniority': get_seniority(title_lower)}

    if 'manufacturing engineer' in title_lower:
        return {'category': 'manufacturing_engineer', 'seniority': get_seniority(title_lower)}

    # Science
    if 'research scientist' in title_lower or 'scientist' in title_lower:
        specialty = 'biology' if 'bio' in title_lower else 'chemistry' if 'chem' in title_lower else 'physics' if 'physic' in title_lower else 'general'
        return {'category': 'scientist', 'seniority': get_seniority(title_lower), 'specialty': specialty}

    if 'chemist' in title_lower:
        return {'category': 'chemist', 'seniority': get_seniority(title_lower)}

    if 'biologist' in title_lower or 'biological' in title_lower:
        return {'category': 'biologist', 'seniority': get_seniority(title_lower)}

    # Project/Program Management
    if 'project manager' in title_lower:
        return {'category': 'project_manager', 'seniority': get_seniority(title_lower)}

    if 'program manager' in title_lower:
        return {'category': 'program_manager', 'seniority': get_seniority(title_lower)}

    if 'scrum master' in title_lower or 'agile coach' in title_lower:
        return {'category': 'scrum_master'}

    # Operations
    if 'operations manager' in title_lower or 'ops manager' in title_lower:
        return {'category': 'operations_manager', 'seniority': get_seniority(title_lower)}

    if 'supply chain' in title_lower or 'logistics' in title_lower:
        return {'category': 'supply_chain', 'seniority': get_seniority(title_lower)}

    if 'procurement' in title_lower or 'purchasing' in title_lower:
        return {'category': 'procurement', 'seniority': get_seniority(title_lower)}

    # Administrative
    if 'executive assistant' in title_lower or 'administrative assistant' in title_lower or 'office manager' in title_lower:
        return {'category': 'admin_assistant', 'seniority': get_seniority(title_lower)}

    if 'customer service' in title_lower or 'customer support' in title_lower or 'customer success' in title_lower:
        return {'category': 'customer_service', 'seniority': get_seniority(title_lower)}

    # Legal
    if 'paralegal' in title_lower:
        return {'category': 'paralegal'}

    if 'attorney' in title_lower or 'lawyer' in title_lower or 'legal counsel' in title_lower:
        return {'category': 'attorney', 'seniority': get_seniority(title_lower)}

    if 'compliance' in title_lower:
        return {'category': 'compliance', 'seniority': get_seniority(title_lower)}

    if 'regulatory' in title_lower:
        return {'category': 'regulatory_affairs', 'seniority': get_seniority(title_lower)}

    # Government/Public sector
    if 'contract specialist' in title_lower or 'contracting' in title_lower:
        return {'category': 'contract_specialist', 'seniority': get_seniority(title_lower)}

    if 'correctional officer' in title_lower:
        return {'category': 'correctional_officer', 'seniority': get_seniority(title_lower)}

    if 'police officer' in title_lower or 'law enforcement' in title_lower:
        return {'category': 'police_officer'}

    if 'inspector' in title_lower:
        return {'category': 'inspector', 'seniority': get_seniority(title_lower)}

    # Consulting
    if 'consultant' in title_lower or 'consulting' in title_lower:
        return {'category': 'consultant', 'seniority': get_seniority(title_lower)}

    # Estimating/Construction
    if 'estimator' in title_lower:
        return {'category': 'estimator', 'seniority': get_seniority(title_lower)}

    if 'construction' in title_lower:
        return {'category': 'construction', 'seniority': get_seniority(title_lower)}

    # Curatorial/Museum
    if 'curator' in title_lower or 'curatorial' in title_lower:
        return {'category': 'curator'}

    # Research/Academic
    if 'research' in title_lower and ('fellow' in title_lower or 'associate' in title_lower or 'assistant' in title_lower):
        return {'category': 'research_fellow'}

    # Fallback: try to determine if it's management or individual contributor
    if any(kw in title_lower for kw in ['director', 'vp ', 'vice president', 'head of', 'chief']):
        return {'category': 'executive', 'seniority': 'senior'}

    if any(kw in title_lower for kw in ['manager', 'lead', 'supervisor']):
        return {'category': 'manager', 'seniority': get_seniority(title_lower)}

    if 'engineer' in title_lower:
        return {'category': 'general_engineer', 'seniority': get_seniority(title_lower)}

    if 'analyst' in title_lower:
        return {'category': 'general_analyst', 'seniority': get_seniority(title_lower)}

    if 'specialist' in title_lower:
        return {'category': 'specialist', 'seniority': get_seniority(title_lower)}

    if 'coordinator' in title_lower:
        return {'category': 'coordinator'}

    if 'technician' in title_lower:
        return {'category': 'technician'}

    if 'assistant' in title_lower:
        return {'category': 'assistant'}

    # Default
    return {'category': 'general', 'seniority': get_seniority(title_lower)}


def get_seniority(title_lower):
    """Determine seniority level from title."""
    if any(kw in title_lower for kw in ['intern', 'internship']):
        return 'intern'
    if any(kw in title_lower for kw in ['junior', 'jr ', 'entry', 'associate', ' i ', ' 1 ', 'new grad']):
        return 'entry'
    if any(kw in title_lower for kw in ['senior', 'sr ', 'lead', 'principal', 'staff', ' iii', ' 3 ', 'director', 'vp ', 'head']):
        return 'senior'
    return 'mid'


def extract_medical_specialty(title_lower):
    """Extract medical specialty from title."""
    if 'primary care' in title_lower or 'family' in title_lower:
        return 'primary_care'
    if 'cardio' in title_lower:
        return 'cardiology'
    if 'neuro' in title_lower:
        return 'neurology'
    if 'oncolog' in title_lower:
        return 'oncology'
    if 'pediatr' in title_lower:
        return 'pediatrics'
    if 'psychiatr' in title_lower or 'mental health' in title_lower:
        return 'psychiatry'
    if 'emergency' in title_lower or 'urgent' in title_lower:
        return 'emergency'
    if 'surgeon' in title_lower or 'surgery' in title_lower:
        return 'surgery'
    return 'general'


def extract_nursing_specialty(title_lower):
    """Extract nursing specialty from title."""
    if 'icu' in title_lower or 'intensive care' in title_lower:
        return 'icu'
    if 'emergency' in title_lower or 'er ' in title_lower:
        return 'emergency'
    if 'pediatr' in title_lower:
        return 'pediatrics'
    if 'oncolog' in title_lower:
        return 'oncology'
    if 'mental health' in title_lower or 'psych' in title_lower:
        return 'mental_health'
    if 'surgical' in title_lower or 'or ' in title_lower:
        return 'surgical'
    return 'general'


# ============================================================================
# DESCRIPTION TEMPLATES
# ============================================================================

DESCRIPTIONS = {
    'software_engineer': {
        'overview': """We are looking for a talented {seniority_adj}Software Engineer to join our team and help build innovative software solutions. In this role, you will design, develop, and maintain high-quality applications that solve real business problems and delight users.""",
        'responsibilities': [
            "Design and implement scalable, maintainable software solutions using modern technologies and best practices",
            "Write clean, well-tested code and participate in code reviews to maintain high code quality standards",
            "Collaborate with product managers, designers, and other engineers to understand requirements and deliver features",
            "Debug and resolve technical issues, optimize application performance, and improve system reliability",
            "Contribute to technical architecture decisions and help establish engineering standards",
            "Mentor junior team members and share knowledge through documentation and tech talks"
        ],
        'qualifications': [
            "Strong proficiency in one or more programming languages (e.g., Python, Java, JavaScript, Go, C++)",
            "Experience with web frameworks, APIs, databases, and cloud platforms",
            "Solid understanding of software engineering principles, data structures, and algorithms",
            "Familiarity with version control (Git), CI/CD pipelines, and agile development methodologies",
            "Excellent problem-solving skills and attention to detail",
            "Strong communication skills and ability to work effectively in a team environment"
        ],
        'closing': """This is an excellent opportunity to work on challenging technical problems, grow your skills, and make a real impact. We offer competitive compensation, professional development opportunities, and a collaborative work environment where your contributions are valued."""
    },

    'data_scientist': {
        'overview': """We are seeking a {seniority_adj}Data Scientist to join our team and help transform data into actionable insights. You will apply statistical analysis, machine learning, and data visualization techniques to solve complex business problems and drive data-informed decision making.""",
        'responsibilities': [
            "Develop and deploy machine learning models to solve business problems and improve outcomes",
            "Conduct exploratory data analysis to identify patterns, trends, and opportunities",
            "Build data pipelines and create automated reporting and visualization dashboards",
            "Collaborate with stakeholders to understand business needs and translate them into data science projects",
            "Design and run experiments (A/B tests) to evaluate the impact of changes and new features",
            "Communicate findings to technical and non-technical audiences through presentations and reports"
        ],
        'qualifications': [
            "Strong foundation in statistics, machine learning, and predictive modeling techniques",
            "Proficiency in Python or R, with experience using libraries like pandas, scikit-learn, TensorFlow, or PyTorch",
            "Experience with SQL and data manipulation in large-scale data environments",
            "Familiarity with data visualization tools (Tableau, Power BI, matplotlib, seaborn)",
            "Excellent analytical and problem-solving skills with attention to detail",
            "Strong communication skills and ability to explain complex concepts to diverse audiences"
        ],
        'closing': """Join our team to work on impactful data science projects that drive real business value. We offer a collaborative environment where you can grow your skills, work with cutting-edge tools, and make meaningful contributions to our organization's success."""
    },

    'data_analyst': {
        'overview': """We are looking for a {seniority_adj}Data Analyst to help us make sense of our data and provide insights that drive business decisions. You will analyze datasets, create visualizations, build reports, and partner with stakeholders to answer critical business questions.""",
        'responsibilities': [
            "Analyze large datasets to identify trends, patterns, and actionable insights",
            "Create dashboards and reports that communicate key metrics to stakeholders",
            "Develop and maintain SQL queries and data models to support reporting needs",
            "Partner with business teams to understand their data needs and deliver relevant analyses",
            "Ensure data quality and accuracy through validation and documentation processes",
            "Present findings and recommendations to leadership and cross-functional teams"
        ],
        'qualifications': [
            "Strong proficiency in SQL and experience querying relational databases",
            "Experience with data visualization tools such as Tableau, Power BI, or Looker",
            "Proficiency in Excel and familiarity with Python or R for data analysis",
            "Strong analytical and critical thinking skills with attention to detail",
            "Ability to translate business questions into data analysis and communicate findings clearly",
            "Experience working with stakeholders to understand requirements and deliver insights"
        ],
        'closing': """This role offers the opportunity to work with diverse datasets and directly impact business decisions. We value curiosity, analytical rigor, and clear communication, and we're committed to supporting your professional growth."""
    },

    'product_manager': {
        'overview': """We are seeking a {seniority_adj}Product Manager to drive the strategy and execution of our product roadmap. You will work at the intersection of business, technology, and user experience to define product vision, prioritize features, and deliver solutions that customers love.""",
        'responsibilities': [
            "Define product strategy and roadmap aligned with company goals and customer needs",
            "Gather and prioritize product requirements through customer research, data analysis, and stakeholder input",
            "Write clear product specifications, user stories, and acceptance criteria for engineering teams",
            "Collaborate with engineering, design, and marketing to deliver high-quality product releases",
            "Analyze product metrics and user feedback to inform decisions and measure success",
            "Communicate product plans, progress, and results to stakeholders at all levels"
        ],
        'qualifications': [
            "Proven experience in product management with a track record of shipping successful products",
            "Strong analytical skills and ability to use data to drive product decisions",
            "Excellent communication and stakeholder management skills",
            "Experience with agile development methodologies and product management tools",
            "Deep empathy for users and ability to translate customer needs into product features",
            "Technical aptitude and ability to work effectively with engineering teams"
        ],
        'closing': """Join us to shape products that make a difference. We offer a collaborative environment where product managers have real ownership and impact, competitive compensation, and opportunities for growth."""
    },

    'ux_designer': {
        'overview': """We are looking for a {seniority_adj}UX Designer to create intuitive, user-centered designs that solve real problems. You will conduct user research, create wireframes and prototypes, and collaborate with product and engineering teams to deliver exceptional user experiences.""",
        'responsibilities': [
            "Conduct user research including interviews, surveys, and usability testing to understand user needs",
            "Create wireframes, prototypes, and high-fidelity designs that effectively communicate design concepts",
            "Develop and maintain design systems, style guides, and component libraries",
            "Collaborate with product managers and engineers to implement designs and ensure quality",
            "Analyze user feedback and metrics to iterate on designs and improve user experience",
            "Advocate for users throughout the product development process"
        ],
        'qualifications': [
            "Strong portfolio demonstrating user-centered design process and problem-solving skills",
            "Proficiency in design tools such as Figma, Sketch, or Adobe Creative Suite",
            "Experience with user research methods and usability testing",
            "Understanding of interaction design, information architecture, and visual design principles",
            "Excellent communication skills and ability to present and defend design decisions",
            "Ability to work collaboratively in a fast-paced, iterative environment"
        ],
        'closing': """Join our design team to create products that users love. We value design thinking, user empathy, and continuous improvement, and we're committed to supporting your creative and professional growth."""
    },

    'accountant': {
        'overview': """We are seeking a {seniority_adj}Accountant to join our finance team and help maintain accurate financial records. You will handle day-to-day accounting activities, prepare financial statements, ensure compliance with regulations, and support the organization's financial health.""",
        'responsibilities': [
            "Prepare and post journal entries, reconcile accounts, and maintain accurate financial records",
            "Assist with month-end and year-end close processes, including financial statement preparation",
            "Perform account reconciliations and resolve discrepancies in a timely manner",
            "Prepare financial reports and analyses for management and stakeholders",
            "Support audit processes by preparing documentation and responding to auditor requests",
            "Ensure compliance with GAAP, company policies, and regulatory requirements"
        ],
        'qualifications': [
            "Bachelor's degree in Accounting, Finance, or related field",
            "CPA certification preferred or in progress",
            "Strong knowledge of GAAP and accounting principles",
            "Proficiency in accounting software (e.g., QuickBooks, SAP, Oracle) and Microsoft Excel",
            "Excellent attention to detail and organizational skills",
            "Strong analytical and problem-solving abilities"
        ],
        'closing': """Join our finance team and contribute to the financial integrity of our organization. We offer competitive compensation, professional development opportunities, and a collaborative work environment."""
    },

    'nurse': {
        'overview': """We are seeking a compassionate and skilled {specialty_adj}Registered Nurse to provide high-quality patient care. You will assess patient conditions, develop care plans, administer treatments, and collaborate with healthcare teams to ensure optimal patient outcomes.""",
        'responsibilities': [
            "Assess patient conditions through physical examinations, health histories, and diagnostic tests",
            "Develop and implement individualized care plans in collaboration with physicians and care teams",
            "Administer medications, treatments, and procedures according to physician orders and protocols",
            "Monitor patient progress, document observations, and communicate changes to the care team",
            "Educate patients and families about health conditions, treatments, and self-care practices",
            "Maintain compliance with healthcare regulations, safety standards, and infection control protocols"
        ],
        'qualifications': [
            "Current RN license in the state of practice",
            "BLS certification required; ACLS and specialty certifications as applicable",
            "Strong clinical assessment and critical thinking skills",
            "Excellent communication and interpersonal skills",
            "Ability to work effectively in a fast-paced healthcare environment",
            "Compassionate approach to patient care with strong attention to detail"
        ],
        'closing': """Join our healthcare team and make a meaningful difference in patients' lives. We offer competitive compensation, comprehensive benefits, and opportunities for professional growth and continuing education."""
    },

    'physician': {
        'overview': """We are seeking a {specialty_adj}Physician to join our medical team and provide exceptional patient care. You will diagnose and treat patients, develop treatment plans, and collaborate with healthcare professionals to deliver comprehensive, evidence-based medical care.""",
        'responsibilities': [
            "Examine patients, obtain medical histories, and order diagnostic tests to diagnose conditions",
            "Develop and implement treatment plans, prescribe medications, and perform procedures as needed",
            "Coordinate patient care with specialists, nurses, and other healthcare providers",
            "Document patient encounters, treatment plans, and outcomes in electronic health records",
            "Participate in quality improvement initiatives and evidence-based practice development",
            "Educate patients on health maintenance, disease prevention, and treatment options"
        ],
        'qualifications': [
            "MD or DO degree from an accredited medical school",
            "Board certification or eligibility in the relevant specialty",
            "Active, unrestricted medical license in the state of practice",
            "DEA registration and ability to prescribe controlled substances",
            "Excellent clinical skills, diagnostic abilities, and patient communication",
            "Commitment to evidence-based medicine and continuous learning"
        ],
        'closing': """Join our medical team and practice medicine in a supportive environment that values quality care and physician well-being. We offer competitive compensation, comprehensive benefits, and a commitment to work-life balance."""
    },

    'it_support': {
        'overview': """We are looking for an {seniority_adj}IT Support Technician to provide technical assistance and ensure smooth operation of our technology infrastructure. You will troubleshoot hardware and software issues, support end users, and help maintain our IT systems.""",
        'responsibilities': [
            "Provide first-line technical support to employees via phone, email, chat, and in-person",
            "Troubleshoot and resolve hardware, software, network, and connectivity issues",
            "Set up and configure computers, mobile devices, printers, and other equipment",
            "Install, update, and maintain software applications and operating systems",
            "Document support requests, resolutions, and procedures in the ticketing system",
            "Assist with onboarding new employees and training users on technology tools"
        ],
        'qualifications': [
            "Experience in IT support, help desk, or technical support role",
            "Strong knowledge of Windows and/or macOS operating systems",
            "Familiarity with networking concepts, Active Directory, and common business applications",
            "Excellent troubleshooting and problem-solving skills",
            "Strong communication skills and customer service orientation",
            "IT certifications (CompTIA A+, Network+, etc.) preferred"
        ],
        'closing': """Join our IT team and help keep our organization running smoothly. We offer competitive compensation, professional development opportunities, and a collaborative environment where your contributions are valued."""
    },

    'project_manager': {
        'overview': """We are seeking a {seniority_adj}Project Manager to lead cross-functional initiatives from conception to completion. You will define project scope, develop plans, manage resources, and ensure successful delivery of projects on time and within budget.""",
        'responsibilities': [
            "Define project scope, objectives, deliverables, and success criteria with stakeholders",
            "Develop comprehensive project plans including timelines, milestones, and resource allocation",
            "Lead project teams, facilitate meetings, and drive collaboration across functions",
            "Track project progress, identify risks, and implement mitigation strategies",
            "Manage project budgets, timelines, and stakeholder expectations",
            "Communicate project status, issues, and outcomes to leadership and stakeholders"
        ],
        'qualifications': [
            "Proven experience managing complex projects from initiation to completion",
            "Strong knowledge of project management methodologies (Agile, Waterfall, hybrid)",
            "PMP, Scrum Master, or other relevant certifications preferred",
            "Excellent organizational, leadership, and communication skills",
            "Proficiency in project management tools (Jira, Asana, MS Project, etc.)",
            "Ability to manage multiple priorities and work effectively under pressure"
        ],
        'closing': """Join our team to lead impactful projects that drive business results. We offer competitive compensation, professional development, and a collaborative environment where project managers can grow their careers."""
    },

    'sales': {
        'overview': """We are looking for a motivated {seniority_adj}Sales Representative to join our team and drive revenue growth. You will identify and develop new business opportunities, build relationships with prospects and customers, and consistently achieve sales targets.""",
        'responsibilities': [
            "Prospect and qualify leads through outbound outreach and inbound inquiry follow-up",
            "Conduct discovery calls and product demonstrations to understand customer needs",
            "Develop and present proposals, negotiate terms, and close deals",
            "Build and maintain strong relationships with customers throughout the sales cycle",
            "Manage your pipeline in CRM, accurately forecasting deals and maintaining activity metrics",
            "Collaborate with marketing, product, and customer success teams to ensure customer satisfaction"
        ],
        'qualifications': [
            "Proven track record of meeting or exceeding sales targets",
            "Strong prospecting, presentation, and negotiation skills",
            "Experience with CRM systems (Salesforce, HubSpot, etc.)",
            "Excellent communication and interpersonal skills",
            "Self-motivated with strong work ethic and competitive drive",
            "Ability to understand customer needs and articulate value propositions"
        ],
        'closing': """Join our sales team and be rewarded for your success. We offer competitive base salary plus uncapped commission, comprehensive benefits, and a supportive environment where top performers thrive."""
    },

    'marketing': {
        'overview': """We are seeking a {seniority_adj}Marketing {specialty_adj}professional to help grow our brand and drive customer acquisition. You will develop and execute marketing campaigns, create compelling content, and analyze performance to optimize our marketing efforts.""",
        'responsibilities': [
            "Develop and execute marketing campaigns across multiple channels (digital, content, email, events)",
            "Create compelling content including blog posts, case studies, social media, and sales collateral",
            "Manage marketing automation, email campaigns, and lead nurturing programs",
            "Analyze campaign performance, track KPIs, and optimize based on data and insights",
            "Collaborate with sales, product, and design teams to align marketing efforts with business goals",
            "Stay current on marketing trends, tools, and best practices"
        ],
        'qualifications': [
            "Experience in B2B or B2C marketing with demonstrated results",
            "Strong writing and content creation skills",
            "Proficiency in marketing automation and analytics tools",
            "Understanding of digital marketing channels including SEO, SEM, social media, and email",
            "Analytical mindset with ability to derive insights from data",
            "Excellent project management and communication skills"
        ],
        'closing': """Join our marketing team and help shape our brand story. We offer competitive compensation, creative freedom, and opportunities to grow your marketing career."""
    },

    'customer_service': {
        'overview': """We are looking for a {seniority_adj}Customer Service Representative to provide exceptional support to our customers. You will handle inquiries, resolve issues, and ensure customers have a positive experience with our products and services.""",
        'responsibilities': [
            "Respond to customer inquiries via phone, email, chat, and social media in a timely manner",
            "Troubleshoot customer issues, identify solutions, and escalate complex problems as needed",
            "Process orders, returns, refunds, and account changes accurately",
            "Document customer interactions and feedback in the CRM system",
            "Identify opportunities to improve customer experience and share feedback with the team",
            "Meet or exceed performance metrics including response time, resolution rate, and customer satisfaction"
        ],
        'qualifications': [
            "Experience in customer service, support, or client-facing role",
            "Excellent verbal and written communication skills",
            "Strong problem-solving abilities and attention to detail",
            "Proficiency with CRM and support ticketing systems",
            "Ability to remain calm and professional in challenging situations",
            "Customer-focused mindset with genuine desire to help others"
        ],
        'closing': """Join our customer service team and make a difference in customers' experiences every day. We offer competitive pay, comprehensive benefits, and opportunities for career advancement."""
    },

    'hr': {
        'overview': """We are seeking a {seniority_adj}Human Resources professional to support our people operations and help create a great workplace. You will handle HR processes, support employees throughout their lifecycle, and contribute to HR programs and initiatives.""",
        'responsibilities': [
            "Support the full employee lifecycle including onboarding, offboarding, and employee relations",
            "Administer HR programs including benefits, leaves, performance management, and compliance",
            "Maintain accurate employee records and HRIS data",
            "Assist with recruiting activities including job postings, screening, and interview coordination",
            "Respond to employee inquiries about policies, benefits, and procedures",
            "Support HR projects and initiatives as needed"
        ],
        'qualifications': [
            "Experience in human resources or people operations",
            "Knowledge of HR practices, employment law, and compliance requirements",
            "Proficiency in HRIS systems and Microsoft Office",
            "Excellent interpersonal and communication skills",
            "Strong attention to detail and organizational abilities",
            "Ability to handle confidential information with discretion"
        ],
        'closing': """Join our HR team and help us build a great workplace. We offer competitive compensation, comprehensive benefits, and a collaborative environment where you can grow your HR career."""
    },

    'contract_specialist': {
        'overview': """We are seeking a Contract Specialist to manage the full lifecycle of contracts and ensure compliance with applicable regulations. You will draft, negotiate, and administer contracts while supporting organizational procurement and contracting needs.""",
        'responsibilities': [
            "Prepare solicitation documents, evaluate proposals, and conduct contract negotiations",
            "Draft, review, and administer contracts including modifications and closeouts",
            "Ensure compliance with federal acquisition regulations (FAR) and organizational policies",
            "Serve as primary point of contact between the organization and contractors",
            "Monitor contractor performance and resolve contract-related issues",
            "Maintain accurate contract files and documentation"
        ],
        'qualifications': [
            "Bachelor's degree in Business, Contract Management, or related field",
            "Experience in government contracting or procurement",
            "Knowledge of Federal Acquisition Regulation (FAR) and contracting procedures",
            "Strong negotiation, analytical, and communication skills",
            "Attention to detail and ability to manage multiple contracts simultaneously",
            "Relevant certifications (CFCM, CPCM, etc.) preferred"
        ],
        'closing': """Join our contracting team and play a key role in supporting organizational missions through effective contract management. We offer competitive compensation and opportunities for professional development."""
    },

    'estimator': {
        'overview': """We are seeking an {seniority_adj}Estimator to prepare accurate cost estimates for construction projects. You will analyze project requirements, calculate material and labor costs, and develop comprehensive bids that balance competitiveness with profitability.""",
        'responsibilities': [
            "Review project plans, specifications, and documents to understand scope and requirements",
            "Perform quantity takeoffs and calculate material, labor, and equipment costs",
            "Solicit and evaluate subcontractor and vendor bids",
            "Develop comprehensive cost estimates and bid proposals",
            "Coordinate with project managers, architects, and clients during the bidding process",
            "Analyze historical data and market conditions to improve estimate accuracy"
        ],
        'qualifications': [
            "Experience in construction estimating with knowledge of building methods and materials",
            "Proficiency in estimating software (e.g., Bluebeam, PlanSwift, Sage) and Microsoft Excel",
            "Strong analytical skills and attention to detail",
            "Ability to read and interpret construction drawings and specifications",
            "Excellent communication and negotiation skills",
            "Degree in Construction Management, Engineering, or related field preferred"
        ],
        'closing': """Join our preconstruction team and help win projects that build our portfolio. We offer competitive compensation, professional development, and opportunities to grow within a respected construction firm."""
    },

    'regulatory_affairs': {
        'overview': """We are seeking a {seniority_adj}Regulatory Affairs professional to ensure our products and processes comply with applicable regulations. You will navigate complex regulatory landscapes, prepare submissions, and serve as a liaison with regulatory agencies.""",
        'responsibilities': [
            "Prepare and submit regulatory filings, registrations, and applications to regulatory agencies",
            "Monitor regulatory changes and assess their impact on products and operations",
            "Provide regulatory guidance and support to product development and manufacturing teams",
            "Maintain regulatory documentation, product registrations, and compliance records",
            "Communicate with regulatory agencies and respond to inquiries and inspections",
            "Support audits and ensure ongoing compliance with regulatory requirements"
        ],
        'qualifications': [
            "Bachelor's degree in Life Sciences, Pharmacy, Engineering, or related field",
            "Experience in regulatory affairs with knowledge of relevant regulations (FDA, EMA, etc.)",
            "Strong attention to detail and ability to interpret complex regulatory requirements",
            "Excellent written and verbal communication skills",
            "Ability to manage multiple projects and meet deadlines",
            "RAC certification preferred"
        ],
        'closing': """Join our regulatory team and help bring safe, compliant products to market. We offer competitive compensation, professional development, and the opportunity to impact product development and patient safety."""
    },

    'executive': {
        'overview': """We are seeking a strategic leader to drive organizational success and lead high-performing teams. In this executive role, you will set vision and strategy, build organizational capability, and deliver results that advance our mission.""",
        'responsibilities': [
            "Develop and execute strategic plans aligned with organizational goals and market opportunities",
            "Build, lead, and develop high-performing teams across the organization",
            "Drive operational excellence and continuous improvement initiatives",
            "Manage budgets, resources, and key performance metrics",
            "Represent the organization to external stakeholders, partners, and customers",
            "Collaborate with executive leadership to shape organizational direction"
        ],
        'qualifications': [
            "Proven executive leadership experience with track record of driving results",
            "Strong strategic thinking and business acumen",
            "Excellent leadership, communication, and stakeholder management skills",
            "Experience building and developing high-performing teams",
            "Ability to navigate complexity and drive change in dynamic environments",
            "Advanced degree (MBA, etc.) preferred"
        ],
        'closing': """Join our leadership team and make a significant impact on our organization's future. We offer competitive executive compensation, equity participation, and the opportunity to shape our strategic direction."""
    },

    'manager': {
        'overview': """We are seeking a {seniority_adj}Manager to lead a team and drive results within our organization. You will set priorities, develop team members, manage performance, and ensure your team delivers on its objectives while contributing to broader organizational goals.""",
        'responsibilities': [
            "Lead, coach, and develop team members to achieve individual and team goals",
            "Set clear priorities, delegate effectively, and manage team workload and capacity",
            "Monitor performance, provide feedback, and conduct performance reviews",
            "Drive process improvements and implement best practices within your function",
            "Collaborate with cross-functional partners to achieve organizational objectives",
            "Communicate team progress, challenges, and needs to leadership"
        ],
        'qualifications': [
            "Proven experience in a management or leadership role",
            "Strong people management, coaching, and development skills",
            "Excellent communication and interpersonal abilities",
            "Ability to prioritize, delegate, and drive results through others",
            "Problem-solving skills and ability to make decisions under pressure",
            "Relevant functional expertise in your area of management"
        ],
        'closing': """Join our team as a leader who develops people and drives results. We offer competitive compensation, leadership development opportunities, and a collaborative environment where managers can grow their careers."""
    },

    'general_engineer': {
        'overview': """We are seeking a {seniority_adj}Engineer to apply technical expertise to solve complex problems and deliver innovative solutions. You will design, analyze, and implement engineering solutions while collaborating with cross-functional teams.""",
        'responsibilities': [
            "Design, develop, and implement engineering solutions to meet project requirements",
            "Conduct technical analysis, calculations, and simulations to validate designs",
            "Prepare technical documentation, specifications, and reports",
            "Collaborate with cross-functional teams including other engineers, scientists, and stakeholders",
            "Ensure compliance with relevant codes, standards, and regulations",
            "Support testing, troubleshooting, and continuous improvement of systems and processes"
        ],
        'qualifications': [
            "Bachelor's degree in Engineering or related technical field",
            "Strong technical skills and engineering fundamentals",
            "Proficiency in relevant engineering software and tools",
            "Excellent analytical and problem-solving abilities",
            "Strong communication skills and ability to work in teams",
            "PE license or EIT certification preferred"
        ],
        'closing': """Join our engineering team and work on challenging projects that make a difference. We offer competitive compensation, professional development, and opportunities to grow your engineering career."""
    },

    'general_analyst': {
        'overview': """We are seeking a {seniority_adj}Analyst to gather, analyze, and interpret information to support business decisions. You will conduct research, prepare reports, and provide recommendations that help the organization achieve its objectives.""",
        'responsibilities': [
            "Gather and analyze data from various sources to support business decisions",
            "Prepare reports, presentations, and documentation for stakeholders",
            "Identify trends, patterns, and opportunities through systematic analysis",
            "Collaborate with team members and stakeholders to understand requirements",
            "Maintain accurate records and ensure data quality",
            "Support process improvements and special projects as needed"
        ],
        'qualifications': [
            "Bachelor's degree in relevant field",
            "Strong analytical and critical thinking skills",
            "Proficiency in Microsoft Excel and data analysis tools",
            "Excellent attention to detail and organizational abilities",
            "Strong written and verbal communication skills",
            "Ability to manage multiple priorities and meet deadlines"
        ],
        'closing': """Join our team as an analyst and contribute to informed decision-making. We offer competitive compensation, professional development, and opportunities for career growth."""
    },

    'specialist': {
        'overview': """We are seeking a Specialist to apply your expertise in supporting organizational goals and solving complex challenges. You will serve as a subject matter expert, provide guidance, and deliver high-quality work in your area of specialization.""",
        'responsibilities': [
            "Apply specialized knowledge and expertise to support organizational objectives",
            "Analyze situations, develop recommendations, and implement solutions",
            "Serve as a resource and subject matter expert for your functional area",
            "Prepare documentation, reports, and presentations as needed",
            "Collaborate with team members and stakeholders to achieve goals",
            "Stay current on best practices and developments in your specialty"
        ],
        'qualifications': [
            "Relevant experience and expertise in the specialized area",
            "Strong analytical and problem-solving skills",
            "Excellent attention to detail and quality focus",
            "Strong communication and interpersonal skills",
            "Ability to work independently and as part of a team",
            "Relevant certifications or credentials preferred"
        ],
        'closing': """Join our team and apply your specialized expertise to meaningful work. We offer competitive compensation, professional development, and opportunities to deepen your specialization."""
    },

    'general': {
        'overview': """We are seeking a qualified professional to join our team and contribute to organizational success. In this role, you will apply your skills and expertise to support team objectives while growing professionally.""",
        'responsibilities': [
            "Perform assigned duties and responsibilities to support team and organizational goals",
            "Collaborate effectively with colleagues and stakeholders",
            "Maintain high quality standards in all work products",
            "Communicate progress, challenges, and ideas to supervisors and team members",
            "Participate in continuous improvement and professional development activities",
            "Contribute positively to team culture and organizational success"
        ],
        'qualifications': [
            "Relevant experience or education for the role",
            "Strong work ethic and commitment to quality",
            "Excellent communication and interpersonal skills",
            "Ability to learn quickly and adapt to new situations",
            "Strong organizational skills and attention to detail",
            "Team player with positive attitude"
        ],
        'closing': """Join our team and contribute to our continued success. We offer competitive compensation, comprehensive benefits, and opportunities for professional growth and development."""
    },
}

# Add more specialized descriptions
DESCRIPTIONS.update({
    'biomedical_engineer': {
        'overview': """We are seeking a {seniority_adj}Biomedical/Biopharma Engineer to support pharmaceutical manufacturing and process development. You will work on upstream and downstream bioprocesses, equipment qualification, and process optimization to ensure reliable production of life-saving therapies.""",
        'responsibilities': [
            "Support upstream and downstream bioprocessing operations including cell culture, fermentation, purification, and formulation",
            "Lead equipment qualification, validation, and troubleshooting activities",
            "Develop and optimize manufacturing processes to improve yield, quality, and efficiency",
            "Author and review technical documents including protocols, reports, SOPs, and batch records",
            "Investigate deviations, support root cause analysis, and implement corrective actions",
            "Collaborate with cross-functional teams including Manufacturing, Quality, and R&D"
        ],
        'qualifications': [
            "Bachelor's or Master's degree in Biomedical Engineering, Biochemical Engineering, Chemical Engineering, or related field",
            "Experience in biopharmaceutical manufacturing or process development",
            "Knowledge of cGMP regulations and pharmaceutical manufacturing practices",
            "Familiarity with bioprocessing equipment including bioreactors, chromatography systems, and filtration",
            "Strong analytical, troubleshooting, and problem-solving skills",
            "Excellent documentation and communication abilities"
        ],
        'closing': """Join our team and contribute to the development and manufacturing of innovative therapies. We offer competitive compensation, comprehensive benefits, and the opportunity to make a real impact on patient lives."""
    },

    'curator': {
        'overview': """We are seeking a Curatorial professional to contribute to the research, interpretation, and presentation of collections. You will conduct scholarly research, develop exhibitions, and engage with academic and public audiences to advance the institution's mission.""",
        'responsibilities': [
            "Conduct original research on collection objects and contribute to scholarly publications",
            "Develop and curate exhibitions, including concept development, object selection, and interpretation",
            "Acquire new works and build collections in alignment with institutional priorities",
            "Write exhibition texts, catalog entries, and educational materials",
            "Collaborate with colleagues on cross-departmental projects and programs",
            "Engage with scholars, artists, collectors, and the public to advance the institution's visibility"
        ],
        'qualifications': [
            "Advanced degree (MA or PhD) in Art History, Museum Studies, or related field",
            "Demonstrated expertise in relevant collection area with scholarly publications",
            "Experience with exhibition development and collection management",
            "Strong research, writing, and presentation skills",
            "Excellent interpersonal skills and ability to work collaboratively",
            "Commitment to public engagement and education"
        ],
        'closing': """Join our curatorial team and contribute to meaningful scholarship and public engagement. We offer competitive compensation, professional development, and the opportunity to work with exceptional collections."""
    },

    'research_fellow': {
        'overview': """We are seeking a Research Fellow to conduct innovative research and contribute to our scholarly mission. You will lead research projects, publish findings, and collaborate with colleagues to advance knowledge in your field.""",
        'responsibilities': [
            "Design and conduct original research projects aligned with institutional priorities",
            "Analyze data, interpret results, and publish findings in peer-reviewed journals",
            "Present research at conferences, seminars, and public programs",
            "Collaborate with colleagues, students, and external partners on research initiatives",
            "Contribute to grant writing and funding proposals",
            "Participate in the intellectual life of the institution through teaching, mentoring, and service"
        ],
        'qualifications': [
            "PhD or equivalent terminal degree in relevant field",
            "Demonstrated research excellence with publication record",
            "Strong analytical, writing, and presentation skills",
            "Experience with relevant research methodologies and tools",
            "Ability to work independently and collaboratively",
            "Commitment to scholarly integrity and public engagement"
        ],
        'closing': """Join our research community and advance your scholarly career. We offer competitive stipend, research resources, and an intellectually vibrant environment to support your work."""
    },

    'physical_therapist': {
        'overview': """We are seeking a Physical Therapist to provide expert rehabilitation services to patients. You will evaluate patients, develop treatment plans, implement therapeutic interventions, and help patients achieve their functional goals and improve quality of life.""",
        'responsibilities': [
            "Conduct comprehensive evaluations of patients' physical function and mobility",
            "Develop individualized treatment plans based on assessment findings and patient goals",
            "Implement therapeutic interventions including exercise, manual therapy, and modalities",
            "Educate patients and families on exercises, injury prevention, and self-management",
            "Document evaluations, progress notes, and outcomes in the medical record",
            "Collaborate with physicians, other therapists, and healthcare team members"
        ],
        'qualifications': [
            "Doctor of Physical Therapy (DPT) degree from an accredited program",
            "Current state licensure as a Physical Therapist",
            "Strong clinical reasoning and manual therapy skills",
            "Excellent communication and patient education abilities",
            "CPR certification required",
            "Specialty certification (OCS, NCS, etc.) preferred"
        ],
        'closing': """Join our rehabilitation team and help patients achieve their recovery goals. We offer competitive compensation, continuing education support, and a collaborative practice environment."""
    },

    'devops': {
        'overview': """We are seeking a {seniority_adj}DevOps/Platform Engineer to build and maintain our infrastructure and deployment pipelines. You will automate processes, improve system reliability, and enable engineering teams to deploy software quickly and safely.""",
        'responsibilities': [
            "Design, implement, and maintain CI/CD pipelines for automated build, test, and deployment",
            "Manage cloud infrastructure (AWS, GCP, Azure) using infrastructure-as-code tools",
            "Monitor system performance, implement alerting, and respond to incidents",
            "Improve system reliability, scalability, and security through automation and best practices",
            "Collaborate with development teams to improve deployment processes and developer experience",
            "Document infrastructure, processes, and runbooks for operational excellence"
        ],
        'qualifications': [
            "Experience with cloud platforms (AWS, GCP, Azure) and infrastructure-as-code (Terraform, CloudFormation)",
            "Strong proficiency in scripting languages (Python, Bash) and automation tools",
            "Experience with containerization (Docker, Kubernetes) and orchestration",
            "Knowledge of CI/CD tools (Jenkins, GitHub Actions, GitLab CI)",
            "Understanding of networking, security, and monitoring best practices",
            "Strong troubleshooting skills and ability to work in on-call rotation"
        ],
        'closing': """Join our platform team and help build reliable, scalable infrastructure. We offer competitive compensation, learning opportunities, and the chance to work on challenging technical problems."""
    },

    'data_engineer': {
        'overview': """We are seeking a {seniority_adj}Data Engineer to build and maintain data infrastructure that powers analytics and machine learning. You will design data pipelines, optimize data warehouses, and ensure data quality and availability for the organization.""",
        'responsibilities': [
            "Design and implement scalable data pipelines for ingestion, transformation, and storage",
            "Build and maintain data warehouses and data lakes using modern data stack tools",
            "Optimize query performance and data model design for analytics workloads",
            "Ensure data quality through validation, monitoring, and documentation",
            "Collaborate with data scientists and analysts to understand data requirements",
            "Implement data governance practices including security, privacy, and compliance"
        ],
        'qualifications': [
            "Strong proficiency in SQL and experience with data warehouses (Snowflake, BigQuery, Redshift)",
            "Experience with data pipeline tools (Airflow, dbt, Spark) and ETL/ELT processes",
            "Proficiency in Python or other programming languages for data processing",
            "Understanding of data modeling, dimensional modeling, and data architecture",
            "Experience with cloud platforms and data infrastructure",
            "Strong problem-solving skills and attention to data quality"
        ],
        'closing': """Join our data team and build the foundation for data-driven decision making. We offer competitive compensation, growth opportunities, and the chance to work with modern data technologies."""
    },

    'financial_analyst': {
        'overview': """We are seeking a {seniority_adj}Financial Analyst to support financial planning, analysis, and reporting. You will build financial models, analyze performance, and provide insights that drive business decisions and strategic planning.""",
        'responsibilities': [
            "Develop and maintain financial models for forecasting, budgeting, and scenario analysis",
            "Analyze financial performance, identify trends, and provide actionable insights",
            "Prepare financial reports, presentations, and dashboards for leadership",
            "Support the annual budgeting process and monthly/quarterly forecasting",
            "Conduct ad-hoc analysis to support business decisions and strategic initiatives",
            "Collaborate with business partners to understand drivers and improve financial performance"
        ],
        'qualifications': [
            "Bachelor's degree in Finance, Accounting, Economics, or related field",
            "Strong financial modeling and analytical skills",
            "Advanced proficiency in Excel and experience with financial systems",
            "Excellent attention to detail and accuracy",
            "Strong communication skills and ability to present to leadership",
            "CFA, CPA, or MBA preferred"
        ],
        'closing': """Join our finance team and provide insights that drive business success. We offer competitive compensation, professional development, and opportunities for career advancement."""
    },

    'compliance': {
        'overview': """We are seeking a {seniority_adj}Compliance professional to ensure organizational adherence to laws, regulations, and internal policies. You will develop compliance programs, conduct assessments, and provide guidance to mitigate regulatory risk.""",
        'responsibilities': [
            "Develop and implement compliance policies, procedures, and training programs",
            "Monitor regulatory changes and assess impact on the organization",
            "Conduct compliance assessments, audits, and investigations",
            "Provide compliance guidance and support to business units",
            "Manage regulatory filings, reporting, and communications with regulators",
            "Track and report on compliance metrics and program effectiveness"
        ],
        'qualifications': [
            "Bachelor's degree in Law, Business, or related field",
            "Experience in compliance, legal, or regulatory role",
            "Knowledge of relevant regulations and compliance frameworks",
            "Strong analytical and investigative skills",
            "Excellent written and verbal communication",
            "Compliance certification (CCEP, CRCM, etc.) preferred"
        ],
        'closing': """Join our compliance team and help ensure we operate with integrity and meet our regulatory obligations. We offer competitive compensation and the opportunity to protect the organization from risk."""
    },

    'correctional_officer': {
        'overview': """We are seeking Correctional Officers to maintain safety and security within our correctional facility. You will supervise inmates, enforce rules and regulations, respond to emergencies, and contribute to rehabilitation programs while ensuring the safety of staff and the public.""",
        'responsibilities': [
            "Supervise inmates during daily activities including meals, recreation, and work assignments",
            "Conduct security rounds, inmate counts, and facility inspections",
            "Search inmates, cells, and common areas for contraband",
            "Respond to emergencies, disturbances, and security threats following established protocols",
            "Document incidents, prepare reports, and maintain accurate records",
            "Escort inmates to medical appointments, court proceedings, and transfers"
        ],
        'qualifications': [
            "High school diploma or GED; some college preferred",
            "Ability to pass background investigation, drug screening, and medical examination",
            "Physical fitness to perform essential duties including responding to emergencies",
            "Strong situational awareness and ability to remain calm under pressure",
            "Excellent observation, communication, and interpersonal skills",
            "Ability to work rotating shifts including nights, weekends, and holidays"
        ],
        'closing': """Join our correctional team and contribute to public safety. We offer competitive salary, comprehensive benefits including retirement, paid training, and opportunities for career advancement within the corrections system."""
    },

    'consultant': {
        'overview': """We are seeking a {seniority_adj}Consultant to deliver expert advisory services to our clients. You will analyze business challenges, develop recommendations, and work with client teams to implement solutions that drive measurable results.""",
        'responsibilities': [
            "Engage with clients to understand their business challenges and objectives",
            "Conduct research, analysis, and diagnostic assessments",
            "Develop recommendations and present findings to client leadership",
            "Support implementation of solutions and change management initiatives",
            "Build and maintain strong client relationships",
            "Contribute to proposal development and business development activities"
        ],
        'qualifications': [
            "Bachelor's degree required; MBA or advanced degree preferred",
            "Relevant consulting or industry experience",
            "Strong analytical, problem-solving, and strategic thinking skills",
            "Excellent communication and presentation abilities",
            "Ability to work effectively with clients and team members",
            "Willingness to travel as required"
        ],
        'closing': """Join our consulting team and make an impact with leading organizations. We offer competitive compensation, professional development, and the opportunity to work on challenging and rewarding engagements."""
    },
})


def build_description(title, company, location, salary_range, role_info):
    """Build a structured description from the role info."""

    category = role_info.get('category', 'general')
    seniority = role_info.get('seniority', 'mid')
    specialty = role_info.get('specialty', '')

    # Get the description template, fall back to 'general' if not found
    template = DESCRIPTIONS.get(category, DESCRIPTIONS['general'])

    # Build seniority adjective
    seniority_adj = ''
    if seniority == 'senior':
        seniority_adj = 'Senior '
    elif seniority == 'entry':
        seniority_adj = 'Junior '
    elif seniority == 'intern':
        seniority_adj = 'Intern '

    # Build specialty adjective
    specialty_adj = ''
    if specialty:
        specialty_map = {
            'backend': 'Backend ',
            'frontend': 'Frontend ',
            'full stack': 'Full Stack ',
            'digital': 'Digital ',
            'content': 'Content ',
            'product': 'Product ',
            'primary_care': 'Primary Care ',
            'cardiology': 'Cardiology ',
            'neurology': 'Neurology ',
            'oncology': 'Oncology ',
            'pediatrics': 'Pediatric ',
            'psychiatry': 'Psychiatry ',
            'emergency': 'Emergency ',
            'surgery': 'Surgery ',
            'icu': 'ICU ',
            'mental_health': 'Mental Health ',
            'surgical': 'Surgical ',
            'biology': 'Biology ',
            'chemistry': 'Chemistry ',
            'physics': 'Physics ',
        }
        specialty_adj = specialty_map.get(specialty, '')

    # Build the description
    overview = template['overview'].format(
        seniority_adj=seniority_adj,
        specialty_adj=specialty_adj
    )

    # Build responsibilities section
    responsibilities = "\n\n**Key Responsibilities:**\n"
    for resp in template['responsibilities']:
        responsibilities += f"• {resp}\n"

    # Build qualifications section
    qualifications = "\n**Qualifications:**\n"
    for qual in template['qualifications']:
        qualifications += f"• {qual}\n"

    # Add company-specific context
    company_context = ""
    if company:
        company_lower = company.lower()
        if 'veterans health' in company_lower or 'va ' in company_lower:
            company_context = f"\n\n**About {company}:**\nThe Veterans Health Administration is the largest integrated healthcare system in the United States, providing care to millions of veterans at medical centers and clinics nationwide. Working for the VHA means serving those who served our country.\n"
        elif 'air force' in company_lower or 'department of the air force' in company_lower:
            company_context = f"\n\n**About the Employer:**\nAs part of the Air Force civilian workforce, you will support the mission of the United States Air Force. Civilian employees are essential to Air Force operations, bringing specialized skills and continuity to critical programs.\n"
        elif 'bureau of prisons' in company_lower:
            company_context = f"\n\n**About the Bureau of Prisons:**\nThe Federal Bureau of Prisons protects public safety by ensuring that federal offenders serve their sentences in facilities that are safe, humane, cost-efficient, and appropriately secure. BOP employees contribute to public safety and inmate rehabilitation.\n"
        elif 'harvard' in company_lower:
            company_context = f"\n\n**About {company}:**\nHarvard University is one of the world's most renowned academic institutions, committed to excellence in teaching, learning, and research. Working at Harvard means being part of a vibrant community advancing knowledge and making a positive impact.\n"
        elif 'takeda' in company_lower:
            company_context = f"\n\n**About Takeda:**\nTakeda is a global, research and development-driven pharmaceutical company committed to bringing better health and a brighter future to patients by translating science into life-changing medicines.\n"

    # Build closing
    closing = f"\n{template['closing']}"

    # Add location and salary context
    location_salary = ""
    if location or salary_range:
        location_salary = "\n\n**Position Details:**\n"
        if location:
            location_salary += f"• Location: {location}\n"
        if salary_range:
            location_salary += f"• Compensation: {salary_range}\n"

    # Assemble full description
    full_description = overview + company_context + responsibilities + qualifications + location_salary + closing

    return full_description.strip()


def needs_enrichment(desc, title):
    """Determine if a description needs to be enriched."""
    if not desc:
        return True

    desc_lower = desc.lower()

    # Too short
    if len(desc) < 800:
        return True

    # Contains metadata that shouldn't be there
    if 'job title:' in desc_lower or 'employment type:' in desc_lower or 'posting date:' in desc_lower:
        return True

    # Truncated (ends with ellipsis or cut off)
    if desc.rstrip().endswith('…') or desc.rstrip().endswith('...'):
        return True

    # Generic template descriptions (check for our old template signatures)
    if 'This management position leads a team in achieving organizational objectives while developing talent' in desc:
        return True

    if 'This Specialist position applies subject matter expertise to support organizational objectives' in desc:
        return True

    if 'offers an opportunity to contribute your skills and expertise to meaningful work' in desc:
        return True

    # Missing structure (no bullet points and relatively short)
    if '•' not in desc and '**' not in desc and len(desc) < 1200:
        return True

    return False


def enrich_jobs(limit=100, dry_run=False):
    """Enrich jobs that need better descriptions."""
    conn = get_db()
    enriched = 0

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get all jobs that might need enrichment
        cur.execute("""
            SELECT id, title, company_name, location, salary_range, description
            FROM watchable_positions
            WHERE title IS NOT NULL AND company_name IS NOT NULL
            ORDER BY
                CASE WHEN salary_range IS NOT NULL THEN 0 ELSE 1 END,
                LENGTH(COALESCE(description, ''))
        """)

        all_jobs = cur.fetchall()
        print(f"Checking {len(all_jobs)} jobs for enrichment needs...")

        jobs_to_enrich = []
        for job in all_jobs:
            if needs_enrichment(job['description'], job['title']):
                jobs_to_enrich.append(job)
                if limit and len(jobs_to_enrich) >= limit:
                    break

        print(f"Found {len(jobs_to_enrich)} jobs needing enrichment")

        for job in jobs_to_enrich:
            new_desc = generate_structured_description(
                job['title'],
                job['company_name'],
                job['location'],
                job['salary_range']
            )

            if dry_run:
                print(f"\n{'='*70}")
                print(f"WOULD ENRICH: {job['title']} at {job['company_name']}")
                print(f"Location: {job['location']}")
                print(f"Salary: {job['salary_range']}")
                print(f"\nOLD DESCRIPTION ({len(job['description'] or '')} chars):")
                print((job['description'] or 'None')[:300] + '...' if job['description'] and len(job['description']) > 300 else (job['description'] or 'None'))
                print(f"\nNEW DESCRIPTION ({len(new_desc)} chars):")
                print(new_desc[:800] + '...' if len(new_desc) > 800 else new_desc)
            else:
                cur.execute("""
                    UPDATE watchable_positions
                    SET description = %s
                    WHERE id = %s
                """, (new_desc, job['id']))
                enriched += 1

                if enriched % 100 == 0:
                    conn.commit()
                    print(f"  Enriched {enriched} jobs...")

        if not dry_run:
            conn.commit()
            print(f"\nEnriched {enriched} job descriptions")

    conn.close()
    return enriched


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Enhanced job description enrichment')
    parser.add_argument('--limit', type=int, default=100, help='Number of jobs to process')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    parser.add_argument('--all', action='store_true', help='Process all jobs needing enrichment')

    args = parser.parse_args()

    if args.all:
        total = 0
        while True:
            enriched = enrich_jobs(limit=500, dry_run=args.dry_run)
            total += enriched
            if enriched == 0:
                break
            print(f"Total enriched so far: {total}")
        print(f"\nCompleted! Total enriched: {total}")
    else:
        enrich_jobs(limit=args.limit, dry_run=args.dry_run)
