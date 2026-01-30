#!/usr/bin/env python3
"""
Title Normalization and Role Mapping
=====================================

Maps raw job titles to canonical roles and extracts seniority levels.

Implements Phase 4 of the comprehensive plan:
- Deterministic-first system with regex rules
- ML classifier for ambiguous titles (optional)
- Confidence scoring
- Human-in-the-loop queue for low-confidence matches

Author: ShortList.ai
"""

import re
import logging
from typing import Tuple, Optional, Dict, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TitleParseResult:
    """Result of title parsing."""
    canonical_role_id: Optional[int]
    canonical_role_name: Optional[str]
    seniority: Optional[str]  # intern/entry/mid/senior/lead/manager/director/exec
    title_confidence: float
    seniority_confidence: float
    level_tokens: List[str]
    function_tokens: List[str]
    domain_tokens: List[str]


class TitleNormalizer:
    """
    Title normalization engine.

    Parses job titles into canonical roles and seniority levels.
    """

    # Seniority detection patterns (ordered by specificity)
    SENIORITY_PATTERNS = [
        # Exec level
        (r'\b(chief|ceo|cfo|cto|cio|coo|president|vp|vice president)\b', 'exec', 0.95),
        (r'\bc-level\b', 'exec', 0.95),

        # Director level
        (r'\b(director|head of)\b', 'director', 0.90),
        (r'\bevp\b', 'director', 0.85),  # Executive VP

        # Manager level
        (r'\b(manager|mgr|managing|supervisor|lead manager)\b', 'manager', 0.88),
        (r'\bteam lead\b', 'manager', 0.75),

        # Lead/Principal level
        (r'\b(principal|lead|staff|architect)\b', 'lead', 0.85),
        (r'\bsenior lead\b', 'lead', 0.90),

        # Senior level
        (r'\b(senior|sr\.?)\b', 'senior', 0.90),
        (r'\biii\b', 'senior', 0.85),

        # Mid level
        (r'\b(mid-level|intermediate)\b', 'mid', 0.88),
        (r'\bii\b', 'mid', 0.85),

        # Entry level
        (r'\b(junior|jr\.?|entry|entry-level|associate|analyst)\b', 'entry', 0.88),
        (r'\bi\b', 'entry', 0.80),

        # Intern
        (r'\b(intern|internship|co-op|coop)\b', 'intern', 0.95),
    ]

    # Common role patterns (high-precision patterns)
    ROLE_PATTERNS = {
        # Software & Engineering
        'Software Engineer': [
            r'\bsoftware engineer\b',
            r'\bsoftware developer\b',
            r'\bsde\b',
            r'\bapplication developer\b',
            r'\bcomputer programmer\b',
            r'\bprogrammer\b',
            r'\bjava developer\b',
            r'\bdot ?net developer\b',
            r'\bsalesforce developer\b',
            r'\bsenior software associate\b',
            r'\bsoftware development\b(?!.*manager)',
        ],
        'Systems Engineer': [
            r'\bsystems? engineer\b',
            r'\bcomputer systems engineer\b',
            r'\bsystems? architect\b',
        ],
        'Network Engineer': [
            r'\bnetwork engineer\b',
            r'\bnetworking engineer\b',
        ],
        'Hardware Engineer': [
            r'\bhardware engineer\b',
            r'\belectrical engineer\b',
            r'\belectronics engineer\b',
        ],
        'Design Engineer': [
            r'\bdesign engineer\b',
            r'\bproduct design engineer\b',
            r'\bmechanical engineer\b',
        ],
        'Technical Operations Engineer': [
            r'\btechnical operations engineer\b',
            r'\boperations engineer\b',
            r'\bsite reliability engineer\b',
            r'\bsre\b',
        ],
        'DevOps Engineer': [
            r'\bdevops engineer\b',
            r'\bdev ops\b',
        ],
        'QA Engineer': [
            r'\bqa engineer\b',
            r'\bquality assurance engineer\b',
            r'\bsoftware quality assurance\b',
            r'\btest engineer\b',
            r'\btesting engineer\b',
        ],
        'Database Administrator': [
            r'\bdatabase administrator\b',
            r'\bdba\b',
        ],
        'Systems Analyst': [
            r'\bsystems? analyst\b',
            r'\bcomputer systems analyst\b',
        ],
        'Process Engineer': [
            r'\bprocess engineer\b',
        ],
        'Validation Engineer': [
            r'\bvalidation engineer\b',
        ],
        'Quality Engineer': [
            r'\bquality engineer\b',
        ],
        'Civil Engineer': [
            r'\bcivil engineer\b',
        ],

        # Data & Analytics
        'Data Scientist': [
            r'\bdata scientist\b',
            r'\bml engineer\b',
            r'\bmachine learning engineer\b',
        ],
        'Data Engineer': [
            r'\bdata engineer\b',
        ],
        'Data Analyst': [
            r'\bdata analyst\b',
            r'\bbusiness intelligence engineer\b',
            r'\bbi engineer\b',
            r'\banalytics engineer\b',
        ],

        # Product & Management
        'Product Manager': [
            r'\bproduct manager\b',
            r'\bpm\b(?!.*project)',  # PM but not project manager
        ],
        'Project Manager': [
            r'\bproject manager\b',
            r'\bproject lead\b',
        ],
        'Program Manager': [
            r'\bprogram manager\b',
        ],
        'Development Manager': [
            r'\bdevelopment manager\b',
            r'\bdev manager\b',
        ],
        'Engineering Manager': [
            r'\bengineering manager\b',
        ],
        'Technical Lead': [
            r'\btechnical lead\b',
            r'\btech lead\b',
        ],

        # Design & UX
        'Designer': [
            r'\bdesigner\b',
            r'\bux designer\b',
            r'\bui designer\b',
            r'\bdesign strategist\b',
            r'\bproduct designer\b',
            r'\bgraphic designer\b',
        ],
        'Architect': [
            r'\barchitect\b(?!.*system)',  # Physical architects, not systems
            r'\bvdc engineer\b',
        ],

        # Research & Science
        'Research Scientist': [
            r'\bresearch scientist\b',
            r'\bresearcher\b',
            r'\bresearch fellow\b',
        ],
        'Economist': [
            r'\beconomist\b',
        ],

        # Business & Finance
        'Financial Analyst': [
            r'\bfinancial analyst\b',
            r'\bfinance analyst\b',
        ],
        'Accountant': [
            r'\baccountant\b',
            r'\bauditor\b',
            r'\bcpa\b',
        ],
        'Business Analyst': [
            r'\bbusiness analyst\b',
        ],

        # Healthcare
        'Registered Nurse': [
            r'\bregistered nurse\b',
            r'\brn\b',
        ],
        'Physician': [
            r'\bphysician\b',
            r'\bdoctor\b',
            r'\bmd\b',
            r'\bpsychiatrist\b',
            r'\bhospitalist\b',
        ],
        'Medical Technologist': [
            r'\bmedical technologist\b',
            r'\bmedical laboratory technician\b',
        ],
        'Physical Therapist': [
            r'\bphysical therapist\b',
            r'\bpt\b(?=.*therapist)',
        ],
        'Postdoctoral Researcher': [
            r'\bpostdoc(toral)?\b',
            r'\bpost-doc(toral)?\b',
        ],

        # Operations & Regulatory
        'Operations Manager': [
            r'\boperations manager\b',
            r'\bops manager\b',
        ],
        'Regulatory Affairs Specialist': [
            r'\bregulatory (affairs|operations)\b',
        ],

        # Sales & Marketing
        'Sales Representative': [
            r'\bsales (representative|rep)\b',
            r'\baccount executive\b',
        ],
        'Marketing Manager': [
            r'\bmarketing manager\b',
        ],

        # Administrative & Support
        'Administrative Assistant': [
            r'\badministrative assistant\b',
            r'\bexecutive assistant\b',
            r'\boffice (assistant|admin)\b',
            r'\badmin(istrative)? support (assistant|professional|clerk)\b',
            r'\blead admin support\b',
            r'\boffice support\b',
        ],
        'Customer Service Representative': [
            r'\bcustomer service\b',
            r'\bcsr\b',
            r'\bcustomer service rep(resentative)?\b',
            r'\bassociate customer service\b',
        ],
        'Human Resources Specialist': [
            r'\bhr (specialist|generalist)\b',
            r'\bhuman resources\b',
        ],

        # Consulting
        'Consultant': [
            r'\bconsultant\b',
        ],

        # Education & Academic
        'Teacher': [
            r'\bteacher\b',
            r'\beducator\b',
        ],
        'Professor': [
            r'\bprofessor\b',
            r'\binstructor\b',
            r'\blecturer\b',
        ],

        # Government & Public Service (Top 10 additions - Phase 1)
        'Social Worker': [
            r'\bsocial worker\b',
            r'\bclinical social worker\b',
            r'\bcsw\b',
            r'\bsocial services specialist\b',
            r'\bsocial svcs specialist\b',
            r'\bsocial service(s)? (specialist|rep|representative)\b',
            r'\bassociate social services\b',
            r'\bsr social services\b',
        ],
        'Correction Officer': [
            r'\bcorrection(s|al)? officer\b',
            r'\bcorrection(s|al)? worker\b',
            r'\bcorrection(s|al)? sergeant\b',
            r'\bcorrection(s|al)? program\b',
        ],
        'Police Officer': [
            r'\bpolice officer\b',
            r'\bstate police trooper\b',
            r'\blaw enforcement\b',
            r'\bsheriff\b',
            r'\btrooper\b',
            r'\bcorporal\b',
            r'\bdetective\b(?!.*lieutenant)',
            r'\bofficer\b(?=.*(police|patrol|special))',
            r'\bspecial police officer\b',
        ],
        'Licensed Practical Nurse': [
            r'\blicensed practical nurse\b',
            r'\blpn\b',
            r'\bpractical nurse\b',
            r'\bpsychiatric nurse\b',
            r'\bsupervisory psychiatric nurse\b',
        ],
        'Program Coordinator': [
            r'\bprogram coordinator\b',
            r'\bprogram assistant\b',
        ],
        'Environmental Analyst': [
            r'\benvironmental analyst\b',
            r'\benvironmental specialist\b',
        ],
        'Human Services Coordinator': [
            r'\bhuman services coordinator\b',
            r'\bsocial services coordinator\b',
        ],
        'Mental Health Worker': [
            r'\bmental health worker\b',
            r'\bmental health counselor\b',
            r'\bbehavioral health\b',
        ],

        # Tech/Engineering (Top 10 additions - Phase 1)
        'Technical Program Manager': [
            r'\btechnical program manager\b',
            r'\btpm\b',
            r'\btechnical program management\b',
            r'\btechnical program specialist\b',
        ],
        'Staff Engineer': [
            r'\bstaff engineer\b',
            r'\bprincipal engineer\b',
            r'\bstaff scientist\b',
        ],

        # Phase 2 Expansion (Roles 11-20)
        'Applied Scientist': [
            r'\bapplied scientist\b',
            r'\bapplied research scientist\b',
        ],
        'Site Reliability Engineer': [
            r'\bsite reliability engineer\b',
            r'\bsre\b',
            r'\breliability engineer\b',
        ],
        'Nursing Assistant': [
            r'\bnursing assistant\b',
            r'\bnursing aide\b',
            r'\bcna\b',
            r'\bcertified nursing assistant\b',
        ],
        'Occupational Therapist': [
            r'\boccupational therapist\b',
            r'\bot\b(?=.*therapist)',
            r'\botr\b',
        ],
        'Market Research Analyst': [
            r'\bmarket research analyst\b',
            r'\bmarket analyst\b',
            r'\bmarketing research\b',
        ],
        'Quantitative Analyst': [
            r'\bquantitative analyst\b',
            r'\bquant analyst\b',
            r'\bquantitative researcher\b',
        ],
        'Tax Specialist': [
            r'\btax specialist\b',
            r'\btax senior\b',
            r'\btax accountant\b',
            r'\btax analyst\b',
        ],
        'Attorney': [
            r'\battorney\b',
            r'\blawyer\b',
            r'\bcounsel\b(?!.*mental)',
            r'\blegal counsel\b',
        ],
        'Clerk': [
            r'\bclerk\b',
            r'\boffice clerk\b',
            r'\badministrative clerk\b',
            r'\bcourt clerk\b',
            r'\bjudicial clerk\b',
            r'\bsenior court clerk\b',
        ],
        'Technical Specialist': [
            r'\btechnical specialist\b',
            r'\bsolution specialist\b',
            r'\btechnical support\b',
            r'\bapplication support\b',
            r'\boffice support specialist\b',
        ],

        # Phase 3 Expansion (Roles 21-35) - Government & Business Focus
        'Management Analyst': [
            r'\bmanagement analyst\b',
            r'\bbusiness management specialist\b',
        ],
        'Environmental Engineer': [
            r'\benvironmental engineer\b',
        ],
        'Paralegal': [
            r'\bparalegal\b',
            r'\blegal assistant\b',
        ],
        'Vocational Rehabilitation Counselor': [
            r'\bvocational rehabilitation counselor\b',
            r'\bvocational rehab counselor\b',
            r'\brehab counselor\b',
            r'\bqual voc rehab counselor\b',
        ],
        'Supervisor': [
            r'\bresidential supervisor\b',
            r'\bcash supervisor\b',
            r'\bsupervisor\b(?!.*correction)',
        ],
        'Highway Maintenance Worker': [
            r'\bhighway maint(enance)? worker\b',
            r'\bhighway maint(enance)? foreman\b',
        ],
        'Lieutenant': [
            r'\blieutenant\b',
            r'\bdetective lieutenant\b',
        ],
        'Sergeant': [
            r'\bsergeant\b',
            r'\bstate police sergeant\b',
        ],
        'Research Associate': [
            r'\bresearch associate\b',
        ],
        'Solutions Architect': [
            r'\bsolutions? architect\b',
            r'\bcloud solution architect\b',
            r'\benterprise architect\b',
        ],
        'Librarian': [
            r'\blibrarian\b',
        ],
        'Firefighter': [
            r'\bfirefighter\b',
            r'\bfire fighter\b',
        ],
        'Compliance Officer': [
            r'\bcompliance officer\b',
        ],
        'Tax Examiner': [
            r'\btax examiner\b',
        ],
        'Mechanic': [
            r'\bmechanic\b',
            r'\bmotor equipment mechanic\b',
        ],

        # Phase 4 Expansion (Roles 36-45) - Final Round
        'Developmental Services Worker': [
            r'\bdevelopmental services w(or)?k(er)?\b',
        ],
        'Child Support Enforcement Specialist': [
            r'\bchild supp(ort)? enforce(ment)? spec(ialist)?\b',
        ],
        'Captain': [
            r'\bcaptain\b',
        ],
        'Caseworker': [
            r'\bcaseworker\b',
            r'\bcase worker\b',
            r'\byouth services caseworker\b',
        ],
        'Inspector': [
            r'\binspector\b',
            r'\bconstruction inspector\b',
            r'\bbus inspector\b',
        ],
        'System Administrator': [
            r'\bsystem administrator\b',
            r'\bsystems administrator\b',
            r'\bsysadmin\b',
        ],
        'Administrative Secretary': [
            r'\badministrative secretary\b',
        ],
        'Recreational Therapist': [
            r'\brecreational therapist\b',
        ],
        'Nurse Practitioner': [
            r'\bnurse practitioner\b',
            r'\bnp\b(?=.*nurse)',
        ],
        'Statistician': [
            r'\bstatistician\b',
        ],

        # Phase 5 Expansion (Roles 91-105) - Blue-Collar & Service Roles
        'Truck Driver': [
            r'\btruck driver\b',
            r'\bcommercial driver\b',
            r'\bcdl driver\b',
            r'\bdelivery driver\b',
            r'\btractor trailer driver\b',
        ],
        'Warehouse Worker': [
            r'\bwarehouse worker\b',
            r'\bwarehouse associate\b',
            r'\bwarehouse oper(ator)?\b',
            r'\bmaterial handler\b',
        ],
        'Housekeeper': [
            r'\bhousekeep(er|ing)\b',
            r'\bhotel housekeep(er|ing)\b',
            r'\broom attendant\b',
        ],
        'Cook': [
            r'\bcook\b',
            r'\bprep cook\b',
            r'\bline cook\b',
            r'\bchef\b',
        ],
        'Caregiver': [
            r'\bcaregiv(er|ing)\b',
            r'\bcare giv(er|ing)\b',
            r'\bhome care (aide|worker)\b',
            r'\bdirect care (aide|worker)\b',
            r'\bsupport care (assistant|asst)\b',
            r'\bsecurity support care\b',
            r'\bresident(ial)? treatment work(er)?\b',
        ],
        'General Laborer': [
            r'\bgeneral labor(er)?\b',
            r'\bfactory worker\b',
            r'\bplant labor(er)?\b',
            r'\bproduction helper\b',
            r'\bmanufacturing labor(er)?\b',
        ],
        'Landscape Laborer': [
            r'\blandscap(e|ing) labor(er)?\b',
            r'\bgrounds(keeper|worker)\b',
            r'\blandscap(e|ing) crew\b',
        ],
        'Janitor': [
            r'\bjanitor\b',
            r'\bcustodian\b',
            r'\bcleaning (worker|technician)\b',
            r'\bsanitation (worker|technician)\b',
            r'\bcustodial (worker|assistant)\b',
        ],
        'Server': [
            r'\bserver\b',
            r'\bwaitress\b',
            r'\bwaiter\b',
            r'\bwaitstaff\b',
        ],
        'Food Service Worker': [
            r'\bfood service (worker|aide)\b',
            r'\bfoodservice worker\b',
            r'\bcafeteria worker\b',
            r'\bkitchen (helper|assistant)\b',
        ],
        'Production Worker': [
            r'\bproduction worker\b',
            r'\bassembly worker\b',
            r'\bmanufacturing (associate|worker)\b',
            r'\bproduction oper(ator)?\b',
        ],
        'Poultry Worker': [
            r'\bpoultry (cutter|trimmer|processor|worker)\b',
            r'\bmeat cutter\b',
            r'\bfish cutter\b',
            r'\bbutcher\b',
        ],
        'Nanny': [
            r'\bnanny\b',
            r'\bchildcare provider\b',
            r'\bchild care provider\b',
        ],
        'Animal Caretaker': [
            r'\banimal care(taker|giver)?\b',
            r'\banimal breeder\b',
            r'\bstable attendant\b',
            r'\bkennel (attendant|worker)\b',
        ],
        'Sewing Machine Operator': [
            r'\bsewing (machine )?oper(ator)?\b',
            r'\bsewing helper\b',
            r'\bsewing (worker|technician)\b',
            r'\btextile worker\b',
        ],

        # Phase 6 - Government/State Payroll Expansion
        'Direct Care Worker': [
            r'\bdirect care (worker|aide)\b',
            r'\bsupport care (assistant|worker)\b',
            r'\bresidential (aide|worker)\b',
        ],
        'Maintenance Worker': [
            r'\bmaintenance worker\b',
            r'\bmaintenance (technician|tech)\b',
            r'\bsenior maintenance worker\b',
            r'\bintermediate maintenance\b',
            r'\bmaintenance crew\b',
            r'\bmaintenance.?grounds\b',
        ],
        'Benefits Specialist': [
            r'\bbenefit(s)? program (technician|specialist|supervisor|associate)\b',
            r'\bincome maint(enance)? worker\b',
            r'\beligibility (worker|specialist)\b',
        ],
        'Youth Services Worker': [
            r'\byouth services worker\b',
            r'\byouth (development|services) (rep|specialist)\b',
        ],
        'Program Specialist': [
            r'\bprogram specialist\b',
            r'\bprogram (analyst|monitor)\b',
        ],
        'Motor Vehicle Operator': [
            r'\bmotor vehicle operator\b',
            r'\bbus (driver|operator|attendant)\b',
        ],
        'Security Officer': [
            r'\bsecurity (officer|guard)\b',
            r'\btraffic control officer\b',
        ],
        'Parole Officer': [
            r'\bprobation (and|&)? parole officer\b',
            r'\bparole officer\b',
            r'\bprobation officer\b',
        ],
        'Eligibility Worker': [
            r'\beligibility (worker|interviewer|specialist)\b',
        ],
        'Public Defender': [
            r'\bpublic defender\b',
            r'\bassistant public defender\b',
        ],
        'Emergency Dispatcher': [
            r'\bemergency (dispatcher|communications)\b',
            r'\b911 (dispatcher|operator)\b',
        ],
        'EMT/Paramedic': [
            r'\bemt\b',
            r'\bparamedic\b',
            r'\bfirefighter (emt|paramedic|tech)\b',
            r'\bemergency medical technician\b',
        ],
        'Equipment Operator': [
            r'\bequipment operator\b',
            r'\bheavy equipment operator\b',
            r'\bemergency maint equip operat\b',
        ],
        'Dietary Aide': [
            r'\bdietary (aide|assistant)\b',
            r'\bfood service (assistant|aide)\b',
        ],
        'Trades Worker': [
            r'\bspecialized trades worker\b',
            r'\bskilled trades\b',
        ],
        'Adjunct Faculty': [
            r'\badjunct (professor|faculty|instructor)\b',
        ],
    }

    def __init__(self, database_manager=None):
        """
        Initialize title normalizer.

        Args:
            database_manager: Optional database manager for loading rules
        """
        self.db = database_manager
        self.role_id_cache = {}

        # Load role mappings from database if available
        if self.db:
            self._load_role_mappings()

    def _load_role_mappings(self):
        """Load canonical roles and mapping rules from database."""
        try:
            conn = self.db.get_connection()
            with conn.cursor() as cursor:
                # Load canonical roles
                cursor.execute("""
                    SELECT id, name, soc_code, role_family
                    FROM canonical_roles
                """)
                for row in cursor.fetchall():
                    self.role_id_cache[row[1]] = {
                        'id': row[0],
                        'name': row[1],
                        'soc_code': row[2],
                        'role_family': row[3]
                    }
            self.db.release_connection(conn)
            logger.info(f"Loaded {len(self.role_id_cache)} canonical roles from database")
        except Exception as e:
            logger.warning(f"Could not load role mappings from database: {e}")

    def parse_title(self, title: str) -> TitleParseResult:
        """
        Parse a raw job title into canonical role and seniority.

        Returns:
            TitleParseResult with canonical role, seniority, and confidence scores
        """
        if not title:
            return TitleParseResult(
                canonical_role_id=None,
                canonical_role_name=None,
                seniority=None,
                title_confidence=0.0,
                seniority_confidence=0.0,
                level_tokens=[],
                function_tokens=[],
                domain_tokens=[]
            )

        title_lower = title.lower()

        # Parse seniority
        seniority, seniority_confidence = self._detect_seniority(title_lower)

        # Parse into components
        level_tokens, function_tokens, domain_tokens = self._tokenize_title(title_lower)

        # Map to canonical role
        canonical_role_name, title_confidence = self._map_to_canonical_role(title_lower)

        # Get role ID if we have database
        canonical_role_id = None
        if canonical_role_name and self.role_id_cache:
            role_info = self.role_id_cache.get(canonical_role_name)
            if role_info:
                canonical_role_id = role_info['id']

        return TitleParseResult(
            canonical_role_id=canonical_role_id,
            canonical_role_name=canonical_role_name,
            seniority=seniority,
            title_confidence=title_confidence,
            seniority_confidence=seniority_confidence,
            level_tokens=level_tokens,
            function_tokens=function_tokens,
            domain_tokens=domain_tokens
        )

    def _detect_seniority(self, title_lower: str) -> Tuple[Optional[str], float]:
        """
        Detect seniority level from title.

        Returns:
            (seniority_level, confidence)
        """
        for pattern, level, confidence in self.SENIORITY_PATTERNS:
            if re.search(pattern, title_lower):
                return level, confidence

        # Default to mid if no seniority detected
        return 'mid', 0.3

    def _tokenize_title(self, title_lower: str) -> Tuple[List[str], List[str], List[str]]:
        """
        Tokenize title into level, function, and domain tokens.

        Returns:
            (level_tokens, function_tokens, domain_tokens)
        """
        # Level tokens (seniority indicators)
        level_keywords = ['senior', 'junior', 'lead', 'principal', 'staff', 'intern',
                         'entry', 'associate', 'manager', 'director', 'chief', 'vp', 'i', 'ii', 'iii']
        level_tokens = [word for word in title_lower.split() if word in level_keywords]

        # Function tokens (what they do)
        function_keywords = ['engineer', 'developer', 'analyst', 'manager', 'scientist',
                           'specialist', 'consultant', 'coordinator', 'representative',
                           'assistant', 'technician', 'nurse', 'physician', 'teacher']
        function_tokens = [word for word in title_lower.split() if word in function_keywords]

        # Domain tokens (area of work)
        domain_keywords = ['software', 'data', 'financial', 'marketing', 'sales', 'customer',
                          'product', 'project', 'human', 'resources', 'it', 'network',
                          'security', 'web', 'mobile', 'cloud', 'database']
        domain_tokens = [word for word in title_lower.split() if word in domain_keywords]

        return level_tokens, function_tokens, domain_tokens

    def _map_to_canonical_role(self, title_lower: str) -> Tuple[Optional[str], float]:
        """
        Map title to canonical role using pattern matching.

        Returns:
            (canonical_role_name, confidence)
        """
        # Try high-precision patterns first
        for canonical_role, patterns in self.ROLE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, title_lower):
                    return canonical_role, 0.90

        # Try fuzzy matching on function words
        if 'engineer' in title_lower:
            if 'software' in title_lower or 'application' in title_lower:
                return 'Software Engineer', 0.75
            elif 'data' in title_lower:
                return 'Data Engineer', 0.75
            elif 'network' in title_lower:
                return 'Network Engineer', 0.75
            else:
                return 'Engineer', 0.60

        if 'analyst' in title_lower:
            if 'data' in title_lower:
                return 'Data Analyst', 0.75
            elif 'financial' in title_lower or 'finance' in title_lower:
                return 'Financial Analyst', 0.75
            elif 'business' in title_lower:
                return 'Business Analyst', 0.75
            else:
                return 'Analyst', 0.60

        if 'manager' in title_lower:
            if 'product' in title_lower:
                return 'Product Manager', 0.75
            elif 'project' in title_lower:
                return 'Project Manager', 0.75
            elif 'marketing' in title_lower:
                return 'Marketing Manager', 0.75
            else:
                return 'Manager', 0.55

        # No match
        return None, 0.0

    def should_queue_for_review(self, result: TitleParseResult, threshold: float = 0.7) -> bool:
        """
        Determine if this title mapping should go to human review queue.

        Args:
            result: TitleParseResult from parse_title
            threshold: Confidence threshold below which to queue for review

        Returns:
            True if should be reviewed
        """
        return (result.title_confidence < threshold or
                result.seniority_confidence < threshold)

    def batch_parse_titles(self, titles: List[str]) -> List[TitleParseResult]:
        """
        Parse multiple titles in batch.

        Args:
            titles: List of raw job titles

        Returns:
            List of TitleParseResult
        """
        return [self.parse_title(title) for title in titles]


def seed_canonical_roles(database_manager):
    """
    Seed database with common canonical roles based on SOC/O*NET.

    Expanded set covering common H-1B positions across tech, engineering,
    healthcare, business, and other fields.
    """
    roles = [
        # Software & Technology
        {
            'name': 'Software Engineer',
            'soc_code': '15-1252',
            'onet_code': '15-1252.00',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Develop, create, and modify general computer applications software or specialized utility programs.',
            'typical_skills': ['Programming', 'Problem Solving', 'Software Development', 'Testing']
        },
        {
            'name': 'Systems Engineer',
            'soc_code': '15-1252',
            'onet_code': '15-1252.00',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Design and develop solutions for complex computer systems and networks.',
            'typical_skills': ['Systems Architecture', 'Infrastructure', 'Integration', 'Troubleshooting']
        },
        {
            'name': 'Network Engineer',
            'soc_code': '15-1244',
            'onet_code': '15-1244.00',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Design, implement, and maintain network infrastructure.',
            'typical_skills': ['Networking', 'TCP/IP', 'Routing', 'Security']
        },
        {
            'name': 'Hardware Engineer',
            'soc_code': '17-2061',
            'onet_code': '17-2061.00',
            'role_family': 'Engineering',
            'category': 'Architecture and Engineering',
            'description': 'Research, design, develop, test, and oversee the manufacturing of computer hardware.',
            'typical_skills': ['Electronics', 'Circuit Design', 'CAD', 'Testing']
        },
        {
            'name': 'Design Engineer',
            'soc_code': '17-2141',
            'onet_code': '17-2141.00',
            'role_family': 'Engineering',
            'category': 'Architecture and Engineering',
            'description': 'Design and develop products, systems, or structures.',
            'typical_skills': ['CAD', 'Product Design', 'Prototyping', 'Analysis']
        },
        {
            'name': 'Technical Operations Engineer',
            'soc_code': '15-1252',
            'onet_code': '15-1252.00',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Ensure reliability and performance of technical systems and infrastructure.',
            'typical_skills': ['DevOps', 'Automation', 'Monitoring', 'Incident Response']
        },

        # Data & Analytics
        {
            'name': 'Data Scientist',
            'soc_code': '15-2051',
            'onet_code': '15-2051.00',
            'role_family': 'Data & Analytics',
            'category': 'Computer and Mathematical',
            'description': 'Develop and implement a set of techniques or analytics applications to transform raw data into meaningful information.',
            'typical_skills': ['Statistics', 'Machine Learning', 'Python', 'SQL', 'Data Analysis']
        },
        {
            'name': 'Data Engineer',
            'soc_code': '15-1243',
            'onet_code': '15-1243.00',
            'role_family': 'Data & Analytics',
            'category': 'Computer and Mathematical',
            'description': 'Build and maintain data pipelines and infrastructure.',
            'typical_skills': ['ETL', 'SQL', 'Data Warehousing', 'Pipeline Development']
        },
        {
            'name': 'Data Analyst',
            'soc_code': '15-2051',
            'onet_code': '15-2051.01',
            'role_family': 'Data & Analytics',
            'category': 'Computer and Mathematical',
            'description': 'Analyze data to help organizations make better business decisions.',
            'typical_skills': ['SQL', 'Excel', 'Data Visualization', 'Business Intelligence']
        },

        # Product & Management
        {
            'name': 'Product Manager',
            'soc_code': '11-2032',
            'onet_code': '11-2032.00',
            'role_family': 'Product',
            'category': 'Management',
            'description': 'Plan, direct, or coordinate marketing policies and programs.',
            'typical_skills': ['Product Strategy', 'Requirements Gathering', 'Stakeholder Management', 'Analytics']
        },
        {
            'name': 'Project Manager',
            'soc_code': '11-9199',
            'onet_code': '11-9199.00',
            'role_family': 'Management',
            'category': 'Management',
            'description': 'Plan, direct, or coordinate activities of a project to ensure goals are accomplished.',
            'typical_skills': ['Project Planning', 'Coordination', 'Risk Management', 'Communication']
        },
        {
            'name': 'Program Manager',
            'soc_code': '11-9199',
            'onet_code': '11-9199.00',
            'role_family': 'Management',
            'category': 'Management',
            'description': 'Manage multiple related projects and coordinate resources across them.',
            'typical_skills': ['Program Management', 'Strategy', 'Leadership', 'Resource Planning']
        },
        {
            'name': 'Development Manager',
            'soc_code': '11-3021',
            'onet_code': '11-3021.00',
            'role_family': 'Management',
            'category': 'Management',
            'description': 'Plan, direct, or coordinate activities of software development teams.',
            'typical_skills': ['Team Leadership', 'Technical Planning', 'Agile', 'People Management']
        },

        # Design & Creative
        {
            'name': 'Designer',
            'soc_code': '27-1021',
            'onet_code': '27-1021.00',
            'role_family': 'Design',
            'category': 'Arts and Design',
            'description': 'Design or create graphics to meet specific commercial or promotional needs.',
            'typical_skills': ['Design', 'Creative Thinking', 'User Experience', 'Visual Communication']
        },
        {
            'name': 'Architect',
            'soc_code': '17-1011',
            'onet_code': '17-1011.00',
            'role_family': 'Architecture',
            'category': 'Architecture and Engineering',
            'description': 'Plan and design structures and spaces.',
            'typical_skills': ['Architecture', 'CAD', 'Design', 'Building Codes']
        },

        # Research & Science
        {
            'name': 'Research Scientist',
            'soc_code': '19-1029',
            'onet_code': '19-1029.00',
            'role_family': 'Research',
            'category': 'Life, Physical, and Social Science',
            'description': 'Conduct research to advance knowledge in a specific field.',
            'typical_skills': ['Research Methods', 'Data Analysis', 'Scientific Writing', 'Experimentation']
        },
        {
            'name': 'Economist',
            'soc_code': '19-3011',
            'onet_code': '19-3011.00',
            'role_family': 'Research',
            'category': 'Life, Physical, and Social Science',
            'description': 'Conduct research, prepare reports, or formulate plans to address economic problems.',
            'typical_skills': ['Economic Analysis', 'Statistics', 'Research', 'Forecasting']
        },

        # Business & Finance
        {
            'name': 'Financial Analyst',
            'soc_code': '13-2051',
            'onet_code': '13-2051.00',
            'role_family': 'Finance',
            'category': 'Business and Financial Operations',
            'description': 'Conduct quantitative analyses of information affecting investment programs of public or private institutions.',
            'typical_skills': ['Financial Modeling', 'Excel', 'Accounting', 'Analysis']
        },
        {
            'name': 'Accountant',
            'soc_code': '13-2011',
            'onet_code': '13-2011.00',
            'role_family': 'Finance',
            'category': 'Business and Financial Operations',
            'description': 'Examine, analyze, and interpret accounting records to prepare financial statements.',
            'typical_skills': ['Accounting', 'Financial Reporting', 'Auditing', 'Tax']
        },
        {
            'name': 'Business Analyst',
            'soc_code': '13-1111',
            'onet_code': '13-1111.00',
            'role_family': 'Business',
            'category': 'Business and Financial Operations',
            'description': 'Analyze business operations and recommend improvements.',
            'typical_skills': ['Business Analysis', 'Requirements Gathering', 'Process Improvement', 'Documentation']
        },

        # Healthcare
        {
            'name': 'Registered Nurse',
            'soc_code': '29-1141',
            'onet_code': '29-1141.00',
            'role_family': 'Healthcare',
            'category': 'Healthcare Practitioners',
            'description': 'Assess patient health problems and needs, develop and implement nursing care plans, and maintain medical records.',
            'typical_skills': ['Patient Care', 'Medical Knowledge', 'Communication', 'Critical Thinking']
        },
        {
            'name': 'Physician',
            'soc_code': '29-1216',
            'onet_code': '29-1216.00',
            'role_family': 'Healthcare',
            'category': 'Healthcare Practitioners',
            'description': 'Diagnose and treat mental, emotional, and behavioral disorders.',
            'typical_skills': ['Medical Diagnosis', 'Patient Care', 'Treatment Planning', 'Medical Knowledge']
        },

        # Operations & Regulatory
        {
            'name': 'Operations Manager',
            'soc_code': '11-1021',
            'onet_code': '11-1021.00',
            'role_family': 'Operations',
            'category': 'Management',
            'description': 'Plan, direct, or coordinate the operations of organizations.',
            'typical_skills': ['Operations Management', 'Process Optimization', 'Leadership', 'Strategy']
        },
        {
            'name': 'Regulatory Affairs Specialist',
            'soc_code': '13-1041',
            'onet_code': '13-1041.00',
            'role_family': 'Regulatory',
            'category': 'Business and Financial Operations',
            'description': 'Ensure compliance with regulations and manage regulatory submissions.',
            'typical_skills': ['Regulatory Compliance', 'Documentation', 'Policy', 'Risk Management']
        },

        # Sales & Marketing
        {
            'name': 'Sales Representative',
            'soc_code': '41-4011',
            'onet_code': '41-4011.00',
            'role_family': 'Sales',
            'category': 'Sales',
            'description': 'Sell goods or services to customers.',
            'typical_skills': ['Sales', 'Communication', 'Negotiation', 'Customer Relationship']
        },
        {
            'name': 'Marketing Manager',
            'soc_code': '11-2021',
            'onet_code': '11-2021.00',
            'role_family': 'Marketing',
            'category': 'Management',
            'description': 'Plan, direct, or coordinate marketing policies and programs.',
            'typical_skills': ['Marketing Strategy', 'Campaign Management', 'Analytics', 'Brand Management']
        },

        # Administrative & Support
        {
            'name': 'Administrative Assistant',
            'soc_code': '43-6014',
            'onet_code': '43-6014.00',
            'role_family': 'Administrative',
            'category': 'Office and Administrative Support',
            'description': 'Perform routine administrative functions such as drafting correspondence, scheduling appointments, organizing and maintaining paper and electronic files.',
            'typical_skills': ['Organization', 'Communication', 'Microsoft Office', 'Scheduling']
        },
        {
            'name': 'Customer Service Representative',
            'soc_code': '43-4051',
            'onet_code': '43-4051.00',
            'role_family': 'Customer Service',
            'category': 'Office and Administrative Support',
            'description': 'Interact with customers to provide information and resolve problems.',
            'typical_skills': ['Customer Service', 'Communication', 'Problem Solving', 'Product Knowledge']
        },
        {
            'name': 'Human Resources Specialist',
            'soc_code': '13-1071',
            'onet_code': '13-1071.00',
            'role_family': 'Human Resources',
            'category': 'Business and Financial Operations',
            'description': 'Recruit, screen, interview, or place individuals for employment.',
            'typical_skills': ['Recruiting', 'HR Policy', 'Employee Relations', 'Compliance']
        },

        # Education
        {
            'name': 'Teacher',
            'soc_code': '25-2031',
            'onet_code': '25-2031.00',
            'role_family': 'Education',
            'category': 'Educational Instruction',
            'description': 'Instruct students in academic subjects.',
            'typical_skills': ['Teaching', 'Curriculum Development', 'Communication', 'Assessment']
        },
        {
            'name': 'Professor',
            'soc_code': '25-1199',
            'onet_code': '25-1199.00',
            'role_family': 'Education',
            'category': 'Educational Instruction',
            'description': 'Teach courses in their field of specialization at the college or university level.',
            'typical_skills': ['Research', 'Teaching', 'Academic Writing', 'Expertise in Field']
        },

        # New High-Impact Roles (January 2026)
        {
            'name': 'DevOps Engineer',
            'soc_code': '15-1252',
            'onet_code': '15-1252.02',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Build and maintain tools for deployment, monitoring, and operations.',
            'typical_skills': ['CI/CD', 'Docker', 'Kubernetes', 'Automation', 'Infrastructure']
        },
        {
            'name': 'QA Engineer',
            'soc_code': '15-1253',
            'onet_code': '15-1253.00',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Test software applications and systems to ensure quality.',
            'typical_skills': ['Testing', 'QA Methodologies', 'Automation', 'Bug Tracking']
        },
        {
            'name': 'Database Administrator',
            'soc_code': '15-1242',
            'onet_code': '15-1242.00',
            'role_family': 'Data & Analytics',
            'category': 'Computer and Mathematical',
            'description': 'Administer, test, and implement computer databases.',
            'typical_skills': ['SQL', 'Database Design', 'Backup & Recovery', 'Performance Tuning']
        },
        {
            'name': 'Systems Analyst',
            'soc_code': '15-1211',
            'onet_code': '15-1211.00',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Analyze data processing problems to improve computer systems.',
            'typical_skills': ['Systems Analysis', 'Requirements Gathering', 'Technical Documentation', 'Problem Solving']
        },
        {
            'name': 'Process Engineer',
            'soc_code': '17-2112',
            'onet_code': '17-2112.00',
            'role_family': 'Engineering',
            'category': 'Architecture and Engineering',
            'description': 'Design and optimize manufacturing and industrial processes.',
            'typical_skills': ['Process Improvement', 'Manufacturing', 'Lean/Six Sigma', 'Technical Analysis']
        },
        {
            'name': 'Validation Engineer',
            'soc_code': '17-2199',
            'onet_code': '17-2199.08',
            'role_family': 'Engineering',
            'category': 'Architecture and Engineering',
            'description': 'Validate that systems and processes meet specifications and regulations.',
            'typical_skills': ['Validation', 'Testing', 'Compliance', 'Documentation']
        },
        {
            'name': 'Quality Engineer',
            'soc_code': '17-2112',
            'onet_code': '17-2112.01',
            'role_family': 'Engineering',
            'category': 'Architecture and Engineering',
            'description': 'Ensure quality standards are met in manufacturing and development.',
            'typical_skills': ['Quality Control', 'Statistical Analysis', 'Process Improvement', 'ISO Standards']
        },
        {
            'name': 'Civil Engineer',
            'soc_code': '17-2051',
            'onet_code': '17-2051.00',
            'role_family': 'Engineering',
            'category': 'Architecture and Engineering',
            'description': 'Design and oversee construction of infrastructure projects.',
            'typical_skills': ['Civil Engineering', 'AutoCAD', 'Project Management', 'Construction']
        },
        {
            'name': 'Engineering Manager',
            'soc_code': '11-9041',
            'onet_code': '11-9041.00',
            'role_family': 'Management',
            'category': 'Management',
            'description': 'Plan, direct, or coordinate activities in engineering or technical fields.',
            'typical_skills': ['Engineering Management', 'Team Leadership', 'Technical Strategy', 'Budget Management']
        },
        {
            'name': 'Technical Lead',
            'soc_code': '15-1299',
            'onet_code': '15-1299.09',
            'role_family': 'Management',
            'category': 'Computer and Mathematical',
            'description': 'Lead technical teams and make architectural decisions.',
            'typical_skills': ['Technical Leadership', 'Architecture', 'Mentoring', 'Code Review']
        },
        {
            'name': 'Consultant',
            'soc_code': '13-1111',
            'onet_code': '13-1111.00',
            'role_family': 'Consulting',
            'category': 'Business and Financial Operations',
            'description': 'Provide expert advice and solutions to organizations.',
            'typical_skills': ['Consulting', 'Problem Solving', 'Communication', 'Industry Expertise']
        },
        {
            'name': 'Medical Technologist',
            'soc_code': '29-2011',
            'onet_code': '29-2011.00',
            'role_family': 'Healthcare',
            'category': 'Healthcare Practitioners',
            'description': 'Perform medical laboratory tests for diagnosis and treatment.',
            'typical_skills': ['Laboratory Testing', 'Medical Equipment', 'Quality Control', 'Data Analysis']
        },
        {
            'name': 'Physical Therapist',
            'soc_code': '29-1123',
            'onet_code': '29-1123.00',
            'role_family': 'Healthcare',
            'category': 'Healthcare Practitioners',
            'description': 'Help patients recover from injuries and improve movement.',
            'typical_skills': ['Physical Therapy', 'Patient Care', 'Exercise Prescription', 'Rehabilitation']
        },
        {
            'name': 'Postdoctoral Researcher',
            'soc_code': '19-1099',
            'onet_code': '19-1099.01',
            'role_family': 'Research',
            'category': 'Life, Physical, and Social Science',
            'description': 'Conduct advanced research in academic or scientific settings.',
            'typical_skills': ['Research', 'Academic Writing', 'Laboratory Skills', 'Grant Writing']
        },

        # Top 10 New Roles (January 2026 - Phase 1 Expansion)
        {
            'name': 'Social Worker',
            'soc_code': '21-1021',
            'onet_code': '21-1021.00',
            'role_family': 'Healthcare & Social Service',
            'category': 'Community and Social Service',
            'description': 'Help people solve and cope with problems in everyday lives.',
            'typical_skills': ['Case Management', 'Counseling', 'Social Services', 'Crisis Intervention']
        },
        {
            'name': 'Correction Officer',
            'soc_code': '33-3012',
            'onet_code': '33-3012.00',
            'role_family': 'Protective Service',
            'category': 'Protective Service',
            'description': 'Guard inmates in correctional facilities.',
            'typical_skills': ['Security', 'Surveillance', 'Crisis Management', 'Law Enforcement']
        },
        {
            'name': 'Police Officer',
            'soc_code': '33-3051',
            'onet_code': '33-3051.00',
            'role_family': 'Protective Service',
            'category': 'Protective Service',
            'description': 'Maintain law and order, protect people and property.',
            'typical_skills': ['Law Enforcement', 'Investigation', 'Public Safety', 'Emergency Response']
        },
        {
            'name': 'Licensed Practical Nurse',
            'soc_code': '29-2061',
            'onet_code': '29-2061.00',
            'role_family': 'Healthcare',
            'category': 'Healthcare Practitioners',
            'description': 'Provide basic nursing care under supervision of registered nurses and doctors.',
            'typical_skills': ['Patient Care', 'Medical Procedures', 'Vital Signs', 'Documentation']
        },
        {
            'name': 'Program Coordinator',
            'soc_code': '13-1199',
            'onet_code': '13-1199.00',
            'role_family': 'Administrative',
            'category': 'Business and Financial Operations',
            'description': 'Coordinate and oversee programs and projects.',
            'typical_skills': ['Program Management', 'Coordination', 'Communication', 'Scheduling']
        },
        {
            'name': 'Environmental Analyst',
            'soc_code': '19-2041',
            'onet_code': '19-2041.00',
            'role_family': 'Science',
            'category': 'Life, Physical, and Social Science',
            'description': 'Analyze environmental data and assess environmental conditions.',
            'typical_skills': ['Environmental Science', 'Data Analysis', 'Compliance', 'Field Work']
        },
        {
            'name': 'Human Services Coordinator',
            'soc_code': '21-1093',
            'onet_code': '21-1093.00',
            'role_family': 'Social Service',
            'category': 'Community and Social Service',
            'description': 'Coordinate social service programs and assist clients.',
            'typical_skills': ['Case Management', 'Social Services', 'Client Advocacy', 'Program Coordination']
        },
        {
            'name': 'Mental Health Worker',
            'soc_code': '21-1014',
            'onet_code': '21-1014.00',
            'role_family': 'Healthcare & Social Service',
            'category': 'Community and Social Service',
            'description': 'Provide mental health services and support to clients.',
            'typical_skills': ['Mental Health', 'Counseling', 'Crisis Intervention', 'Patient Care']
        },
        {
            'name': 'Technical Program Manager',
            'soc_code': '11-9199',
            'onet_code': '11-9199.00',
            'role_family': 'Management',
            'category': 'Management',
            'description': 'Manage technical programs and coordinate engineering teams.',
            'typical_skills': ['Program Management', 'Technical Leadership', 'Cross-functional Coordination', 'Agile']
        },
        {
            'name': 'Staff Engineer',
            'soc_code': '15-1252',
            'onet_code': '15-1252.00',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Senior individual contributor engineer role with technical leadership.',
            'typical_skills': ['Software Engineering', 'Technical Leadership', 'System Design', 'Mentorship']
        },

        # Phase 2 Expansion (January 2026 - Roles 11-20)
        {
            'name': 'Applied Scientist',
            'soc_code': '15-2099',
            'onet_code': '15-2099.01',
            'role_family': 'Data Science & Research',
            'category': 'Computer and Mathematical',
            'description': 'Apply machine learning and AI to solve business problems.',
            'typical_skills': ['Machine Learning', 'Research', 'Statistics', 'Python']
        },
        {
            'name': 'Site Reliability Engineer',
            'soc_code': '15-1252',
            'onet_code': '15-1252.02',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Ensure reliability and uptime of production systems.',
            'typical_skills': ['SRE', 'Monitoring', 'Incident Response', 'Automation']
        },
        {
            'name': 'Nursing Assistant',
            'soc_code': '31-1131',
            'onet_code': '31-1131.00',
            'role_family': 'Healthcare',
            'category': 'Healthcare Support',
            'description': 'Provide basic patient care under supervision.',
            'typical_skills': ['Patient Care', 'Vital Signs', 'Hygiene', 'Mobility Assistance']
        },
        {
            'name': 'Occupational Therapist',
            'soc_code': '29-1122',
            'onet_code': '29-1122.00',
            'role_family': 'Healthcare',
            'category': 'Healthcare Practitioners',
            'description': 'Help patients develop, recover, or maintain daily living skills.',
            'typical_skills': ['Occupational Therapy', 'Patient Assessment', 'Treatment Planning', 'Rehabilitation']
        },
        {
            'name': 'Market Research Analyst',
            'soc_code': '13-1161',
            'onet_code': '13-1161.00',
            'role_family': 'Business & Marketing',
            'category': 'Business and Financial Operations',
            'description': 'Research market conditions to examine potential sales of products or services.',
            'typical_skills': ['Market Research', 'Data Analysis', 'Survey Design', 'Reporting']
        },
        {
            'name': 'Quantitative Analyst',
            'soc_code': '15-2099',
            'onet_code': '15-2099.01',
            'role_family': 'Finance & Analysis',
            'category': 'Computer and Mathematical',
            'description': 'Develop and implement complex mathematical models for financial analysis.',
            'typical_skills': ['Quantitative Analysis', 'Financial Modeling', 'Statistics', 'Programming']
        },
        {
            'name': 'Tax Specialist',
            'soc_code': '13-2082',
            'onet_code': '13-2082.00',
            'role_family': 'Accounting & Finance',
            'category': 'Business and Financial Operations',
            'description': 'Prepare tax returns and advise on tax matters.',
            'typical_skills': ['Tax Preparation', 'Tax Law', 'Compliance', 'Financial Analysis']
        },
        {
            'name': 'Attorney',
            'soc_code': '23-1011',
            'onet_code': '23-1011.00',
            'role_family': 'Legal',
            'category': 'Legal',
            'description': 'Represent clients in criminal and civil litigation and other legal matters.',
            'typical_skills': ['Legal Research', 'Litigation', 'Contract Law', 'Legal Writing']
        },
        {
            'name': 'Clerk',
            'soc_code': '43-9061',
            'onet_code': '43-9061.00',
            'role_family': 'Administrative',
            'category': 'Office and Administrative Support',
            'description': 'Perform general office duties such as filing, typing, and data entry.',
            'typical_skills': ['Filing', 'Data Entry', 'Office Administration', 'Record Keeping']
        },
        {
            'name': 'Technical Specialist',
            'soc_code': '15-1299',
            'onet_code': '15-1299.09',
            'role_family': 'Technical Support',
            'category': 'Computer and Mathematical',
            'description': 'Provide technical support and expertise in specific technical areas.',
            'typical_skills': ['Technical Support', 'Problem Solving', 'Customer Service', 'Product Knowledge']
        },

        # Phase 3 Expansion (January 2026 - Roles 21-35)
        {
            'name': 'Management Analyst',
            'soc_code': '13-1111',
            'onet_code': '13-1111.00',
            'role_family': 'Business & Management',
            'category': 'Business and Financial Operations',
            'description': 'Analyze and propose improvements to organizational operations.',
            'typical_skills': ['Business Analysis', 'Process Improvement', 'Data Analysis', 'Reporting']
        },
        {
            'name': 'Environmental Engineer',
            'soc_code': '17-2081',
            'onet_code': '17-2081.00',
            'role_family': 'Engineering',
            'category': 'Architecture and Engineering',
            'description': 'Design and oversee environmental protection projects.',
            'typical_skills': ['Environmental Engineering', 'Compliance', 'Project Management', 'Technical Design']
        },
        {
            'name': 'Paralegal',
            'soc_code': '23-2011',
            'onet_code': '23-2011.00',
            'role_family': 'Legal',
            'category': 'Legal',
            'description': 'Assist lawyers by investigating facts, preparing legal documents, and researching legal precedent.',
            'typical_skills': ['Legal Research', 'Document Preparation', 'Case Management', 'Legal Writing']
        },
        {
            'name': 'Vocational Rehabilitation Counselor',
            'soc_code': '21-1015',
            'onet_code': '21-1015.00',
            'role_family': 'Social Service',
            'category': 'Community and Social Service',
            'description': 'Help people with disabilities develop skills and find employment.',
            'typical_skills': ['Counseling', 'Case Management', 'Vocational Assessment', 'Job Placement']
        },
        {
            'name': 'Supervisor',
            'soc_code': '43-1011',
            'onet_code': '43-1011.00',
            'role_family': 'Management',
            'category': 'Management',
            'description': 'Supervise and coordinate activities of workers.',
            'typical_skills': ['Supervision', 'Team Management', 'Scheduling', 'Performance Management']
        },
        {
            'name': 'Highway Maintenance Worker',
            'soc_code': '47-4051',
            'onet_code': '47-4051.00',
            'role_family': 'Construction & Maintenance',
            'category': 'Construction and Extraction',
            'description': 'Maintain highways, roads, and runways.',
            'typical_skills': ['Road Maintenance', 'Equipment Operation', 'Safety', 'Manual Labor']
        },
        {
            'name': 'Lieutenant',
            'soc_code': '33-1012',
            'onet_code': '33-1012.00',
            'role_family': 'Protective Service',
            'category': 'Protective Service',
            'description': 'Supervise and coordinate activities of police officers or firefighters.',
            'typical_skills': ['Law Enforcement', 'Leadership', 'Emergency Response', 'Investigation']
        },
        {
            'name': 'Sergeant',
            'soc_code': '33-1012',
            'onet_code': '33-1012.00',
            'role_family': 'Protective Service',
            'category': 'Protective Service',
            'description': 'Supervise and coordinate activities of police officers.',
            'typical_skills': ['Law Enforcement', 'Supervision', 'Investigation', 'Public Safety']
        },
        {
            'name': 'Research Associate',
            'soc_code': '19-4061',
            'onet_code': '19-4061.00',
            'role_family': 'Research',
            'category': 'Life, Physical, and Social Science',
            'description': 'Assist scientists and engineers in research and development.',
            'typical_skills': ['Research', 'Data Collection', 'Laboratory Skills', 'Technical Analysis']
        },
        {
            'name': 'Solutions Architect',
            'soc_code': '15-1199',
            'onet_code': '15-1199.09',
            'role_family': 'Engineering',
            'category': 'Computer and Mathematical',
            'description': 'Design and oversee implementation of complex technical solutions.',
            'typical_skills': ['Solution Architecture', 'Cloud Computing', 'System Design', 'Technical Leadership']
        },
        {
            'name': 'Librarian',
            'soc_code': '25-4021',
            'onet_code': '25-4021.00',
            'role_family': 'Education & Library',
            'category': 'Education, Training, and Library',
            'description': 'Administer and maintain library collections and assist users.',
            'typical_skills': ['Library Science', 'Information Management', 'Research Assistance', 'Cataloging']
        },
        {
            'name': 'Firefighter',
            'soc_code': '33-2011',
            'onet_code': '33-2011.00',
            'role_family': 'Protective Service',
            'category': 'Protective Service',
            'description': 'Control and extinguish fires and respond to emergencies.',
            'typical_skills': ['Firefighting', 'Emergency Medical', 'Equipment Operation', 'Physical Fitness']
        },
        {
            'name': 'Compliance Officer',
            'soc_code': '13-1041',
            'onet_code': '13-1041.07',
            'role_family': 'Business & Compliance',
            'category': 'Business and Financial Operations',
            'description': 'Ensure organization complies with laws and regulations.',
            'typical_skills': ['Compliance', 'Auditing', 'Policy Development', 'Risk Management']
        },
        {
            'name': 'Tax Examiner',
            'soc_code': '13-2081',
            'onet_code': '13-2081.00',
            'role_family': 'Accounting & Finance',
            'category': 'Business and Financial Operations',
            'description': 'Determine tax liability and collect taxes from individuals or businesses.',
            'typical_skills': ['Tax Law', 'Auditing', 'Financial Analysis', 'Compliance']
        },
        {
            'name': 'Mechanic',
            'soc_code': '49-3023',
            'onet_code': '49-3023.00',
            'role_family': 'Maintenance & Repair',
            'category': 'Installation, Maintenance, and Repair',
            'description': 'Diagnose and repair mechanical issues in vehicles and equipment.',
            'typical_skills': ['Mechanical Repair', 'Diagnostics', 'Equipment Maintenance', 'Problem Solving']
        },

        # Phase 4 Expansion (January 2026 - Roles 36-45)
        {
            'name': 'Developmental Services Worker',
            'soc_code': '21-1093',
            'onet_code': '21-1093.00',
            'role_family': 'Social Service',
            'category': 'Community and Social Service',
            'description': 'Assist individuals with developmental disabilities in daily activities.',
            'typical_skills': ['Direct Care', 'Behavioral Support', 'Daily Living Assistance', 'Documentation']
        },
        {
            'name': 'Child Support Enforcement Specialist',
            'soc_code': '23-1099',
            'onet_code': '23-1099.00',
            'role_family': 'Legal Support',
            'category': 'Legal',
            'description': 'Enforce child support orders and investigate cases.',
            'typical_skills': ['Case Management', 'Legal Procedures', 'Investigation', 'Enforcement']
        },
        {
            'name': 'Captain',
            'soc_code': '33-1011',
            'onet_code': '33-1011.00',
            'role_family': 'Protective Service',
            'category': 'Protective Service',
            'description': 'Command and supervise police, fire, or correctional officers.',
            'typical_skills': ['Leadership', 'Emergency Management', 'Operations', 'Personnel Management']
        },
        {
            'name': 'Caseworker',
            'soc_code': '21-1099',
            'onet_code': '21-1099.00',
            'role_family': 'Social Service',
            'category': 'Community and Social Service',
            'description': 'Assess and provide services to individuals and families in need.',
            'typical_skills': ['Case Management', 'Assessment', 'Resource Coordination', 'Documentation']
        },
        {
            'name': 'Inspector',
            'soc_code': '47-4011',
            'onet_code': '47-4011.00',
            'role_family': 'Construction & Inspection',
            'category': 'Construction and Extraction',
            'description': 'Inspect buildings, vehicles, or equipment for compliance and safety.',
            'typical_skills': ['Inspection', 'Building Codes', 'Safety Standards', 'Documentation']
        },
        {
            'name': 'System Administrator',
            'soc_code': '15-1244',
            'onet_code': '15-1244.00',
            'role_family': 'IT Operations',
            'category': 'Computer and Mathematical',
            'description': 'Install, configure, and maintain computer systems and networks.',
            'typical_skills': ['System Administration', 'Linux/Windows', 'Networking', 'Troubleshooting']
        },
        {
            'name': 'Administrative Secretary',
            'soc_code': '43-6014',
            'onet_code': '43-6014.00',
            'role_family': 'Administrative',
            'category': 'Office and Administrative Support',
            'description': 'Provide high-level administrative support to executives or departments.',
            'typical_skills': ['Executive Support', 'Scheduling', 'Communication', 'Office Management']
        },
        {
            'name': 'Recreational Therapist',
            'soc_code': '29-1125',
            'onet_code': '29-1125.00',
            'role_family': 'Healthcare',
            'category': 'Healthcare Practitioners',
            'description': 'Use recreation and leisure activities to help patients improve health.',
            'typical_skills': ['Recreational Therapy', 'Activity Planning', 'Patient Assessment', 'Treatment Goals']
        },
        {
            'name': 'Nurse Practitioner',
            'soc_code': '29-1171',
            'onet_code': '29-1171.00',
            'role_family': 'Healthcare',
            'category': 'Healthcare Practitioners',
            'description': 'Provide advanced nursing care and can prescribe medication.',
            'typical_skills': ['Advanced Practice Nursing', 'Diagnosis', 'Prescribing', 'Patient Care']
        },
        {
            'name': 'Statistician',
            'soc_code': '15-2041',
            'onet_code': '15-2041.00',
            'role_family': 'Data Science & Research',
            'category': 'Computer and Mathematical',
            'description': 'Apply statistical methods to collect, analyze, and interpret data.',
            'typical_skills': ['Statistical Analysis', 'Data Modeling', 'R/Python', 'Research Design']
        },

        # Phase 5 Expansion (January 2026 - Roles 46-60) - Blue-Collar & Service Roles
        {
            'name': 'Truck Driver',
            'soc_code': '53-3032',
            'onet_code': '53-3032.00',
            'role_family': 'Transportation',
            'category': 'Transportation and Material Moving',
            'description': 'Drive truck to transport goods and materials over long or short distances.',
            'typical_skills': ['Commercial Driving', 'CDL License', 'Route Planning', 'Vehicle Maintenance']
        },
        {
            'name': 'Warehouse Worker',
            'soc_code': '53-7065',
            'onet_code': '53-7065.00',
            'role_family': 'Material Handling',
            'category': 'Transportation and Material Moving',
            'description': 'Receive, store, and distribute materials, tools, and products.',
            'typical_skills': ['Inventory Management', 'Forklift Operation', 'Material Handling', 'Shipping & Receiving']
        },
        {
            'name': 'Housekeeper',
            'soc_code': '37-2012',
            'onet_code': '37-2012.00',
            'role_family': 'Cleaning & Maintenance',
            'category': 'Building and Grounds Cleaning and Maintenance',
            'description': 'Clean and maintain hotels, homes, hospitals, and other facilities.',
            'typical_skills': ['Cleaning', 'Sanitization', 'Attention to Detail', 'Time Management']
        },
        {
            'name': 'Cook',
            'soc_code': '35-2014',
            'onet_code': '35-2014.00',
            'role_family': 'Food Preparation',
            'category': 'Food Preparation and Serving',
            'description': 'Prepare and cook food in restaurants, cafeterias, and other facilities.',
            'typical_skills': ['Food Preparation', 'Cooking', 'Food Safety', 'Kitchen Equipment']
        },
        {
            'name': 'Caregiver',
            'soc_code': '39-9021',
            'onet_code': '39-9021.00',
            'role_family': 'Personal Care',
            'category': 'Personal Care and Service',
            'description': 'Provide personal care and assistance to elderly, disabled, or ill individuals.',
            'typical_skills': ['Personal Care', 'Patient Support', 'Companionship', 'Daily Living Assistance']
        },
        {
            'name': 'General Laborer',
            'soc_code': '51-9199',
            'onet_code': '51-9199.00',
            'role_family': 'Production',
            'category': 'Production Occupations',
            'description': 'Perform various physical tasks in manufacturing, construction, or other settings.',
            'typical_skills': ['Physical Labor', 'Equipment Operation', 'Safety Procedures', 'Teamwork']
        },
        {
            'name': 'Landscape Laborer',
            'soc_code': '37-3011',
            'onet_code': '37-3011.00',
            'role_family': 'Grounds Maintenance',
            'category': 'Building and Grounds Cleaning and Maintenance',
            'description': 'Maintain lawns, gardens, and grounds using hand and power tools.',
            'typical_skills': ['Landscaping', 'Equipment Operation', 'Plant Care', 'Outdoor Work']
        },
        {
            'name': 'Janitor',
            'soc_code': '37-2011',
            'onet_code': '37-2011.00',
            'role_family': 'Cleaning & Maintenance',
            'category': 'Building and Grounds Cleaning and Maintenance',
            'description': 'Clean and maintain buildings, offices, and other facilities.',
            'typical_skills': ['Cleaning', 'Maintenance', 'Custodial Work', 'Facility Operations']
        },
        {
            'name': 'Server',
            'soc_code': '35-3031',
            'onet_code': '35-3031.00',
            'role_family': 'Food Service',
            'category': 'Food Preparation and Serving',
            'description': 'Serve food and beverages to customers in restaurants and dining facilities.',
            'typical_skills': ['Customer Service', 'Food Service', 'Menu Knowledge', 'Order Taking']
        },
        {
            'name': 'Food Service Worker',
            'soc_code': '35-3023',
            'onet_code': '35-3023.00',
            'role_family': 'Food Service',
            'category': 'Food Preparation and Serving',
            'description': 'Perform various food preparation and service tasks in cafeterias and institutions.',
            'typical_skills': ['Food Service', 'Food Prep', 'Cleaning', 'Customer Service']
        },
        {
            'name': 'Production Worker',
            'soc_code': '51-2090',
            'onet_code': '51-2090.00',
            'role_family': 'Manufacturing',
            'category': 'Production Occupations',
            'description': 'Assemble or produce goods in manufacturing facilities.',
            'typical_skills': ['Assembly', 'Manufacturing', 'Quality Control', 'Equipment Operation']
        },
        {
            'name': 'Poultry Worker',
            'soc_code': '51-3022',
            'onet_code': '51-3022.00',
            'role_family': 'Food Processing',
            'category': 'Production Occupations',
            'description': 'Process, trim, and cut poultry, meat, or fish products.',
            'typical_skills': ['Meat Processing', 'Knife Skills', 'Food Safety', 'Production Line Work']
        },
        {
            'name': 'Nanny',
            'soc_code': '39-9011',
            'onet_code': '39-9011.00',
            'role_family': 'Childcare',
            'category': 'Personal Care and Service',
            'description': 'Provide childcare in private households.',
            'typical_skills': ['Childcare', 'Early Childhood Development', 'Safety', 'Activity Planning']
        },
        {
            'name': 'Animal Caretaker',
            'soc_code': '39-2021',
            'onet_code': '39-2021.00',
            'role_family': 'Animal Care',
            'category': 'Personal Care and Service',
            'description': 'Care for animals in kennels, animal shelters, zoos, or breeding facilities.',
            'typical_skills': ['Animal Care', 'Feeding', 'Cleaning', 'Animal Handling']
        },
        {
            'name': 'Sewing Machine Operator',
            'soc_code': '51-6031',
            'onet_code': '51-6031.00',
            'role_family': 'Textile Production',
            'category': 'Production Occupations',
            'description': 'Operate sewing machines to produce garments and textile products.',
            'typical_skills': ['Sewing', 'Machine Operation', 'Pattern Reading', 'Quality Control']
        },
    ]

    for role in roles:
        try:
            database_manager.insert_canonical_role(**role)
            logger.info(f"Inserted canonical role: {role['name']}")
        except Exception as e:
            logger.warning(f"Could not insert role {role['name']}: {e}")


if __name__ == "__main__":
    # Test the title normalizer
    logging.basicConfig(level=logging.INFO)

    normalizer = TitleNormalizer()

    test_titles = [
        "Senior Software Engineer",
        "Junior Data Analyst",
        "Product Manager",
        "Staff Engineer",
        "Director of Engineering",
        "Intern - Software Development",
        "RN - Registered Nurse",
        "Financial Analyst II",
        "Lead Product Designer",
    ]

    print("\n" + "="*60)
    print("TITLE NORMALIZATION TEST")
    print("="*60)

    for title in test_titles:
        result = normalizer.parse_title(title)
        print(f"\nRaw Title: {title}")
        print(f"  Canonical Role: {result.canonical_role_name} (confidence: {result.title_confidence:.2f})")
        print(f"  Seniority: {result.seniority} (confidence: {result.seniority_confidence:.2f})")
        print(f"  Needs Review: {normalizer.should_queue_for_review(result)}")
