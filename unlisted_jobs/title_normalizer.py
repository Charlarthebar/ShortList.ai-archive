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
        'Software Engineer': [
            r'\bsoftware engineer\b',
            r'\bsoftware developer\b',
            r'\bsde\b',
            r'\bapplication developer\b',
        ],
        'Data Scientist': [
            r'\bdata scientist\b',
            r'\bml engineer\b',
            r'\bmachine learning engineer\b',
        ],
        'Product Manager': [
            r'\bproduct manager\b',
            r'\bpm\b(?!.*project)',  # PM but not project manager
        ],
        'Project Manager': [
            r'\bproject manager\b',
        ],
        'Financial Analyst': [
            r'\bfinancial analyst\b',
            r'\bfinance analyst\b',
        ],
        'Accountant': [
            r'\baccountant\b',
            r'\bcpa\b',
        ],
        'Registered Nurse': [
            r'\bregistered nurse\b',
            r'\brn\b',
        ],
        'Physician': [
            r'\bphysician\b',
            r'\bdoctor\b',
            r'\bmd\b',
        ],
        'Teacher': [
            r'\bteacher\b',
            r'\beducator\b',
        ],
        'Sales Representative': [
            r'\bsales (representative|rep)\b',
            r'\baccount executive\b',
        ],
        'Customer Service Representative': [
            r'\bcustomer service\b',
            r'\bcsr\b',
        ],
        'Administrative Assistant': [
            r'\badministrative assistant\b',
            r'\bexecutive assistant\b',
            r'\boffice (assistant|admin)\b',
        ],
        'Marketing Manager': [
            r'\bmarketing manager\b',
        ],
        'Human Resources Specialist': [
            r'\bhr (specialist|generalist)\b',
            r'\bhuman resources\b',
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

    This is a starter set - in production you'd load the full SOC taxonomy.
    """
    roles = [
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
            'name': 'Data Scientist',
            'soc_code': '15-2051',
            'onet_code': '15-2051.00',
            'role_family': 'Data & Analytics',
            'category': 'Computer and Mathematical',
            'description': 'Develop and implement a set of techniques or analytics applications to transform raw data into meaningful information.',
            'typical_skills': ['Statistics', 'Machine Learning', 'Python', 'SQL', 'Data Analysis']
        },
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
            'name': 'Financial Analyst',
            'soc_code': '13-2051',
            'onet_code': '13-2051.00',
            'role_family': 'Finance',
            'category': 'Business and Financial Operations',
            'description': 'Conduct quantitative analyses of information affecting investment programs of public or private institutions.',
            'typical_skills': ['Financial Modeling', 'Excel', 'Accounting', 'Analysis']
        },
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
            'name': 'Sales Representative',
            'soc_code': '41-4011',
            'onet_code': '41-4011.00',
            'role_family': 'Sales',
            'category': 'Sales',
            'description': 'Sell goods or services to customers.',
            'typical_skills': ['Sales', 'Communication', 'Negotiation', 'Customer Relationship']
        },
        {
            'name': 'Administrative Assistant',
            'soc_code': '43-6014',
            'onet_code': '43-6014.00',
            'role_family': 'Administrative',
            'category': 'Office and Administrative Support',
            'description': 'Perform routine administrative functions such as drafting correspondence, scheduling appointments, organizing and maintaining paper and electronic files.',
            'typical_skills': ['Organization', 'Communication', 'Microsoft Office', 'Scheduling']
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
