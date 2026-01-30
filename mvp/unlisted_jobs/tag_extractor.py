"""
Tag Extractor for Two-Level Role System
========================================

Extracts domain, tech stack, and function tags from:
- Job titles
- Job descriptions
- Company industry/metadata

This is Level 2 of the role system - fine-grained specialization
that sits on top of canonical roles.
"""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


@dataclass
class ExtractedTags:
    """Container for extracted tags with confidence scores."""
    domain_tags: List[Tuple[str, float]] = field(default_factory=list)  # (tag_name, confidence)
    tech_tags: List[Tuple[str, float]] = field(default_factory=list)
    function_tags: List[Tuple[str, float]] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            'domain': [(t, c) for t, c in self.domain_tags],
            'tech': [(t, c) for t, c in self.tech_tags],
            'function': [(t, c) for t, c in self.function_tags],
        }


class TagExtractor:
    """
    Extracts specialization tags from job titles and descriptions.

    Design principles:
    - High precision over recall (better to miss a tag than add wrong one)
    - Confidence scores reflect certainty
    - Aliases normalized to canonical tag names
    """

    # =========================================================================
    # DOMAIN PATTERNS (Industry/Vertical/Problem Space)
    # =========================================================================
    DOMAIN_PATTERNS = {
        # Industries - typically from company context or explicit mention
        'Healthcare': [
            r'\b(healthcare|health\s*care|medical|clinical|hospital|pharma|biotech|health\s*tech)\b',
            r'\b(patient|ehr|emr|hipaa|fhir|hl7)\b',
        ],
        'Fintech': [
            r'\b(fintech|financial\s*(services|technology)|banking|payments?|insurance|insurtech)\b',
            r'\b(trading|investment|wealth|lending|credit|mortgage)\b',
        ],
        'E-commerce': [
            r'\b(e-?commerce|retail|marketplace|shopping|merchant|checkout|cart)\b',
        ],
        'Gaming': [
            r'\b(gaming|game\s*dev|video\s*games?|game\s*engine|unity|unreal)\b',
        ],
        'Adtech': [
            r'\b(adtech|advertising|marketing\s*tech|programmatic|rtb|dsp|ssp)\b',
        ],
        'Edtech': [
            r'\b(edtech|education|learning|lms|e-?learning|courseware)\b',
        ],
        'Govtech': [
            r'\b(govtech|government|public\s*sector|civic\s*tech|federal|municipal)\b',
        ],
        'Logistics': [
            r'\b(logistics|supply\s*chain|shipping|freight|transportation|warehouse|fulfillment)\b',
        ],
        'Media': [
            r'\b(media|entertainment|streaming|content|video|music|podcast)\b',
        ],
        # Problem Spaces - often in title
        'Security': [
            r'\b(security|cybersecurity|infosec|appsec|devsecops|soc\s*analyst)\b',
            r'\b(penetration|vulnerability|threat|incident\s*response)\b',
        ],
        'AI/ML': [
            r'\b(machine\s*learning|ml\b|artificial\s*intelligence|ai\b|deep\s*learning)\b',
            r'\b(nlp|natural\s*language|computer\s*vision|neural\s*net|llm)\b',
        ],
        'Data': [
            r'\b(data\s*(engineer|scientist|analyst)|analytics|bi\b|business\s*intelligence)\b',
            r'\b(data\s*warehouse|etl|data\s*pipeline|data\s*lake)\b',
        ],
        'Cloud': [
            r'\b(cloud|aws|gcp|azure|cloud\s*native|multi-?cloud)\b',
        ],
        'Mobile': [
            r'\b(mobile|ios|android|react\s*native|flutter|swift|kotlin)\b',
        ],
        'Crypto': [
            r'\b(crypto|blockchain|web3|defi|smart\s*contract|solidity|ethereum)\b',
        ],
    }

    # =========================================================================
    # TECH STACK PATTERNS (Tools/Technologies)
    # =========================================================================
    TECH_PATTERNS = {
        # Languages
        'Python': [r'\bpython\b', r'\bpy\b(?!.*thon)'],
        'JavaScript': [r'\bjavascript\b', r'\bnode\.?js\b', r'\bjs\b'],
        'TypeScript': [r'\btypescript\b', r'\bts\b(?!.*ql)'],
        'Java': [r'\bjava\b(?!script)', r'\bjvm\b', r'\bspring\b'],
        'Go': [r'\bgolang\b', r'\bgo\b(?=\s*(developer|engineer|experience|programming))'],
        'Rust': [r'\brust\b(?=\s*(developer|engineer|experience|programming))'],
        'C++': [r'\bc\+\+\b', r'\bcpp\b'],
        'SQL': [r'\bsql\b'],
        'Scala': [r'\bscala\b'],
        # Frameworks
        'React': [r'\breact\.?js?\b', r'\breact\s*native\b'],
        'Django': [r'\bdjango\b'],
        'FastAPI': [r'\bfastapi\b'],
        'Spring': [r'\bspring\s*(boot|framework)?\b'],
        'Rails': [r'\bruby\s*on\s*rails\b', r'\brails\b'],
        'Vue': [r'\bvue\.?js?\b'],
        'Angular': [r'\bangular\b'],
        'Next.js': [r'\bnext\.?js\b'],
        # Infrastructure
        'Kubernetes': [r'\bkubernetes\b', r'\bk8s\b'],
        'Docker': [r'\bdocker\b', r'\bcontainer(s|ization)?\b'],
        'Terraform': [r'\bterraform\b', r'\biac\b'],
        'AWS': [r'\baws\b', r'\bamazon\s*web\s*services\b', r'\bec2\b', r'\bs3\b', r'\blambda\b'],
        'GCP': [r'\bgcp\b', r'\bgoogle\s*cloud\b', r'\bbigquery\b'],
        'Azure': [r'\bazure\b', r'\bmicrosoft\s*cloud\b'],
        # Databases
        'PostgreSQL': [r'\bpostgres(ql)?\b', r'\bpg\b'],
        'MySQL': [r'\bmysql\b'],
        'MongoDB': [r'\bmongo(db)?\b'],
        'Redis': [r'\bredis\b'],
        'Elasticsearch': [r'\belasticsearch\b', r'\belastic\b'],
        'Cassandra': [r'\bcassandra\b'],
        'DynamoDB': [r'\bdynamodb\b'],
        # Data Tools
        'Spark': [r'\b(apache\s*)?spark\b', r'\bpyspark\b'],
        'Snowflake': [r'\bsnowflake\b'],
        'Airflow': [r'\b(apache\s*)?airflow\b'],
        'dbt': [r'\bdbt\b'],
        'Kafka': [r'\b(apache\s*)?kafka\b'],
        'Flink': [r'\b(apache\s*)?flink\b'],
        'Databricks': [r'\bdatabricks\b'],
        # ML/AI Tools
        'TensorFlow': [r'\btensorflow\b', r'\btf\b(?=\s*(model|framework))'],
        'PyTorch': [r'\bpytorch\b'],
        'Scikit-learn': [r'\bscikit-?learn\b', r'\bsklearn\b'],
        'Hugging Face': [r'\bhugging\s*face\b', r'\btransformers\b'],
    }

    # =========================================================================
    # FUNCTION PATTERNS (Team/Focus Area)
    # =========================================================================
    FUNCTION_PATTERNS = {
        # Team Types
        'Platform': [
            r'\bplatform\s*(engineer|team|engineering)?\b',
            r'\bdeveloper\s*platform\b',
        ],
        'Infrastructure': [
            r'\binfra(structure)?\s*(engineer|team)?\b',
            r'\bcore\s*infrastructure\b',
        ],
        'Backend': [
            r'\bbackend\b', r'\bback-?end\b', r'\bserver-?side\b',
            r'\bapi\s*(engineer|developer)\b',
        ],
        'Frontend': [
            r'\bfrontend\b', r'\bfront-?end\b', r'\bclient-?side\b',
            r'\bui\s*(engineer|developer)\b', r'\bweb\s*developer\b',
        ],
        'Fullstack': [
            r'\bfull-?stack\b', r'\bfullstack\b',
        ],
        'DevOps': [
            r'\bdevops\b', r'\bdev-?ops\b',
            r'\bsite\s*reliability\b', r'\bsre\b',
        ],
        'QA': [
            r'\bqa\b', r'\bquality\s*assurance\b',
            r'\btest(ing)?\s*(engineer|automation)\b', r'\bsdet\b',
        ],
        # Focus Areas
        'Growth': [
            r'\bgrowth\s*(engineer|team|engineering)?\b',
            r'\bexperimentation\b', r'\ba/b\s*test\b',
        ],
        'Core': [
            r'\bcore\s*(product|systems|team)\b',
        ],
        'Internal Tools': [
            r'\binternal\s*tools?\b', r'\bdeveloper\s*experience\b',
            r'\btooling\b', r'\bdx\b(?=\s*engineer)',
        ],
        'Compliance': [
            r'\bcompliance\b', r'\bregulatory\b', r'\bgrc\b',
            r'\baudit\b', r'\bsox\b', r'\bpci\b',
        ],
        'Integrations': [
            r'\bintegrations?\b', r'\bpartnerships?\b',
            r'\bapi\s*integrations?\b', r'\bthird-?party\b',
        ],
        'Search': [
            r'\bsearch\s*(engineer|team)?\b', r'\bdiscovery\b',
            r'\brelevance\b', r'\branking\b',
        ],
        'Recommendations': [
            r'\brecommendations?\b', r'\bpersonalization\b',
            r'\brecsys\b', r'\bcollaborative\s*filtering\b',
        ],
        'Payments': [
            r'\bpayments?\s*(engineer|team)?\b', r'\bbilling\b',
            r'\bcheckout\b', r'\btransactions?\b', r'\bstripe\b',
        ],
        'Identity': [
            r'\bidentity\b', r'\bauth(entication|orization)?\b',
            r'\biam\b', r'\boauth\b', r'\bsso\b',
        ],
        'Messaging': [
            r'\bmessaging\b', r'\bnotifications?\b',
            r'\bcommunications?\b', r'\bpush\s*notifications?\b',
        ],
        'Observability': [
            r'\bobservability\b', r'\bmonitoring\b',
            r'\blogging\b', r'\btracing\b', r'\bmetrics\b',
        ],
    }

    def __init__(self, db_manager=None):
        """
        Initialize tag extractor.

        Args:
            db_manager: Optional database manager to load custom tags
        """
        self.db = db_manager
        # Could load additional patterns from database here

    def extract_from_title(self, title: str) -> ExtractedTags:
        """
        Extract tags from a job title.

        Title extraction is higher confidence for function/tech,
        lower confidence for domain (usually need company context).
        """
        if not title:
            return ExtractedTags()

        title_lower = title.lower()
        result = ExtractedTags()

        # Domain tags from title (lower confidence - usually implicit)
        for tag, patterns in self.DOMAIN_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, title_lower, re.IGNORECASE):
                    result.domain_tags.append((tag, 0.7))
                    break

        # Tech tags from title (high confidence)
        for tag, patterns in self.TECH_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, title_lower, re.IGNORECASE):
                    result.tech_tags.append((tag, 0.9))
                    break

        # Function tags from title (high confidence)
        for tag, patterns in self.FUNCTION_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, title_lower, re.IGNORECASE):
                    result.function_tags.append((tag, 0.9))
                    break

        return result

    def extract_from_description(self, description: str) -> ExtractedTags:
        """
        Extract tags from a job description.

        Description has more signal but also more noise.
        Use slightly lower confidence than title.
        """
        if not description:
            return ExtractedTags()

        desc_lower = description.lower()
        result = ExtractedTags()

        # Domain tags
        for tag, patterns in self.DOMAIN_PATTERNS.items():
            match_count = 0
            for pattern in patterns:
                match_count += len(re.findall(pattern, desc_lower, re.IGNORECASE))
            if match_count >= 2:  # Require multiple mentions
                confidence = min(0.5 + (match_count * 0.1), 0.85)
                result.domain_tags.append((tag, confidence))

        # Tech tags (require at least 1 mention, prefer 2+)
        for tag, patterns in self.TECH_PATTERNS.items():
            match_count = 0
            for pattern in patterns:
                match_count += len(re.findall(pattern, desc_lower, re.IGNORECASE))
            if match_count >= 1:
                confidence = min(0.6 + (match_count * 0.1), 0.85)
                result.tech_tags.append((tag, confidence))

        # Function tags
        for tag, patterns in self.FUNCTION_PATTERNS.items():
            match_count = 0
            for pattern in patterns:
                match_count += len(re.findall(pattern, desc_lower, re.IGNORECASE))
            if match_count >= 1:
                confidence = min(0.5 + (match_count * 0.15), 0.8)
                result.function_tags.append((tag, confidence))

        return result

    def extract_from_company(self, company_name: str, industry: str = None) -> ExtractedTags:
        """
        Extract domain tags from company context.

        This is often the most reliable source for domain tags.
        """
        result = ExtractedTags()

        if industry:
            # Direct industry mapping
            industry_lower = industry.lower()
            for tag, patterns in self.DOMAIN_PATTERNS.items():
                for pattern in patterns:
                    if re.search(pattern, industry_lower, re.IGNORECASE):
                        result.domain_tags.append((tag, 0.95))
                        break

        if company_name:
            # Infer from company name (lower confidence)
            company_lower = company_name.lower()
            # Well-known company -> domain mappings could go here
            # e.g., "Stripe" -> Fintech, "Epic Games" -> Gaming

        return result

    def merge_tags(self, *tag_sets: ExtractedTags) -> ExtractedTags:
        """
        Merge multiple ExtractedTags, taking max confidence for duplicates.
        """
        merged = ExtractedTags()

        # Helper to merge a specific tag type
        def merge_tag_list(lists):
            combined = {}
            for tag_list in lists:
                for tag, confidence in tag_list:
                    if tag not in combined or confidence > combined[tag]:
                        combined[tag] = confidence
            return [(tag, conf) for tag, conf in combined.items()]

        merged.domain_tags = merge_tag_list([ts.domain_tags for ts in tag_sets])
        merged.tech_tags = merge_tag_list([ts.tech_tags for ts in tag_sets])
        merged.function_tags = merge_tag_list([ts.function_tags for ts in tag_sets])

        return merged

    def extract_all(
        self,
        title: str = None,
        description: str = None,
        company_name: str = None,
        industry: str = None,
        min_confidence: float = 0.5
    ) -> ExtractedTags:
        """
        Extract tags from all available sources and merge.

        Args:
            title: Job title
            description: Job description
            company_name: Company name
            industry: Company industry
            min_confidence: Minimum confidence threshold

        Returns:
            Merged ExtractedTags with tags above threshold
        """
        title_tags = self.extract_from_title(title) if title else ExtractedTags()
        desc_tags = self.extract_from_description(description) if description else ExtractedTags()
        company_tags = self.extract_from_company(company_name, industry) if company_name or industry else ExtractedTags()

        merged = self.merge_tags(title_tags, desc_tags, company_tags)

        # Filter by confidence threshold
        merged.domain_tags = [(t, c) for t, c in merged.domain_tags if c >= min_confidence]
        merged.tech_tags = [(t, c) for t, c in merged.tech_tags if c >= min_confidence]
        merged.function_tags = [(t, c) for t, c in merged.function_tags if c >= min_confidence]

        return merged


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == '__main__':
    extractor = TagExtractor()

    # Test with some example titles
    test_cases = [
        "Senior Backend Engineer, Payments",
        "Staff ML Platform Engineer",
        "Security Engineer - Cloud Infrastructure",
        "Frontend Engineer (React/TypeScript)",
        "Senior Data Engineer - Snowflake/Airflow",
        "DevOps Engineer - Kubernetes",
        "Growth Engineering Manager",
        "iOS Developer - Fintech",
    ]

    for title in test_cases:
        tags = extractor.extract_from_title(title)
        print(f"\n{title}")
        print(f"  Domain:   {tags.domain_tags}")
        print(f"  Tech:     {tags.tech_tags}")
        print(f"  Function: {tags.function_tags}")
