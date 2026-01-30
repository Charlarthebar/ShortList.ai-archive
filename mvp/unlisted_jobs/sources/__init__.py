"""
Data Source Connectors
======================

Connectors for various job data sources organized by reliability tier.

Tier A (High Reliability - 0.85-0.95):
- H-1B visa data (DOL LCA filings)
- PERM visa data (DOL PERM filings)
- Government payroll (federal, state, city)
- NPI Registry (healthcare providers)

Tier B (Medium Reliability - 0.70-0.85):
- Nonprofit 990 data (ProPublica)
- Job board scrapers
- ATS feeds

Tier C (Macro Priors - validation only):
- BLS OEWS (occupation estimates)
- BLS QCEW (employer counts)
"""

# Base classes
from .base import (
    BaseConnector,
    GovernmentPayrollConnector,
    LicensedProfessionalConnector,
    NonprofitConnector,
)

# Tier A - High Reliability
from .h1b_visa import H1BVisaConnector
from .perm_visa import PERMVisaConnector
from .ma_state_payroll import MAStatePayrollConnector
from .healthcare_npi import NPIRegistryConnector
from .federal_opm import FederalOPMConnector
from .city_payroll import BostonPayrollConnector, CambridgePayrollConnector

# Tier B - Medium Reliability
from .nonprofit_990 import ProPublica990Connector

# Tier C - Macro/Aggregate Data
from .bls_oews import BLSOEWSConnector

# All available connectors
__all__ = [
    # Base classes
    'BaseConnector',
    'GovernmentPayrollConnector',
    'LicensedProfessionalConnector',
    'NonprofitConnector',
    # Tier A
    'H1BVisaConnector',
    'PERMVisaConnector',
    'MAStatePayrollConnector',
    'NPIRegistryConnector',
    'FederalOPMConnector',
    'BostonPayrollConnector',
    'CambridgePayrollConnector',
    # Tier B
    'ProPublica990Connector',
    # Tier C
    'BLSOEWSConnector',
]
