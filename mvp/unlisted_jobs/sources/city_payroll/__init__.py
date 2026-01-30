"""
City Payroll Connectors
========================

Connectors for city/municipal employee payroll data.

Supported Cities:
- Boston, MA (~20,000 employees)
- Cambridge, MA (~5,000 employees)

Both use Socrata Open Data APIs (SODA) - free, no auth required.
"""

from .boston import BostonPayrollConnector
from .cambridge import CambridgePayrollConnector

__all__ = ['BostonPayrollConnector', 'CambridgePayrollConnector']
