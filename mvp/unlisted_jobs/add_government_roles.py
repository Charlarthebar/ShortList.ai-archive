#!/usr/bin/env python3
"""
Add Government-Specific Canonical Roles
========================================

Adds roles common in state/government payroll data to improve match rates.
Based on analysis of MO, IA, DC payroll data.
"""

import os
import logging
from database import DatabaseManager, Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if 'DB_USER' not in os.environ:
    os.environ['DB_USER'] = 'noahhopkins'

# New government-focused roles to add
government_roles = [
    # Direct Care & Support roles (high frequency in state payroll)
    {
        'name': 'Direct Care Worker',
        'soc_code': '31-1120',
        'onet_code': '31-1120.00',
        'role_family': 'Healthcare Support',
        'category': 'Healthcare Support',
        'description': 'Provide direct care and support to individuals in residential or healthcare settings.',
        'typical_skills': ['Patient Care', 'Personal Care', 'Safety Monitoring', 'Documentation']
    },
    {
        'name': 'Maintenance Worker',
        'soc_code': '49-9071',
        'onet_code': '49-9071.00',
        'role_family': 'Installation & Maintenance',
        'category': 'Installation, Maintenance, and Repair',
        'description': 'Perform general maintenance and repair tasks on buildings and equipment.',
        'typical_skills': ['Building Maintenance', 'Repairs', 'Equipment Operation', 'HVAC']
    },
    {
        'name': 'Benefits Specialist',
        'soc_code': '13-1141',
        'onet_code': '13-1141.00',
        'role_family': 'Administrative',
        'category': 'Business and Financial Operations',
        'description': 'Administer employee benefits programs or public assistance programs.',
        'typical_skills': ['Benefits Administration', 'Eligibility Determination', 'Customer Service', 'Data Entry']
    },
    {
        'name': 'Youth Services Worker',
        'soc_code': '21-1093',
        'onet_code': '21-1093.00',
        'role_family': 'Social Service',
        'category': 'Community and Social Service',
        'description': 'Work with at-risk youth in residential, correctional, or community settings.',
        'typical_skills': ['Youth Development', 'Counseling', 'Crisis Intervention', 'Case Management']
    },
    {
        'name': 'Program Specialist',
        'soc_code': '13-1199',
        'onet_code': '13-1199.00',
        'role_family': 'Administrative',
        'category': 'Business and Financial Operations',
        'description': 'Coordinate and implement specific programs or initiatives.',
        'typical_skills': ['Program Management', 'Analysis', 'Communication', 'Reporting']
    },
    {
        'name': 'Motor Vehicle Operator',
        'soc_code': '53-3099',
        'onet_code': '53-3099.00',
        'role_family': 'Transportation',
        'category': 'Transportation and Material Moving',
        'description': 'Operate motor vehicles to transport people or materials.',
        'typical_skills': ['Vehicle Operation', 'Safety', 'Route Navigation', 'Vehicle Inspection']
    },
    {
        'name': 'Security Officer',
        'soc_code': '33-9032',
        'onet_code': '33-9032.00',
        'role_family': 'Protective Service',
        'category': 'Protective Service',
        'description': 'Guard, patrol, and monitor premises to prevent theft and violence.',
        'typical_skills': ['Security', 'Surveillance', 'Access Control', 'Emergency Response']
    },
    {
        'name': 'Parole Officer',
        'soc_code': '21-1092',
        'onet_code': '21-1092.00',
        'role_family': 'Social Service',
        'category': 'Community and Social Service',
        'description': 'Monitor and supervise individuals on probation or parole.',
        'typical_skills': ['Case Management', 'Supervision', 'Risk Assessment', 'Report Writing']
    },
    {
        'name': 'Eligibility Worker',
        'soc_code': '43-4061',
        'onet_code': '43-4061.00',
        'role_family': 'Administrative',
        'category': 'Office and Administrative Support',
        'description': 'Determine eligibility for government assistance programs.',
        'typical_skills': ['Eligibility Determination', 'Interviewing', 'Documentation', 'Customer Service']
    },
    {
        'name': 'Public Defender',
        'soc_code': '23-1011',
        'onet_code': '23-1011.00',
        'role_family': 'Legal',
        'category': 'Legal',
        'description': 'Provide legal representation to defendants who cannot afford an attorney.',
        'typical_skills': ['Criminal Law', 'Litigation', 'Client Advocacy', 'Legal Research']
    },
    {
        'name': 'Emergency Dispatcher',
        'soc_code': '43-5031',
        'onet_code': '43-5031.00',
        'role_family': 'Protective Service',
        'category': 'Office and Administrative Support',
        'description': 'Operate communication equipment to dispatch emergency services.',
        'typical_skills': ['Emergency Response', 'Communication', 'Multi-tasking', 'CAD Systems']
    },
    {
        'name': 'EMT/Paramedic',
        'soc_code': '29-2040',
        'onet_code': '29-2040.00',
        'role_family': 'Healthcare',
        'category': 'Healthcare Practitioners',
        'description': 'Provide emergency medical care and transportation.',
        'typical_skills': ['Emergency Medical Care', 'Patient Assessment', 'Life Support', 'Medical Equipment']
    },
    {
        'name': 'Equipment Operator',
        'soc_code': '53-7032',
        'onet_code': '53-7032.00',
        'role_family': 'Construction & Extraction',
        'category': 'Construction and Extraction',
        'description': 'Operate heavy equipment for construction or maintenance.',
        'typical_skills': ['Heavy Equipment', 'Safety', 'Maintenance', 'Construction']
    },
    {
        'name': 'Dietary Aide',
        'soc_code': '35-2021',
        'onet_code': '35-2021.00',
        'role_family': 'Food Service',
        'category': 'Food Preparation and Serving',
        'description': 'Assist in food preparation and service in healthcare or institutional settings.',
        'typical_skills': ['Food Preparation', 'Food Safety', 'Dietary Restrictions', 'Customer Service']
    },
    {
        'name': 'Trades Worker',
        'soc_code': '47-2061',
        'onet_code': '47-2061.00',
        'role_family': 'Construction',
        'category': 'Construction and Extraction',
        'description': 'Perform skilled trades work including carpentry, plumbing, electrical.',
        'typical_skills': ['Skilled Trades', 'Construction', 'Repair', 'Blueprint Reading']
    },
    {
        'name': 'Adjunct Faculty',
        'soc_code': '25-1099',
        'onet_code': '25-1099.00',
        'role_family': 'Education',
        'category': 'Educational Instruction and Library',
        'description': 'Part-time faculty member at a college or university.',
        'typical_skills': ['Teaching', 'Subject Expertise', 'Curriculum Development', 'Student Assessment']
    },
]


def main():
    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    logger.info("Adding government-specific canonical roles to database...")

    added = 0
    skipped = 0

    for role in government_roles:
        try:
            role_id = db.insert_canonical_role(**role)
            logger.info(f"✓ Added: {role['name']} (id={role_id})")
            added += 1
        except Exception as e:
            if 'already exists' in str(e).lower() or 'duplicate' in str(e).lower():
                logger.info(f"- Skipped: {role['name']} (already exists)")
                skipped += 1
            else:
                logger.error(f"✗ Failed to add {role['name']}: {e}")

    logger.info("")
    logger.info(f"Summary: {added} added, {skipped} skipped")
    logger.info("")

    # Count roles
    conn = db.get_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM canonical_roles")
        count = cursor.fetchone()[0]
        logger.info(f"New total: {count} canonical roles")
    db.release_connection(conn)

    db.close_all_connections()


if __name__ == "__main__":
    main()
