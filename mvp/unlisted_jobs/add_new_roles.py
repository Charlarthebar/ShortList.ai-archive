#!/usr/bin/env python3
"""
Add New Canonical Roles to Database
====================================

Adds the 10 new roles from Phase 1 expansion.
"""

import os
import logging
from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set DB_USER
if 'DB_USER' not in os.environ:
    os.environ['DB_USER'] = 'noahhopkins'

# New roles to add
new_roles = [
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
]

def main():
    config = Config()
    db = DatabaseManager(config)
    db.initialize_pool()

    logger.info("Adding 10 new canonical roles to database...")
    
    added = 0
    skipped = 0
    
    for role in new_roles:
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
    logger.info("New total roles in database:")
    
    # Count roles
    conn = db.get_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM canonical_roles")
        count = cursor.fetchone()[0]
        logger.info(f"  {count} canonical roles")
    db.release_connection(conn)
    
    db.close_all_connections()

if __name__ == "__main__":
    main()
