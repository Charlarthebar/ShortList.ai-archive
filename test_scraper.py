#!/usr/bin/env python3
"""
Simple test script to verify the job scraper is working
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Try to import from job_database_builder (the renamed file)
try:
    from job_database_builder import JobDatabaseBuilder, Config
    print("✓ Successfully imported from job_database_builder.py")
except ImportError:
    # Fallback: try importing from data.scraper.josh
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("data_scraper", "data.scraper.josh.py")
        data_scraper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(data_scraper)
        JobDatabaseBuilder = data_scraper.JobDatabaseBuilder
        Config = data_scraper.Config
        print("✓ Successfully imported from data.scraper.josh.py")
    except Exception as e:
        print(f"✗ Error importing: {e}")
        sys.exit(1)

def test_basic_setup():
    """Test basic configuration and setup"""
    print("\n=== Testing Basic Setup ===")

    # Create a minimal config
    config = Config()

    print(f"Database: {config.db_name}")
    print(f"Max workers: {config.max_workers}")
    print(f"Adzuna enabled: {config.enable_adzuna}")
    print(f"USAJOBS enabled: {config.enable_usajobs}")
    print(f"Indeed enabled: {config.enable_indeed}")

    # Check if API keys are set
    if config.adzuna_app_id and config.adzuna_app_key:
        print("✓ Adzuna API keys configured")
    else:
        print("⚠ Adzuna API keys not set (set ADZUNA_APP_ID and ADZUNA_APP_KEY)")

    if config.usajobs_api_key and config.usajobs_email:
        print("✓ USAJOBS API keys configured")
    else:
        print("⚠ USAJOBS API keys not set (set USAJOBS_API_KEY and USAJOBS_EMAIL)")

    return config

def test_database_connection(config):
    """Test database connection"""
    print("\n=== Testing Database Connection ===")

    try:
        from job_database_builder import DatabaseManager
        db = DatabaseManager(config)
        db.connect()
        print("✓ Database connection successful")
        db.create_schema()
        print("✓ Database schema created/verified")
        db.close()
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        print("  Make sure PostgreSQL is running and credentials are correct")
        return False

def main():
    """Run tests"""
    print("Job Database Builder - Test Script")
    print("=" * 50)

    # Test 1: Basic setup
    config = test_basic_setup()

    # Test 2: Database connection
    db_ok = test_database_connection(config)

    # Summary
    print("\n=== Summary ===")
    if db_ok:
        print("✓ All tests passed! You can now run:")
        print("  python job_database_builder.py --locations 10001")
    else:
        print("⚠ Database setup needed. See QUICK_START.md for instructions.")

if __name__ == "__main__":
    main()
