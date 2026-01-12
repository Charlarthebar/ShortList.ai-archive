#!/bin/bash
# Quick database setup script

echo "=== Database Setup ==="
echo ""
echo "Choose your database option:"
echo "1. SQLite (easiest, no setup needed)"
echo "2. PostgreSQL (more powerful, requires installation)"
echo ""
read -p "Enter choice (1 or 2): " choice

if [ "$choice" == "1" ]; then
    echo ""
    echo "✓ SQLite selected - No setup needed!"
    echo "The scraper will automatically use SQLite if PostgreSQL is not available."
    echo ""
    echo "To use SQLite, just run:"
    echo "  python data.scraper.josh.py --locations 10001"
    echo ""
elif [ "$choice" == "2" ]; then
    echo ""
    echo "=== PostgreSQL Setup ==="
    echo ""

    # Check if PostgreSQL is installed
    if ! command -v psql &> /dev/null; then
        echo "PostgreSQL is not installed."
        echo ""
        echo "Install with Homebrew:"
        echo "  brew install postgresql"
        echo "  brew services start postgresql"
        echo ""
        echo "Or download from: https://www.postgresql.org/download/"
        exit 1
    fi

    echo "PostgreSQL is installed!"
    echo ""
    read -p "Enter database name (default: jobs_db): " db_name
    db_name=${db_name:-jobs_db}

    read -p "Enter database user (default: postgres): " db_user
    db_user=${db_user:-postgres}

    read -sp "Enter database password: " db_password
    echo ""

    # Create database
    echo "Creating database..."
    PGPASSWORD=$db_password psql -U $db_user -h localhost -c "CREATE DATABASE $db_name;" 2>/dev/null || echo "Database may already exist"

    echo ""
    echo "✓ Database created!"
    echo ""
    echo "Set these environment variables:"
    echo "  export DB_NAME=$db_name"
    echo "  export DB_USER=$db_user"
    echo "  export DB_PASSWORD=$db_password"
    echo ""
    echo "Or create config.json with these values."
else
    echo "Invalid choice"
    exit 1
fi
