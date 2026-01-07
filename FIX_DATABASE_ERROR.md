# Fix Database Connection Error

You're seeing this error:
```
connection to server at "localhost" (::1), port 5432 failed: fe_sendauth: no password supplied
```

## Quick Fix: Use SQLite (Easiest - No Setup!)

**Just run this instead:**
```bash
python run_with_sqlite.py 10001 02139 90210
```

This uses SQLite instead of PostgreSQL - no database setup needed! The data will be saved to `jobs.db` in the current directory.

## Option 2: Set PostgreSQL Password

If you want to use PostgreSQL, you need to set the password:

### Method A: Environment Variable (Recommended)
```bash
export DB_PASSWORD=your_postgres_password
python data.scraper.josh.py --locations 10001
```

### Method B: Create config.json
```bash
cp config.example.json config.json
```

Then edit `config.json` and add:
```json
{
  "db_password": "your_postgres_password",
  ...
}
```

### Method C: Install/Setup PostgreSQL

If PostgreSQL isn't installed:

**macOS:**
```bash
brew install postgresql
brew services start postgresql

# Set password (if needed)
psql postgres
# Then in psql:
ALTER USER postgres PASSWORD 'your_password';
```

**Then set the password:**
```bash
export DB_PASSWORD=your_password
```

## Recommended: Use SQLite for Now

For testing and getting started, SQLite is much easier:

```bash
# Just run this - no setup needed!
python run_with_sqlite.py 10001
```

The data will be saved to `jobs.db`. You can query it with:
```bash
sqlite3 jobs.db "SELECT COUNT(*) FROM jobs;"
```

You can always migrate to PostgreSQL later if needed!
