#!/bin/bash

# PostgreSQL Stock Database Setup Script
# This script creates the stocks database and all tables

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCHEMA_FILE="${SCRIPT_DIR}/schema.sql"

echo -e "${YELLOW}PostgreSQL Stock Database Setup${NC}"
echo "=================================="
echo ""

# Check if schema file exists
if [ ! -f "${SCHEMA_FILE}" ]; then
    echo -e "${RED}Error: Schema file not found at ${SCHEMA_FILE}${NC}"
    exit 1
fi

# Check if PostgreSQL is accessible
if ! psql -d postgres -c '\q' 2>/dev/null; then
    echo -e "${RED}Error: Cannot connect to PostgreSQL server${NC}"
    echo "Please ensure PostgreSQL is running and you have access."
    exit 1
fi

# Check if database already exists
if psql -d postgres -lqt | cut -d \| -f 1 | grep -qw stocks; then
    echo -e "${YELLOW}Warning: Database 'stocks' already exists${NC}"
    read -p "Do you want to drop and recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Dropping existing database..."
        psql -d postgres -c "DROP DATABASE stocks;" 2>/dev/null
    else
        echo -e "${YELLOW}Aborting setup. Database not modified.${NC}"
        exit 0
    fi
fi

# Create the database
echo "Creating database..."
if ! psql -d postgres -c "CREATE DATABASE stocks;"; then
    echo -e "${RED}✗ Error creating database${NC}"
    exit 1
fi

# Execute the schema file against the stocks database
echo "Creating tables..."
if psql -d stocks -f "${SCHEMA_FILE}"; then
    echo ""
    echo -e "${GREEN}✓ Database 'stocks' created successfully!${NC}"
    echo ""
    echo "Tables created:"
    echo "  - stock_markets"
    echo "  - companies"
    echo "  - stock_listings"
    echo "  - portfolio"
    echo ""
    echo "You can now connect to the database with:"
    echo "  psql -d stocks"
    exit 0
else
    echo ""
    echo -e "${RED}✗ Error creating database${NC}"
    exit 1
fi

