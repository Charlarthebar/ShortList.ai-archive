#!/bin/bash
# Run ShortList UI locally
# Usage: ./run-ui.sh

set -e

cd "$(dirname "$0")"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting ShortList UI...${NC}"

# Check if node_modules exists
if [ ! -d "ui/shortlist-app/node_modules" ]; then
    echo -e "${YELLOW}Installing dependencies...${NC}"
    cd ui/shortlist-app
    npm install
    cd ../..
fi

# Start the React app
cd ui/shortlist-app
echo -e "${GREEN}UI will be available at: http://localhost:3000${NC}"
echo -e "${YELLOW}Note: Make sure the API is running on port 5001${NC}"
echo ""
npm start
