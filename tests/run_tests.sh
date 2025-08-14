#!/bin/bash
# run_tests.sh - Test runner for DataPusher+

echo "================================================"
echo "DataPusher+ Test Suite Runner"
echo "================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if we're in a virtual environment
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo -e "${GREEN}✓ Virtual environment detected: $VIRTUAL_ENV${NC}"
else
    echo -e "${YELLOW}⚠ No virtual environment detected. Consider using one.${NC}"
fi

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check for Python
if command_exists python3; then
    PYTHON=python3
elif command_exists python; then
    PYTHON=python
else
    echo -e "${RED}✗ Python not found. Please install Python 3.8+${NC}"
    exit 1
fi

echo "Using Python: $($PYTHON --version)"

# Check for pytest
if ! $PYTHON -m pytest --version >/dev/null 2>&1; then
    echo -e "${YELLOW}pytest not found. Installing test requirements...${NC}"
    $PYTHON -m pip install pytest pytest-mock pytest-cov
fi

# Check if CKAN is available
$PYTHON -c "import ckan" 2>/dev/null
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ CKAN detected - Running full test suite${NC}"
    TEST_MODE="full"
else
    echo -e "${YELLOW}⚠ CKAN not detected - Running standalone tests only${NC}"
    TEST_MODE="standalone"
fi

# Check for qsv binary
if command_exists qsvdp; then
    echo -e "${GREEN}✓ qsv binary found at: $(which qsvdp)${NC}"
elif command_exists qsv; then
    echo -e "${YELLOW}⚠ qsv found but qsvdp not found. Some tests may fail.${NC}"
else
    echo -e "${YELLOW}⚠ qsv not found. Download from: https://github.com/dathere/qsv/releases${NC}"
fi

echo ""
echo "Select test option:"
echo "1) Run all tests"
echo "2) Run standalone tests only (no CKAN required)"
echo "3) Run specific test file"
echo "4) Run with coverage report"
echo "5) Run with verbose output"
echo "6) Install test dependencies"
echo "7) Exit"

read -p "Enter option (1-7): " option

case $option in
    1)
        echo "Running all tests..."
        if [ "$TEST_MODE" = "full" ]; then
            $PYTHON -m pytest tests/ -v
        else
            $PYTHON -m pytest tests/ -v -m "not ckan"
        fi
        ;;
    2)
        echo "Running standalone tests only..."
        $PYTHON -m pytest tests/ -v -m "standalone or not ckan"
        ;;
    3)
        echo "Available test files:"
        ls tests/test_*.py 2>/dev/null | sed 's/tests\//  - /'
        read -p "Enter test file name (e.g., test_jobs.py): " testfile
        if [ -f "tests/$testfile" ]; then
            $PYTHON -m pytest "tests/$testfile" -v
        else
            echo -e "${RED}Test file not found: tests/$testfile${NC}"
        fi
        ;;
    4)
        echo "Running tests with coverage..."
        if [ "$TEST_MODE" = "full" ]; then
            $PYTHON -m pytest tests/ --cov=ckanext.datapusher_plus --cov-report=html --cov-report=term
        else
            $PYTHON -m pytest tests/ -m "not ckan" --cov=ckanext.datapusher_plus --cov-report=html --cov-report=term
        fi
        echo -e "${GREEN}Coverage report generated in htmlcov/index.html${NC}"
        ;;
    5)
        echo "Running tests with verbose output..."
        if [ "$TEST_MODE" = "full" ]; then
            $PYTHON -m pytest tests/ -vvs --log-cli-level=DEBUG
        else
            $PYTHON -m pytest tests/ -vvs -m "not ckan" --log-cli-level=DEBUG
        fi
        ;;
    6)
        echo "Installing test dependencies..."
        if [ -f "tests/requirements-test.txt" ]; then
            $PYTHON -m pip install -r tests/requirements-test.txt
        else
            # Install basic test requirements
            $PYTHON -m pip install pytest pytest-mock pytest-cov factory-boy faker
        fi
        echo -e "${GREEN}✓ Test dependencies installed${NC}"
        ;;
    7)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo -e "${RED}Invalid option${NC}"
        exit 1
        ;;
esac

# Check test results
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Tests completed successfully${NC}"
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi