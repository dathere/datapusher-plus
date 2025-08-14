# DataPusher+ Test Suite

This directory contains the comprehensive test suite for the DataPusher+ CKAN extension.

## Overview

The test suite is designed to ensure all functionality of DataPusher+ works correctly, from file upload and processing through to data storage in PostgreSQL. The tests are automatically run via GitHub Actions on every push/pull request.

## Test Structure

```
tests/
├── conftest.py           # Pytest configuration and shared fixtures
├── requirements-test.txt # Test dependencies
├── test_jobs.py         # Core job processing tests
├── test_qsv_utils.py    # QSV command wrapper tests
├── test_plugin.py       # CKAN plugin interface tests
├── test_helpers.py      # Helper function tests
├── test_integration.py  # End-to-end integration tests
└── static/              # Test data files
```

## Test Categories

### 1. Unit Tests (`test_jobs.py`)
- Input validation
- File format detection
- Data type inference
- Error handling
- File hash calculation

### 2. QSV Utils Tests (`test_qsv_utils.py`)
- CSV validation
- Statistics generation
- Data deduplication
- Date formatting
- Spatial file conversion
- Excel to CSV conversion

### 3. Plugin Tests (`test_plugin.py`)
- CKAN interface implementation
- Action registration
- Authentication functions
- Helper functions
- Resource hooks

### 4. Helper Tests (`test_helpers.py`)
- Job management functions
- ZIP file handling
- Data dictionary creation
- Column name sanitization
- Resource metadata updates

### 5. Integration Tests (`test_integration.py`)
- Complete CSV upload workflow
- Excel file processing
- Spatial file handling
- PII screening workflow
- Formula processing (DRUF)
- Auto-aliasing
- Preview generation
- Deduplication
- Auto-indexing

## Running Tests Locally

### Prerequisites

1. Install test dependencies:
```bash
pip install -r tests/requirements-test.txt
```

2. Install qsv binary:
```bash
wget https://github.com/dathere/qsv/releases/download/4.0.0/qsv-4.0.0-x86_64-unknown-linux-gnu.zip
unzip qsv-4.0.0-x86_64-unknown-linux-gnu.zip
sudo mv qsv* /usr/local/bin/
```

3. Set up test database:
```bash
createdb -U postgres ckan_test
createdb -U postgres datastore_test
```

### Running All Tests

```bash
pytest tests/ -v
```

### Running Specific Test Categories

```bash
# Run only unit tests
pytest tests/test_jobs.py -v

# Run only integration tests
pytest tests/test_integration.py -v

# Run tests with coverage
pytest tests/ --cov=ckanext.datapusher_plus --cov-report=html

# Run tests in parallel (faster)
pytest tests/ -n auto

# Run with specific CKAN config
pytest tests/ --ckan-ini=/path/to/test.ini
```

### Running with Markers

```bash
# Run only DataPusher+ specific tests
pytest -m datapusher_plus

# Run only integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"

# Run slow tests explicitly
pytest --runslow
```

## GitHub Actions CI/CD

The test suite runs automatically on GitHub Actions with the following matrix:
- CKAN versions: 2.10, 2.11
- Python versions: 3.10, 3.11

The workflow includes:
1. Environment setup (PostgreSQL, Redis, Solr)
2. qsv binary installation
3. CKAN and extension configuration
4. Test execution with coverage
5. Performance benchmarks
6. Security scanning

## Writing New Tests

### Basic Test Structure

```python
import pytest
from unittest.mock import Mock, patch

class TestNewFeature:
    """Test new feature functionality"""
    
    @pytest.fixture
    def setup_data(self):
        """Setup test data"""
        return {"test": "data"}
    
    def test_feature_success(self, setup_data):
        """Test successful feature execution"""
        result = my_function(setup_data)
        assert result == expected_value
    
    @patch('ckanext.datapusher_plus.module.external_function')
    def test_feature_with_mock(self, mock_func):
        """Test feature with mocked dependencies"""
        mock_func.return_value = "mocked_value"
        result = my_function()
        assert result == "expected"
        mock_func.assert_called_once()
```

### Using Fixtures

Common fixtures are available in `conftest.py`:
- `temp_dir`: Temporary directory for file operations
- `sample_csv_file`: Pre-created CSV file
- `mock_qsv_command`: Mocked QSV commands
- `mock_datastore`: Mocked datastore operations
- `mock_postgres_connection`: Mocked PostgreSQL connection

## Test Data

Test data files are stored in `tests/static/`:
- `simple.csv`: Basic CSV for testing
- `simple.xls/xlsx`: Excel files for conversion tests
- `simple.tsv`: Tab-separated values
- `weird_head_padding.csv`: Edge case CSV formats

## Coverage Reports

After running tests with coverage:
```bash
pytest tests/ --cov=ckanext.datapusher_plus --cov-report=html
open htmlcov/index.html
```

Target coverage: >80% for core modules

## Troubleshooting

### Common Issues

1. **qsv binary not found**
   - Ensure qsv is installed in `/usr/local/bin/qsvdp`
   - Check PATH environment variable

2. **Database connection errors**
   - Verify PostgreSQL is running
   - Check database permissions
   - Ensure test databases exist

3. **Import errors**
   - Install all requirements: `pip install -r requirements.txt -r tests/requirements-test.txt`
   - Ensure CKAN is properly installed

4. **Test timeouts**
   - Increase timeout: `pytest --timeout=300`
   - Check for infinite loops or blocking operations

### Debug Mode

Run tests with detailed output:
```bash
pytest tests/ -vvs --log-cli-level=DEBUG
```

## Contributing

When adding new features to DataPusher+:
1. Write tests FIRST (TDD approach)
2. Ensure all tests pass
3. Add integration tests for complex workflows
4. Update this README if adding new test categories
5. Maintain >80% code coverage

## Performance Testing

Run performance benchmarks:
```bash
pytest tests/test_integration.py::TestDataPusherPlusPerformance -v --benchmark-only
```

## Security Testing

Run security scans:
```bash
# Check for vulnerabilities
safety check

# Static security analysis
bandit -r ckanext/datapusher_plus

# Check for secrets
detect-secrets scan
```

## Contact

For questions about the test suite, please open an issue on GitHub.