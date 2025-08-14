# tests/conftest.py
"""
Pytest configuration and fixtures for DataPusher+ tests
This version can run standalone without full CKAN installation
"""

import os
import sys
import pytest
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path so we can import datapusher_plus
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Try to import CKAN test helpers, but make them optional
try:
    import ckan.tests.helpers as helpers
    import ckan.tests.factories as factories
    import ckan.model as model
    import ckan.plugins as plugins
    from ckan.common import config
    CKAN_AVAILABLE = True
except ImportError:
    # Create mock objects for CKAN when it's not available
    helpers = None
    factories = None
    model = None
    plugins = None
    config = {}
    CKAN_AVAILABLE = False

# Set up test environment variables
os.environ['TESTING'] = '1'
os.environ['CKAN_TESTING'] = '1'


# Mock CKAN modules if not available
if not CKAN_AVAILABLE:
    # Create a mock ckan module structure
    class MockHelpers:
        @staticmethod
        def call_action(name, context=None, **kwargs):
            return {"success": True}
        
        @staticmethod
        def _get_test_app():
            return Mock()
    
    class MockFactories:
        @staticmethod
        def User(**kwargs):
            return {"id": "test-user", "name": "testuser", **kwargs}
        
        @staticmethod
        def Sysadmin(**kwargs):
            return {"id": "admin-user", "name": "admin", **kwargs}
        
        @staticmethod
        def Organization(**kwargs):
            return {"id": "test-org", "name": "testorg", **kwargs}
        
        @staticmethod
        def Dataset(**kwargs):
            return {"id": "test-dataset", "name": "testdataset", **kwargs}
        
        @staticmethod
        def Resource(**kwargs):
            return {"id": "test-resource", "name": "testresource", **kwargs}
    
    class MockModel:
        repo = Mock()
        Session = Mock()
    
    class MockPlugins:
        @staticmethod
        def plugin_loaded(name):
            return False
        
        @staticmethod
        def load(name):
            pass
        
        @staticmethod
        def unload(name):
            pass
        
        @staticmethod
        def get_plugin(name):
            return Mock()
    
    helpers = MockHelpers()
    factories = MockFactories()
    model = MockModel()
    plugins = MockPlugins()


@pytest.fixture(scope='session')
def ckan_config(request):
    """Load CKAN configuration for tests (mocked if CKAN not available)"""
    if CKAN_AVAILABLE:
        # Use real CKAN config
        test_ini = os.environ.get('CKAN_INI', 'test.ini')
        if not os.path.exists(test_ini):
            test_ini = '/srv/app/src/ckan/test-core.ini'
        
        try:
            from paste.deploy import appconfig
            from ckan.config.environment import load_environment
            
            conf = appconfig(f'config:{test_ini}')
            load_environment(conf.global_conf, conf.local_conf)
            return config
        except:
            pass
    
    # Return mock config
    return {
        'ckan.site_url': 'http://test.ckan.net',
        'ckan.plugins': 'datastore datapusher_plus',
        'sqlalchemy.url': 'postgresql://ckan_default:pass@localhost/ckan_test'
    }


@pytest.fixture(scope='function')
def clean_db():
    """Clean database between tests (mocked if CKAN not available)"""
    if CKAN_AVAILABLE and model:
        model.repo.rebuild_db()
    else:
        # Just a mock cleanup
        pass


@pytest.fixture(scope='function')
def with_plugins():
    """Load DataPusher+ plugin for tests (mocked if CKAN not available)"""
    if CKAN_AVAILABLE:
        if not plugins.plugin_loaded('datastore'):
            plugins.load('datastore')
        if not plugins.plugin_loaded('datapusher_plus'):
            plugins.load('datapusher_plus')
        
        yield
        
        if plugins.plugin_loaded('datapusher_plus'):
            plugins.unload('datapusher_plus')
        if plugins.plugin_loaded('datastore'):
            plugins.unload('datastore')
    else:
        # Just mock
        yield


@pytest.fixture(scope='function')
def app():
    """Create a test Flask app (mocked if CKAN not available)"""
    if CKAN_AVAILABLE and helpers:
        return helpers._get_test_app()
    else:
        return Mock()


@pytest.fixture(scope='function')
def sysadmin_user():
    """Create a sysadmin user for tests"""
    if CKAN_AVAILABLE and factories:
        return factories.Sysadmin()
    else:
        return {"id": "admin-user", "name": "admin", "email": "admin@test.com"}


@pytest.fixture(scope='function')
def normal_user():
    """Create a normal user for tests"""
    if CKAN_AVAILABLE and factories:
        return factories.User()
    else:
        return {"id": "test-user", "name": "testuser", "email": "user@test.com"}


@pytest.fixture(scope='function')
def organization():
    """Create an organization for tests"""
    if CKAN_AVAILABLE and factories:
        return factories.Organization()
    else:
        return {"id": "test-org", "name": "testorg", "title": "Test Organization"}


@pytest.fixture(scope='function')
def dataset(organization):
    """Create a dataset for tests"""
    if CKAN_AVAILABLE and factories:
        return factories.Dataset(owner_org=organization['id'])
    else:
        return {
            "id": "test-dataset",
            "name": "testdataset",
            "title": "Test Dataset",
            "owner_org": organization['id']
        }


@pytest.fixture(scope='function')
def resource(dataset):
    """Create a resource for tests"""
    if CKAN_AVAILABLE and factories:
        return factories.Resource(
            package_id=dataset['id'],
            format='CSV',
            url='http://example.com/data.csv'
        )
    else:
        return {
            "id": "test-resource",
            "name": "testresource",
            "format": "CSV",
            "url": "http://example.com/data.csv",
            "package_id": dataset['id']
        }


@pytest.fixture(scope='function')
def temp_dir():
    """Create a temporary directory for tests"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope='function')
def sample_csv_file(temp_dir):
    """Create a sample CSV file for tests"""
    import csv
    
    csv_path = os.path.join(temp_dir, 'sample.csv')
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'value', 'date'])
        writer.writerow(['1', 'Item 1', '100.5', '2024-01-01'])
        writer.writerow(['2', 'Item 2', '200.75', '2024-01-02'])
        writer.writerow(['3', 'Item 3', '300.25', '2024-01-03'])
    
    return csv_path


@pytest.fixture(scope='function')
def sample_excel_file(temp_dir):
    """Create a sample Excel file for tests"""
    try:
        import openpyxl
        from openpyxl import Workbook
        
        excel_path = os.path.join(temp_dir, 'sample.xlsx')
        wb = Workbook()
        ws = wb.active
        ws.title = "Data"
        
        # Add headers
        ws.append(['id', 'name', 'value', 'date'])
        # Add data
        ws.append([1, 'Item 1', 100.5, '2024-01-01'])
        ws.append([2, 'Item 2', 200.75, '2024-01-02'])
        ws.append([3, 'Item 3', 300.25, '2024-01-03'])
        
        wb.save(excel_path)
        return excel_path
    except ImportError:
        # If openpyxl is not installed, create a mock file
        excel_path = os.path.join(temp_dir, 'sample.xlsx')
        with open(excel_path, 'wb') as f:
            f.write(b'Mock Excel content')
        return excel_path


@pytest.fixture(scope='function')
def sample_geojson_file(temp_dir):
    """Create a sample GeoJSON file for tests"""
    import json
    
    geojson_path = os.path.join(temp_dir, 'sample.geojson')
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "name": "Location 1",
                    "value": 100
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-122.4194, 37.7749]
                }
            }
        ]
    }
    
    with open(geojson_path, 'w') as f:
        json.dump(geojson_data, f)
    
    return geojson_path


@pytest.fixture(scope='function')
def mock_qsv_command():
    """Mock QSVCommand for tests"""
    with patch('ckanext.datapusher_plus.qsv_utils.QSVCommand') as mock_qsv_class:
        mock_qsv = Mock()
        mock_qsv_class.return_value = mock_qsv
        
        # Setup default mock responses
        mock_qsv.validate.return_value = None
        mock_qsv.index.return_value = None
        mock_qsv.count.return_value = Mock(stdout=b'10')
        mock_qsv.headers.return_value = Mock(stdout=b'id\nname\nvalue\ndate')
        mock_qsv.safenames.return_value = Mock(stdout=b'{"unsafe_headers": []}')
        mock_qsv.stats.return_value = None
        mock_qsv.frequency.return_value = None
        
        yield mock_qsv


@pytest.fixture(scope='function')
def mock_datastore():
    """Mock datastore operations"""
    with patch('ckanext.datapusher_plus.datastore_utils') as mock_dsu:
        mock_dsu.get_resource.return_value = {
            'id': 'test-resource',
            'package_id': 'test-package',
            'format': 'CSV',
            'url': 'http://example.com/data.csv'
        }
        mock_dsu.datastore_resource_exists.return_value = False
        mock_dsu.send_resource_to_datastore.return_value = None
        mock_dsu.update_resource.return_value = None
        mock_dsu.get_package.return_value = {
            'id': 'test-package',
            'name': 'test-package-name',
            'organization': {'name': 'test-org'}
        }
        
        yield mock_dsu


@pytest.fixture(scope='function')
def mock_postgres_connection():
    """Mock PostgreSQL connection"""
    with patch('psycopg2.connect') as mock_connect:
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 10
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []
        mock_connect.return_value = mock_connection
        
        yield mock_connection, mock_cursor


@pytest.fixture(scope='function')
def mock_job():
    """Mock RQ job"""
    with patch('ckanext.datapusher_plus.jobs.get_current_job') as mock_get_job:
        mock_job = Mock()
        mock_job.id = 'test-job-123'
        mock_job.is_started = True
        mock_job.is_finished = False
        mock_job.is_failed = False
        mock_get_job.return_value = mock_job
        
        yield mock_job


@pytest.fixture(scope='function')
def datapusher_config():
    """DataPusher+ configuration for tests"""
    config_values = {
        'QSV_BIN': '/usr/local/bin/qsvdp',
        'PREVIEW_ROWS': 100,
        'DOWNLOAD_TIMEOUT': 30,
        'MAX_CONTENT_LENGTH': 1256000000,
        'CHUNK_SIZE': 16384,
        'SSL_VERIFY': False,
        'USE_PROXY': False,
        'IGNORE_FILE_HASH': False,
        'DEFAULT_EXCEL_SHEET': 0,
        'SORT_AND_DUPE_CHECK': True,
        'DEDUP': False,
        'PII_SCREENING': False,
        'AUTO_INDEX_THRESHOLD': 3,
        'AUTO_INDEX_DATES': True,
        'AUTO_UNIQUE_INDEX': True,
        'AUTO_ALIAS': True,
        'PREFER_DMY': False,
        'TYPE_MAPPING': {
            "String": "text",
            "Integer": "numeric",
            "Float": "numeric",
            "DateTime": "timestamp",
            "Date": "date",
            "NULL": "text"
        }
    }
    
    with patch.multiple('ckanext.datapusher_plus.config', **config_values):
        yield config_values


@pytest.fixture(scope='function')
def mock_requests():
    """Mock requests library for HTTP calls"""
    with patch('ckanext.datapusher_plus.jobs.requests') as mock_req:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1000'}
        mock_response.iter_content = Mock(return_value=[b'test content'])
        mock_response.raise_for_status = Mock()
        mock_req.get.return_value.__enter__.return_value = mock_response
        mock_req.post.return_value = mock_response
        
        yield mock_req


# Helper functions for tests

def create_test_csv(path, rows=10, columns=None):
    """Helper to create a test CSV file"""
    import csv
    
    if columns is None:
        columns = ['id', 'name', 'value']
    
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for i in range(rows):
            row = [i] + [f'col_{j}_{i}' for j in range(1, len(columns))]
            writer.writerow(row)
    
    return path


# Pytest configuration

def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line(
        "markers", "ckan: mark test as requiring CKAN (skip if CKAN not available)"
    )
    config.addinivalue_line(
        "markers", "standalone: mark test as runnable without CKAN"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection"""
    skip_ckan = pytest.mark.skip(reason="CKAN not installed")
    
    for item in items:
        # Skip CKAN-dependent tests if CKAN is not available
        if "ckan" in item.keywords and not CKAN_AVAILABLE:
            item.add_marker(skip_ckan)
        
        # Skip slow tests unless explicitly requested
        if "slow" in item.keywords and not config.getoption("--runslow", False):
            skip_slow = pytest.mark.skip(reason="need --runslow option to run")
            item.add_marker(skip_slow)


def pytest_addoption(parser):
    """Add custom command line options"""
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--ckan-ini", action="store", default="test.ini", help="CKAN config file"
    )


# Print test environment info
print(f"DataPusher+ Test Configuration:")
print(f"  CKAN Available: {CKAN_AVAILABLE}")
print(f"  Test Mode: {'Full' if CKAN_AVAILABLE else 'Standalone (Mocked)'}")
if not CKAN_AVAILABLE:
    print("  Note: Running in standalone mode. Some integration tests will be skipped.")
    print("  For full testing, install CKAN and run tests within CKAN environment.")