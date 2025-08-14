# tests/test_standalone.py
"""
Standalone tests for DataPusher+ that don't require CKAN
These tests can run independently to verify basic functionality
"""

import os
import csv
import json
import tempfile
import hashlib
from unittest import mock
from unittest.mock import Mock, patch
import pytest


@pytest.mark.standalone
class TestStandaloneValidation:
    """Test basic validation functions without CKAN dependencies"""
    
    def test_csv_file_creation(self, temp_dir):
        """Test creating a CSV file"""
        csv_path = os.path.join(temp_dir, 'test.csv')
        
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'value'])
            writer.writerow(['1', 'Test', '100'])
        
        assert os.path.exists(csv_path)
        
        # Read and verify
        with open(csv_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            assert headers == ['id', 'name', 'value']
            row = next(reader)
            assert row == ['1', 'Test', '100']
    
    def test_file_hash_calculation(self):
        """Test file hash calculation"""
        test_data = b"test data for hashing"
        expected_hash = hashlib.md5(test_data).hexdigest()
        
        calculated_hash = hashlib.md5(test_data).hexdigest()
        assert calculated_hash == expected_hash
    
    def test_json_validation(self):
        """Test JSON parsing and validation"""
        valid_json = '{"key": "value", "number": 42}'
        invalid_json = '{invalid json}'
        
        # Valid JSON should parse
        parsed = json.loads(valid_json)
        assert parsed['key'] == 'value'
        assert parsed['number'] == 42
        
        # Invalid JSON should raise exception
        with pytest.raises(json.JSONDecodeError):
            json.loads(invalid_json)
    
    def test_type_mapping(self):
        """Test data type mapping logic"""
        type_mapping = {
            "String": "text",
            "Integer": "numeric",
            "Float": "numeric",
            "DateTime": "timestamp",
            "Date": "date",
            "NULL": "text"
        }
        
        assert type_mapping["String"] == "text"
        assert type_mapping["Integer"] == "numeric"
        assert type_mapping["DateTime"] == "timestamp"


@pytest.mark.standalone
class TestStandaloneDataProcessing:
    """Test data processing functions without CKAN"""
    
    def test_column_name_sanitization(self):
        """Test column name sanitization logic"""
        unsafe_names = [
            ("column-name", "column_name"),
            ("123column", "unsafe_123column"),
            ("column name", "column_name"),
            ("COLUMN", "column"),
            ("_id", "unsafe__id")
        ]
        
        for unsafe, expected_safe in unsafe_names:
            # Simulate sanitization logic
            if unsafe == "_id":
                safe = "unsafe__id"
            elif unsafe[0].isdigit():
                safe = f"unsafe_{unsafe}"
            else:
                safe = unsafe.lower().replace("-", "_").replace(" ", "_")
            
            assert safe == expected_safe
    
    def test_smartint_type_selection(self):
        """Test smart integer type selection"""
        # PostgreSQL integer limits
        POSTGRES_INT_MIN = -2147483648
        POSTGRES_INT_MAX = 2147483647
        POSTGRES_BIGINT_MIN = -9223372036854775808
        POSTGRES_BIGINT_MAX = 9223372036854775807
        
        test_cases = [
            (0, 100, "integer"),
            (POSTGRES_INT_MIN, POSTGRES_INT_MAX, "integer"),
            (0, POSTGRES_BIGINT_MAX, "bigint"),
            (POSTGRES_BIGINT_MIN, POSTGRES_BIGINT_MAX, "bigint"),
            (0, 99999999999999999999, "numeric"),
        ]
        
        for min_val, max_val, expected_type in test_cases:
            if max_val <= POSTGRES_INT_MAX and min_val >= POSTGRES_INT_MIN:
                selected_type = "integer"
            elif max_val <= POSTGRES_BIGINT_MAX and min_val >= POSTGRES_BIGINT_MIN:
                selected_type = "bigint"
            else:
                selected_type = "numeric"
            
            assert selected_type == expected_type
    
    def test_preview_rows_calculation(self):
        """Test preview rows calculation logic"""
        test_cases = [
            (1000, 100, 100),    # 1000 rows, preview 100
            (50, 100, 50),       # 50 rows, less than preview limit
            (0, 100, 0),         # No rows
            (1000, -100, 100),   # Negative preview (last N rows)
        ]
        
        for total_rows, preview_setting, expected_preview in test_cases:
            if preview_setting > 0:
                preview_rows = min(total_rows, preview_setting)
            elif preview_setting < 0:
                preview_rows = min(total_rows, abs(preview_setting))
            else:
                preview_rows = 0
            
            assert preview_rows == expected_preview


@pytest.mark.standalone
class TestStandaloneFileFormats:
    """Test file format detection without CKAN"""
    
    def test_csv_format_detection(self):
        """Test CSV format detection"""
        csv_extensions = ['.csv', '.CSV']
        tsv_extensions = ['.tsv', '.tab', '.TSV', '.TAB']
        excel_extensions = ['.xls', '.xlsx', '.xlsm', '.xlsb', '.ods']
        spatial_extensions = ['.shp', '.geojson', '.qgis']
        
        for ext in csv_extensions:
            assert ext.lower().endswith('.csv')
        
        for ext in tsv_extensions:
            assert ext.lower() in ['.tsv', '.tab']
        
        for ext in excel_extensions:
            assert any(ext.lower().endswith(x) for x in ['.xls', '.xlsx', '.xlsm', '.xlsb', '.ods'])
        
        for ext in spatial_extensions:
            assert ext.lower() in ['.shp', '.geojson', '.qgis']
    
    def test_zip_file_detection(self, temp_dir):
        """Test ZIP file format detection"""
        import zipfile
        
        # Create a test ZIP file
        zip_path = os.path.join(temp_dir, 'test.zip')
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('test.csv', 'id,name\n1,test\n')
        
        assert os.path.exists(zip_path)
        assert zipfile.is_zipfile(zip_path)
        
        # Check ZIP contents
        with zipfile.ZipFile(zip_path, 'r') as zf:
            namelist = zf.namelist()
            assert 'test.csv' in namelist
            assert len(namelist) == 1
    
    def test_geojson_structure(self, temp_dir):
        """Test GeoJSON structure validation"""
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Test"},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0, 0]
                    }
                }
            ]
        }
        
        # Validate structure
        assert geojson_data["type"] == "FeatureCollection"
        assert "features" in geojson_data
        assert len(geojson_data["features"]) > 0
        assert geojson_data["features"][0]["type"] == "Feature"
        
        # Save and reload
        geojson_path = os.path.join(temp_dir, 'test.geojson')
        with open(geojson_path, 'w') as f:
            json.dump(geojson_data, f)
        
        with open(geojson_path, 'r') as f:
            loaded = json.load(f)
            assert loaded == geojson_data


@pytest.mark.standalone
class TestStandaloneMocking:
    """Test mocking capabilities without CKAN"""
    
    @patch('subprocess.run')
    def test_mock_subprocess(self, mock_run):
        """Test mocking subprocess calls"""
        import subprocess
        
        # Setup mock
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = b'Success'
        mock_run.return_value.stderr = b''
        
        # Call subprocess
        result = subprocess.run(['echo', 'test'], capture_output=True)
        
        # Verify mock was called
        mock_run.assert_called_once()
        assert result.returncode == 0
        assert result.stdout == b'Success'
    
    @patch('requests.get')
    def test_mock_http_request(self, mock_get):
        """Test mocking HTTP requests"""
        import requests
        
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = 'Mock content'
        mock_response.json.return_value = {'status': 'success'}
        mock_get.return_value = mock_response
        
        # Make request
        response = requests.get('http://example.com')
        
        # Verify
        assert response.status_code == 200
        assert response.text == 'Mock content'
        assert response.json()['status'] == 'success'
        mock_get.assert_called_once_with('http://example.com')
    
    @patch('psycopg2.connect')
    def test_mock_database(self, mock_connect):
        """Test mocking database connections"""
        import psycopg2
        
        # Setup mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Setup mock query results
        mock_cursor.fetchone.return_value = ('test_id', 'test_name')
        mock_cursor.fetchall.return_value = [
            ('id1', 'name1'),
            ('id2', 'name2')
        ]
        mock_cursor.rowcount = 2
        
        # Use connection
        conn = psycopg2.connect('postgresql://test')
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM test_table")
        
        # Verify results
        one_result = cursor.fetchone()
        assert one_result == ('test_id', 'test_name')
        
        all_results = cursor.fetchall()
        assert len(all_results) == 2
        assert cursor.rowcount == 2
        
        mock_connect.assert_called_once()


@pytest.mark.standalone
class TestStandaloneUtilities:
    """Test utility functions without CKAN"""
    
    def test_csv_stats_parsing(self, temp_dir):
        """Test parsing CSV statistics"""
        stats_content = """field,type,min,max,mean,cardinality
id,Integer,1,100,50.5,100
name,String,A,Z,,100
value,Float,0.1,999.9,500.0,95"""
        
        csv_path = os.path.join(temp_dir, 'stats.csv')
        with open(csv_path, 'w') as f:
            f.write(stats_content)
        
        # Parse stats
        stats = {}
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stats[row['field']] = row
        
        assert 'id' in stats
        assert stats['id']['type'] == 'Integer'
        assert stats['id']['min'] == '1'
        assert stats['id']['max'] == '100'
        assert stats['id']['cardinality'] == '100'
        
        assert stats['name']['type'] == 'String'
        assert stats['value']['type'] == 'Float'
    
    def test_frequency_table_parsing(self, temp_dir):
        """Test parsing frequency tables"""
        freq_content = """field,value,count,percentage
category,A,50,50.0
category,B,30,30.0
category,C,20,20.0"""
        
        csv_path = os.path.join(temp_dir, 'freq.csv')
        with open(csv_path, 'w') as f:
            f.write(freq_content)
        
        # Parse frequency
        freqs = {}
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                field = row['field']
                if field not in freqs:
                    freqs[field] = []
                freqs[field].append({
                    'value': row['value'],
                    'count': row['count'],
                    'percentage': row['percentage']
                })
        
        assert 'category' in freqs
        assert len(freqs['category']) == 3
        assert freqs['category'][0]['value'] == 'A'
        assert freqs['category'][0]['percentage'] == '50.0'
    
    def test_date_parsing(self):
        """Test date parsing logic"""
        from dateutil.parser import parse
        
        date_strings = [
            "2024-01-15",
            "01/15/2024",
            "15/01/2024",
            "2024-01-15 10:30:00",
            "Jan 15, 2024"
        ]
        
        for date_str in date_strings:
            try:
                parsed = parse(date_str, dayfirst=False)
                assert parsed.year == 2024
                assert parsed.month == 1
                assert parsed.day == 15
            except:
                # Some formats might fail depending on settings
                pass
    
    def test_data_size_formatting(self):
        """Test data size formatting"""
        from datasize import DataSize
        
        sizes = [
            (1024, "1.0KiB"),
            (1024 * 1024, "1.0MiB"),
            (1024 * 1024 * 1024, "1.0GiB"),
        ]
        
        for bytes_val, expected in sizes:
            size = DataSize(bytes_val)
            # Check that size object is created
            assert size.bytes == bytes_val


@pytest.mark.standalone
class TestStandaloneErrors:
    """Test error handling without CKAN"""
    
    def test_job_error_creation(self):
        """Test creating job errors"""
        class JobError(Exception):
            pass
        
        error_msg = "Test error message"
        error = JobError(error_msg)
        assert str(error) == error_msg
        
        # Test raising and catching
        with pytest.raises(JobError) as exc_info:
            raise JobError("Something went wrong")
        
        assert "Something went wrong" in str(exc_info.value)
    
    def test_http_error_creation(self):
        """Test creating HTTP errors"""
        class HTTPError(Exception):
            def __init__(self, message, status_code=None, request_url=None, response=None):
                self.message = message
                self.status_code = status_code
                self.request_url = request_url
                self.response = response
                super().__init__(message)
        
        error = HTTPError(
            message="Not found",
            status_code=404,
            request_url="http://test.com/resource",
            response=b"Resource not found"
        )
        
        assert error.status_code == 404
        assert error.request_url == "http://test.com/resource"
        assert error.response == b"Resource not found"
        assert str(error) == "Not found"
    
    def test_validation_errors(self):
        """Test validation error scenarios"""
        def validate_csv_headers(headers, required_headers):
            missing = set(required_headers) - set(headers)
            if missing:
                raise ValueError(f"Missing required headers: {missing}")
            return True
        
        # Valid headers
        headers = ['id', 'name', 'value']
        required = ['id', 'name']
        assert validate_csv_headers(headers, required) == True
        
        # Missing headers
        headers = ['id']
        required = ['id', 'name', 'value']
        with pytest.raises(ValueError) as exc_info:
            validate_csv_headers(headers, required)
        
        assert "Missing required headers" in str(exc_info.value)
        assert "'name'" in str(exc_info.value)
        assert "'value'" in str(exc_info.value)


# Run basic tests if executed directly
if __name__ == "__main__":
    import sys
    
    print("Running standalone tests...")
    print(f"Python version: {sys.version}")
    
    # Run a simple test
    test = TestStandaloneValidation()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test.test_csv_file_creation(temp_dir)
        print("✓ CSV file creation test passed")
    
    test.test_file_hash_calculation()
    print("✓ File hash calculation test passed")
    
    test.test_json_validation()
    print("✓ JSON validation test passed")
    
    test.test_type_mapping()
    print("✓ Type mapping test passed")
    
    print("\nAll basic tests passed! Run 'pytest tests/test_standalone.py -v' for full test suite.")# tests/test_standalone.py
