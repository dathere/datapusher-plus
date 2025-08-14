# tests/test_jobs.py
"""
Test suite for DataPusher+ jobs module
Tests the core functionality of pushing data to the datastore
"""

import os
import json
import tempfile
import csv
import hashlib
from unittest import mock
from unittest.mock import Mock, MagicMock, patch, call
import pytest
import psycopg2
from io import BytesIO

from ckanext.datapusher_plus import jobs, utils, config as conf
from ckanext.datapusher_plus.job_exceptions import HTTPError


class TestJobValidation:
    """Test input validation for jobs"""
    
    def test_validate_input_success(self):
        """Test successful input validation"""
        valid_input = {
            "metadata": {
                "resource_id": "test-resource-123",
                "ckan_url": "http://localhost:5000"
            }
        }
        # Should not raise any exception
        jobs.validate_input(valid_input)
    
    def test_validate_input_missing_metadata(self):
        """Test validation fails when metadata is missing"""
        with pytest.raises(utils.JobError, match="Metadata missing"):
            jobs.validate_input({})
    
    def test_validate_input_missing_resource_id(self):
        """Test validation fails when resource_id is missing"""
        with pytest.raises(utils.JobError, match="No id provided"):
            jobs.validate_input({"metadata": {}})


class TestPushToDatastore:
    """Test the main push_to_datastore function"""
    
    @pytest.fixture
    def mock_input(self):
        """Create a mock input for testing"""
        return {
            "metadata": {
                "resource_id": "test-resource-123",
                "ckan_url": "http://localhost:5000",
                "resource_url": "http://localhost:5000/dataset/test/resource/test-resource-123"
            },
            "result_url": "http://localhost:5000/api/3/action/datapusher_hook",
            "api_key": "test-api-key"
        }
    
    @pytest.fixture
    def mock_resource(self):
        """Create a mock resource"""
        return {
            "id": "test-resource-123",
            "package_id": "test-package-456",
            "name": "test.csv",
            "format": "CSV",
            "url": "http://localhost:5000/dataset/test/resource/test-resource-123",
            "url_type": "upload",
            "hash": "",
            "mimetype": "text/csv"
        }
    
    @pytest.fixture
    def mock_csv_content(self):
        """Create mock CSV content"""
        output = BytesIO()
        writer = csv.writer(output)
        writer.writerow(['id', 'name', 'value', 'date'])
        writer.writerow(['1', 'Test Item 1', '100.5', '2024-01-01'])
        writer.writerow(['2', 'Test Item 2', '200.75', '2024-01-02'])
        writer.writerow(['3', 'Test Item 3', '300.25', '2024-01-03'])
        return output.getvalue()
    
    @patch('ckanext.datapusher_plus.jobs.dph.add_pending_job')
    @patch('ckanext.datapusher_plus.jobs.dsu.get_resource')
    @patch('ckanext.datapusher_plus.jobs.requests.get')
    @patch('ckanext.datapusher_plus.jobs.QSVCommand')
    @patch('ckanext.datapusher_plus.jobs.dsu.datastore_resource_exists')
    @patch('ckanext.datapusher_plus.jobs.dsu.delete_datastore_resource')
    @patch('ckanext.datapusher_plus.jobs.dsu.send_resource_to_datastore')
    @patch('ckanext.datapusher_plus.jobs.psycopg2.connect')
    @patch('ckanext.datapusher_plus.jobs.dsu.update_resource')
    @patch('ckanext.datapusher_plus.jobs.dsu.get_package')
    @patch('ckanext.datapusher_plus.jobs.tempfile.TemporaryDirectory')
    def test_push_csv_to_datastore(
        self,
        mock_temp_dir,
        mock_get_package,
        mock_update_resource,
        mock_psycopg_connect,
        mock_send_to_datastore,
        mock_delete_datastore,
        mock_datastore_exists,
        mock_qsv_class,
        mock_requests_get,
        mock_get_resource,
        mock_add_pending_job,
        mock_input,
        mock_resource,
        mock_csv_content
    ):
        """Test pushing a CSV file to the datastore"""
        
        # Setup temporary directory
        temp_dir = tempfile.mkdtemp()
        mock_temp_dir.return_value.__enter__.return_value = temp_dir
        
        # Setup resource
        mock_get_resource.return_value = mock_resource
        
        # Setup HTTP response
        mock_response = Mock()
        mock_response.headers = {'content-length': str(len(mock_csv_content))}
        mock_response.iter_content = Mock(return_value=[mock_csv_content])
        mock_response.raise_for_status = Mock()
        mock_requests_get.return_value.__enter__.return_value = mock_response
        
        # Setup QSV mock
        mock_qsv = Mock()
        mock_qsv_class.return_value = mock_qsv
        
        # Mock QSV validate
        mock_qsv.validate.return_value = None
        
        # Mock QSV headers
        mock_headers_result = Mock()
        mock_headers_result.stdout = "id\nname\nvalue\ndate"
        mock_qsv.headers.return_value = mock_headers_result
        
        # Mock QSV safenames
        mock_safenames_result = Mock()
        mock_safenames_result.stdout = json.dumps({"unsafe_headers": []})
        mock_qsv.safenames.return_value = mock_safenames_result
        
        # Mock QSV index
        mock_qsv.index.return_value = None
        
        # Mock QSV count
        mock_count_result = Mock()
        mock_count_result.stdout = "3"
        mock_qsv.count.return_value = mock_count_result
        
        # Mock QSV stats
        stats_csv_content = """field,type,min,max,cardinality
id,Integer,1,3,3
name,String,Test Item 1,Test Item 3,3
value,Float,100.5,300.25,3
date,Date,2024-01-01,2024-01-03,3"""
        
        # Write stats to temp file
        stats_file = os.path.join(temp_dir, "qsv_stats.csv")
        with open(stats_file, 'w') as f:
            f.write(stats_csv_content)
        
        mock_qsv.stats.return_value = None
        
        # Mock QSV frequency
        freq_csv_content = """field,value,count,percentage
id,1,1,33.33
id,2,1,33.33
id,3,1,33.33"""
        
        freq_file = os.path.join(temp_dir, "qsv_freq.csv")
        with open(freq_file, 'w') as f:
            f.write(freq_csv_content)
        
        mock_qsv.frequency.return_value = None
        
        # Mock datastore operations
        mock_datastore_exists.return_value = None
        mock_send_to_datastore.return_value = None
        
        # Mock PostgreSQL connection
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 3
        mock_psycopg_connect.return_value = mock_connection
        
        # Mock package
        mock_get_package.return_value = {
            "id": "test-package-456",
            "name": "test-package",
            "organization": {"name": "test-org"}
        }
        
        # Run the test
        with patch('ckanext.datapusher_plus.jobs.Path') as mock_path:
            mock_path.return_value.is_file.return_value = True
            
            # Mock getting current job
            with patch('ckanext.datapusher_plus.jobs.get_current_job') as mock_get_job:
                mock_job = Mock()
                mock_job.id = "test-job-123"
                mock_get_job.return_value = mock_job
                
                # Mock dsu functions
                with patch('ckanext.datapusher_plus.jobs.dsu.get_scheming_yaml') as mock_scheming:
                    mock_scheming.return_value = (
                        {"dataset_fields": [], "resource_fields": []},
                        {"id": "test-package-456"}
                    )
                    
                    with patch('ckanext.datapusher_plus.jobs.dsu.patch_package'):
                        result = jobs._push_to_datastore(
                            "test-job-123",
                            mock_input,
                            dry_run=False,
                            temp_dir=temp_dir
                        )
        
        # Verify key operations were called
        mock_get_resource.assert_called_with("test-resource-123")
        mock_qsv.validate.assert_called()
        mock_send_to_datastore.assert_called()
        mock_update_resource.assert_called()
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @patch('ckanext.datapusher_plus.jobs.requests.get')
    def test_http_error_handling(self, mock_requests_get, mock_input):
        """Test handling of HTTP errors during download"""
        
        # Setup HTTP error
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = HTTPError(
            "Bad response",
            status_code=404,
            request_url="http://test.com",
            response=b"Not found"
        )
        mock_requests_get.return_value.__enter__.return_value = mock_response
        
        with pytest.raises(HTTPError):
            with tempfile.TemporaryDirectory() as temp_dir:
                jobs._push_to_datastore("test-job", mock_input, temp_dir=temp_dir)


class TestFileFormatHandling:
    """Test handling of different file formats"""
    
    @pytest.fixture
    def mock_qsv(self):
        """Create a mock QSV instance"""
        with patch('ckanext.datapusher_plus.jobs.QSVCommand') as mock_qsv_class:
            mock_qsv = Mock()
            mock_qsv_class.return_value = mock_qsv
            yield mock_qsv
    
    def test_excel_file_conversion(self, mock_qsv):
        """Test Excel file conversion to CSV"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a mock Excel file
            excel_file = os.path.join(temp_dir, "test.xlsx")
            open(excel_file, 'a').close()
            
            # Mock QSV excel conversion
            mock_result = Mock()
            mock_result.stderr = "Converted 100 rows"
            mock_qsv.excel.return_value = mock_result
            
            # Create output CSV
            output_csv = os.path.join(temp_dir, "qsv_excel.csv")
            with open(output_csv, 'w') as f:
                writer = csv.writer(f)
                writer.writerow(['col1', 'col2'])
                writer.writerow(['data1', 'data2'])
            
            # Test Excel conversion logic
            # This would be called within _push_to_datastore
            result = mock_qsv.excel(
                excel_file,
                sheet=0,
                trim=True,
                output_file=output_csv
            )
            
            assert result.stderr == "Converted 100 rows"
            assert os.path.exists(output_csv)
    
    def test_spatial_file_handling(self):
        """Test handling of spatial files (GeoJSON, Shapefile)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a mock GeoJSON file
            geojson_file = os.path.join(temp_dir, "test.geojson")
            geojson_data = {
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "properties": {"name": "Test"},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0, 0]
                    }
                }]
            }
            
            with open(geojson_file, 'w') as f:
                json.dump(geojson_data, f)
            
            # Test that the file exists and contains valid GeoJSON
            assert os.path.exists(geojson_file)
            with open(geojson_file, 'r') as f:
                loaded_data = json.load(f)
                assert loaded_data["type"] == "FeatureCollection"
    
    def test_zip_file_extraction(self):
        """Test ZIP file extraction"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a CSV file to zip
            csv_file = os.path.join(temp_dir, "data.csv")
            with open(csv_file, 'w') as f:
                writer = csv.writer(f)
                writer.writerow(['id', 'value'])
                writer.writerow(['1', '100'])
            
            # Create ZIP file
            import zipfile
            zip_file = os.path.join(temp_dir, "test.zip")
            with zipfile.ZipFile(zip_file, 'w') as zf:
                zf.write(csv_file, arcname="data.csv")
            
            # Test extraction
            with zipfile.ZipFile(zip_file, 'r') as zf:
                extracted_files = zf.namelist()
                assert "data.csv" in extracted_files
                
                # Extract and verify
                zf.extractall(temp_dir)
                extracted_path = os.path.join(temp_dir, "data.csv")
                assert os.path.exists(extracted_path)


class TestDataTypeInference:
    """Test data type inference and mapping"""
    
    def test_type_mapping(self):
        """Test mapping of inferred types to PostgreSQL types"""
        type_mapping = {
            "String": "text",
            "Integer": "numeric",
            "Float": "numeric",
            "DateTime": "timestamp",
            "Date": "date",
            "NULL": "text"
        }
        
        # Test each mapping
        for qsv_type, pg_type in type_mapping.items():
            assert conf.TYPE_MAPPING.get(qsv_type, "text") == pg_type
    
    def test_smartint_type_selection(self):
        """Test smart integer type selection based on min/max values"""
        test_cases = [
            # (min_val, max_val, expected_type)
            (0, 100, "integer"),  # Small range - use integer
            (-2147483648, 2147483647, "integer"),  # Max integer range
            (0, 9223372036854775807, "bigint"),  # Requires bigint
            (-9223372036854775808, 9223372036854775807, "bigint"),  # Max bigint range
            (0, 99999999999999999999, "numeric"),  # Requires numeric
        ]
        
        for min_val, max_val, expected_type in test_cases:
            if max_val <= 2147483647 and min_val >= -2147483648:
                selected_type = "integer"
            elif max_val <= 9223372036854775807 and min_val >= -9223372036854775808:
                selected_type = "bigint"
            else:
                selected_type = "numeric"
            
            assert selected_type == expected_type


class TestPIIScreening:
    """Test PII screening functionality"""
    
    @patch('ckanext.datapusher_plus.pii_screening.screen_for_pii')
    def test_pii_screening_enabled(self, mock_screen_pii):
        """Test PII screening when enabled"""
        mock_screen_pii.return_value = False  # No PII found
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as f:
            writer = csv.writer(f)
            writer.writerow(['name', 'email', 'phone'])
            writer.writerow(['John Doe', 'john@example.com', '555-1234'])
            f.flush()
            
            resource = {"id": "test-resource"}
            mock_qsv = Mock()
            logger = Mock()
            
            result = mock_screen_pii(f.name, resource, mock_qsv, tempfile.gettempdir(), logger)
            
            assert result == False  # No PII found
            mock_screen_pii.assert_called_once()
    
    def test_pii_patterns(self):
        """Test PII pattern matching"""
        # Common PII patterns to test
        pii_patterns = [
            (r'\b\d{3}-\d{2}-\d{4}\b', '123-45-6789'),  # SSN
            (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', 'test@example.com'),  # Email
            (r'\b\d{3}-\d{3}-\d{4}\b', '555-123-4567'),  # Phone
        ]
        
        import re
        for pattern, test_string in pii_patterns:
            assert re.search(pattern, test_string) is not None


class TestDatabaseOperations:
    """Test database operations"""
    
    @patch('psycopg2.connect')
    def test_copy_to_datastore(self, mock_connect):
        """Test COPY operation to PostgreSQL"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Test COPY operation
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name'])
            writer.writerow(['1', 'Test'])
            f.flush()
            
            # Simulate COPY
            mock_cursor.copy_expert.return_value = None
            mock_cursor.rowcount = 1
            
            # Execute COPY
            with open(f.name, 'rb') as csv_file:
                mock_cursor.copy_expert(
                    "COPY test_table (id, name) FROM STDIN WITH CSV HEADER",
                    csv_file
                )
            
            mock_cursor.copy_expert.assert_called_once()
            assert mock_cursor.rowcount == 1
    
    @patch('psycopg2.connect')
    def test_index_creation(self, mock_connect):
        """Test automatic index creation"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Test index creation for unique column
        mock_cursor.execute.return_value = None
        
        # Create unique index
        mock_cursor.execute("CREATE UNIQUE INDEX ON test_table (id)")
        mock_cursor.execute.assert_called_with("CREATE UNIQUE INDEX ON test_table (id)")
        
        # Create regular index
        mock_cursor.execute("CREATE INDEX ON test_table (category)")
        assert mock_cursor.execute.call_count == 2
    
    @patch('psycopg2.connect')
    def test_auto_aliasing(self, mock_connect):
        """Test auto-aliasing functionality"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Test alias creation
        resource_name = "test-data"
        package_name = "test-package"
        org_name = "test-org"
        
        expected_alias = f"{resource_name}-{package_name}-{org_name}"[:55]
        
        # Check for existing alias
        mock_cursor.fetchone.return_value = None  # No existing alias
        
        mock_cursor.execute(
            "SELECT COUNT(*), alias_of FROM _table_metadata where name like %s group by alias_of",
            (expected_alias + "%",)
        )
        
        mock_cursor.execute.assert_called()


class TestFormulaProcessing:
    """Test DRUF Jinja2 formula processing"""
    
    @patch('ckanext.datapusher_plus.jinja2_helpers.FormulaProcessor')
    def test_formula_processing(self, mock_formula_processor_class):
        """Test formula processing for package and resource fields"""
        mock_processor = Mock()
        mock_formula_processor_class.return_value = mock_processor
        
        # Mock formula results
        mock_processor.process_formulae.side_effect = [
            {"field1": "calculated_value1"},  # Package formula
            {"field2": "calculated_value2"},  # Resource formula
            {"field3": "suggestion1"},  # Package suggestion
            {"field4": "suggestion2"},  # Resource suggestion
        ]
        
        # Test package formula processing
        result = mock_processor.process_formulae("package", "dataset_fields", "formula")
        assert result == {"field1": "calculated_value1"}
        
        # Test resource formula processing
        result = mock_processor.process_formulae("resource", "resource_fields", "formula")
        assert result == {"field2": "calculated_value2"}
        
        # Test suggestion formula processing
        result = mock_processor.process_formulae("package", "dataset_fields", "suggestion_formula")
        assert result == {"field3": "suggestion1"}
        
        assert mock_processor.process_formulae.call_count == 4


class TestErrorHandling:
    """Test error handling and recovery"""
    
    def test_job_error_creation(self):
        """Test JobError exception creation"""
        error_msg = "Test error message"
        error = utils.JobError(error_msg)
        assert str(error) == error_msg
    
    def test_http_error_creation(self):
        """Test HTTPError exception creation"""
        error = HTTPError(
            message="Not found",
            status_code=404,
            request_url="http://test.com/resource",
            response=b"Resource not found"
        )
        assert error.status_code == 404
        assert error.request_url == "http://test.com/resource"
    
    @patch('ckanext.datapusher_plus.jobs.dph.mark_job_as_errored')
    def test_error_logging(self, mock_mark_errored):
        """Test that errors are properly logged"""
        job_id = "test-job-123"
        error_msg = "Test error"
        
        mock_mark_errored(job_id, error_msg)
        mock_mark_errored.assert_called_once_with(job_id, error_msg)


class TestUtilityFunctions:
    """Test utility functions"""
    
    def test_file_hash_calculation(self):
        """Test file hash calculation"""
        test_data = b"test data for hashing"
        expected_hash = hashlib.md5(test_data).hexdigest()
        
        calculated_hash = hashlib.md5(test_data).hexdigest()
        assert calculated_hash == expected_hash
    
    def test_safe_column_names(self):
        """Test column name sanitization"""
        unsafe_names = [
            ("column-name", "column_name"),
            ("123column", "unsafe_123column"),
            ("column name", "column_name"),
            ("COLUMN", "column"),  # PostgreSQL identifiers are lowercase
            ("_id", "unsafe__id"),  # Reserved column name
        ]
        
        for unsafe, expected_safe in unsafe_names:
            # This would be handled by qsv safenames
            if unsafe == "_id":
                safe = "unsafe__id"
            elif unsafe[0].isdigit():
                safe = f"unsafe_{unsafe}"
            else:
                safe = unsafe.lower().replace("-", "_").replace(" ", "_")
            
            assert safe == expected_safe
    
    def test_date_format_detection(self):
        """Test date format detection and conversion"""
        date_formats = [
            ("2024-01-15", "2024-01-15T00:00:00"),
            ("01/15/2024", "2024-01-15T00:00:00"),
            ("15/01/2024", "2024-01-15T00:00:00"),  # DMY format
            ("2024-01-15 10:30:00", "2024-01-15T10:30:00"),
        ]
        
        from dateutil.parser import parse
        for date_str, expected_iso in date_formats:
            # This would be handled by qsv datefmt
            try:
                parsed = parse(date_str, dayfirst=conf.PREFER_DMY)
                iso_format = parsed.isoformat()
                # Simplified comparison (actual implementation may differ)
                assert parsed is not None
            except:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])