# tests/test_plugin.py
"""
Test suite for DataPusher+ plugin
Tests the CKAN plugin interface and integration
"""

import pytest
from unittest import mock
from unittest.mock import Mock, MagicMock, patch
import ckan.plugins as plugins
from ckan.tests import helpers, factories
import ckan.tests.factories as ckan_factories


class TestDataPusherPlusPlugin:
    """Test the DataPusher+ plugin"""
    
    @classmethod
    def setup_class(cls):
        """Setup test class"""
        if not plugins.plugin_loaded('datapusher_plus'):
            plugins.load('datapusher_plus')
    
    @classmethod
    def teardown_class(cls):
        """Teardown test class"""
        plugins.unload('datapusher_plus')
    
    def test_plugin_interfaces(self):
        """Test that plugin implements required interfaces"""
        plugin = plugins.get_plugin('datapusher_plus')
        
        # Check if plugin implements required interfaces
        assert hasattr(plugin, 'get_actions')
        assert hasattr(plugin, 'get_auth_functions')
        assert hasattr(plugin, 'get_helpers')
        assert hasattr(plugin, 'update_config')
    
    @patch('ckanext.datapusher_plus.plugin.toolkit')
    def test_get_actions(self, mock_toolkit):
        """Test get_actions method"""
        plugin = plugins.get_plugin('datapusher_plus')
        actions = plugin.get_actions()
        
        expected_actions = [
            'datapusher_plus_submit',
            'datapusher_plus_hook',
            'datapusher_plus_status',
            'datapusher_plus_resubmit'
        ]
        
        for action in expected_actions:
            assert action in actions
    
    def test_get_auth_functions(self):
        """Test get_auth_functions method"""
        plugin = plugins.get_plugin('datapusher_plus')
        auth_functions = plugin.get_auth_functions()
        
        expected_auth = [
            'datapusher_plus_submit',
            'datapusher_plus_status'
        ]
        
        for auth in expected_auth:
            assert auth in auth_functions
    
    def test_get_helpers(self):
        """Test get_helpers method"""
        plugin = plugins.get_plugin('datapusher_plus')
        helpers = plugin.get_helpers()
        
        expected_helpers = [
            'datapusher_plus_status',
            'datapusher_plus_status_description',
            'datapusher_plus_link'
        ]
        
        for helper in expected_helpers:
            assert helper in helpers
    
    @patch('ckanext.datapusher_plus.plugin.toolkit')
    def test_update_config(self, mock_toolkit):
        """Test update_config method"""
        plugin = plugins.get_plugin('datapusher_plus')
        config = {}
        
        plugin.update_config(config)
        
        # Check that templates and public directories are added
        mock_toolkit.add_template_directory.assert_called()
        mock_toolkit.add_public_directory.assert_called()
        mock_toolkit.add_resource.assert_called()
    
    def test_resource_controller_hooks(self):
        """Test resource controller hooks"""
        plugin = plugins.get_plugin('datapusher_plus')
        
        # Test before_create hook
        assert hasattr(plugin, 'before_resource_create')
        
        # Test after_create hook
        assert hasattr(plugin, 'after_resource_create')
        
        # Test after_update hook
        assert hasattr(plugin, 'after_resource_update')


# tests/test_helpers.py
"""
Test suite for DataPusher+ helper functions
"""

import os
import tempfile
import zipfile
import csv
import json
from unittest import mock
from unittest.mock import Mock, MagicMock, patch
import pytest

from ckanext.datapusher_plus import helpers as dph
from ckanext.datapusher_plus import utils


class TestJobManagement:
    """Test job management functions"""
    
    @patch('ckanext.datapusher_plus.model.model.Session')
    def test_add_pending_job(self, mock_session):
        """Test adding a pending job"""
        job_id = "test-job-123"
        metadata = {"resource_id": "test-resource"}
        
        dph.add_pending_job(job_id, metadata=metadata, result_url="http://test.com")
        
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
    
    @patch('ckanext.datapusher_plus.model.model.Session')
    def test_mark_job_as_completed(self, mock_session):
        """Test marking job as completed"""
        job_id = "test-job-123"
        job_dict = {"status": "complete"}
        
        dph.mark_job_as_completed(job_id, job_dict)
        
        mock_session.commit.assert_called_once()
    
    @patch('ckanext.datapusher_plus.model.model.Session')
    def test_mark_job_as_errored(self, mock_session):
        """Test marking job as errored"""
        job_id = "test-job-123"
        error_msg = "Test error"
        
        dph.mark_job_as_errored(job_id, error_msg)
        
        mock_session.commit.assert_called_once()
    
    @patch('ckanext.datapusher_plus.model.model.Session')
    def test_get_job_status(self, mock_session):
        """Test getting job status"""
        job_id = "test-job-123"
        
        mock_job = Mock()
        mock_job.status = "running"
        mock_job.error = None
        mock_session.query().filter_by().first.return_value = mock_job
        
        status = dph.get_job_status(job_id)
        
        assert status["status"] == "running"
        assert status["error"] is None


class TestZipFileHandling:
    """Test ZIP file handling functions"""
    
    def test_extract_single_file_from_zip(self):
        """Test extracting a single file from ZIP"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a CSV file
            csv_content = "id,name\n1,test\n"
            csv_file = os.path.join(temp_dir, "data.csv")
            with open(csv_file, 'w') as f:
                f.write(csv_content)
            
            # Create ZIP with single file
            zip_path = os.path.join(temp_dir, "test.zip")
            with zipfile.ZipFile(zip_path, 'w') as zf:
                zf.write(csv_file, arcname="data.csv")
            
            # Test extraction
            logger = Mock()
            file_count, extracted_path, format = dph.extract_zip_or_metadata(
                zip_path, temp_dir, logger
            )
            
            assert file_count == 1
            assert extracted_path.endswith("data.csv")
            assert format == "CSV"
    
    def test_create_zip_manifest(self):
        """Test creating manifest for multi-file ZIP"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple files
            files = [
                ("file1.csv", "id,name\n1,test1\n"),
                ("file2.txt", "test content"),
                ("subdir/file3.json", '{"key": "value"}')
            ]
            
            # Create ZIP with multiple files
            zip_path = os.path.join(temp_dir, "multi.zip")
            with zipfile.ZipFile(zip_path, 'w') as zf:
                for filename, content in files:
                    # Create subdirectory if needed
                    file_path = os.path.join(temp_dir, filename)
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    with open(file_path, 'w') as f:
                        f.write(content)
                    zf.write(file_path, arcname=filename)
            
            # Test manifest creation
            logger = Mock()
            file_count, manifest_path, format = dph.extract_zip_or_metadata(
                zip_path, temp_dir, logger
            )
            
            assert file_count == 3
            assert manifest_path.endswith("_manifest.csv")
            assert format == "CSV"
            
            # Check manifest content
            with open(manifest_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 3
                assert 'filename' in rows[0]
                assert 'file_size' in rows[0]


class TestDataDictionaryHelpers:
    """Test data dictionary helper functions"""
    
    def test_create_data_dictionary(self):
        """Test creating a data dictionary from stats"""
        headers = ['id', 'name', 'value', 'date']
        types = ['Integer', 'String', 'Float', 'Date']
        
        headers_dict = dph.create_data_dictionary(headers, types)
        
        assert len(headers_dict) == 4
        assert headers_dict[0]['id'] == 'id'
        assert headers_dict[0]['type'] == 'numeric'  # Integer maps to numeric
        assert headers_dict[1]['type'] == 'text'  # String maps to text
        assert headers_dict[2]['type'] == 'numeric'  # Float maps to numeric
        assert headers_dict[3]['type'] == 'date'  # Date maps to date
    
    def test_sanitize_column_names(self):
        """Test column name sanitization"""
        unsafe_names = [
            "column-name",
            "123column",
            "column name",
            "COLUMN",
            "_id"
        ]
        
        safe_names = dph.sanitize_column_names(unsafe_names)
        
        assert safe_names[0] == "column_name"
        assert safe_names[1] == "unsafe_123column"
        assert safe_names[2] == "column_name"
        assert safe_names[3] == "column"
        assert safe_names[4] == "unsafe__id"


class TestResourceMetadata:
    """Test resource metadata functions"""
    
    @patch('ckanext.datapusher_plus.helpers.toolkit')
    def test_update_resource_metadata(self, mock_toolkit):
        """Test updating resource metadata"""
        resource = {
            "id": "test-resource",
            "package_id": "test-package",
            "datastore_active": False
        }
        
        metadata = {
            "datastore_active": True,
            "total_record_count": 1000,
            "preview_rows": 100
        }
        
        dph.update_resource_metadata(resource, metadata)
        
        assert resource["datastore_active"] == True
        assert resource["total_record_count"] == 1000
        assert resource["preview_rows"] == 100
    
    def test_calculate_file_hash(self):
        """Test file hash calculation"""
        test_data = b"test data for hashing"
        
        hash_value = dph.calculate_file_hash(test_data)
        
        import hashlib
        expected_hash = hashlib.md5(test_data).hexdigest()
        assert hash_value == expected_hash


class TestStatisticsHelpers:
    """Test statistics helper functions"""
    
    def test_parse_stats_csv(self):
        """Test parsing statistics CSV"""
        stats_content = """field,type,min,max,mean,stddev,cardinality
id,Integer,1,100,50.5,28.87,100
name,String,a,z,,,100
value,Float,0.1,999.9,500.0,288.7,95"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(stats_content)
            f.flush()
            
            stats = dph.parse_stats_csv(f.name)
            
            assert len(stats) == 3
            assert stats['id']['type'] == 'Integer'
            assert stats['id']['min'] == '1'
            assert stats['id']['max'] == '100'
            assert stats['value']['cardinality'] == '95'
        
        os.unlink(f.name)
    
    def test_parse_frequency_csv(self):
        """Test parsing frequency CSV"""
        freq_content = """field,value,count,percentage
category,A,50,50.0
category,B,30,30.0
category,C,20,20.0
status,active,80,80.0
status,inactive,20,20.0"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(freq_content)
            f.flush()
            
            freqs = dph.parse_frequency_csv(f.name)
            
            assert 'category' in freqs
            assert len(freqs['category']) == 3
            assert freqs['category'][0]['value'] == 'A'
            assert freqs['category'][0]['percentage'] == '50.0'
            assert len(freqs['status']) == 2
        
        os.unlink(f.name)


class TestErrorHandling:
    """Test error handling in helper functions"""
    
    def test_safe_json_loads(self):
        """Test safe JSON loading with error handling"""
        valid_json = '{"key": "value"}'
        invalid_json = '{invalid json}'
        
        result = dph.safe_json_loads(valid_json)
        assert result == {"key": "value"}
        
        result = dph.safe_json_loads(invalid_json)
        assert result is None
    
    def test_safe_file_read(self):
        """Test safe file reading with error handling"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test content")
            f.flush()
            
            content = dph.safe_file_read(f.name)
            assert content == "test content"
        
        os.unlink(f.name)
        
        # Test non-existent file
        content = dph.safe_file_read("/non/existent/file.txt")
        assert content is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])