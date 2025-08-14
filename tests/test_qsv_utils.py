# tests/test_qsv_utils.py
"""
Test suite for QSV utilities
Tests the QSVCommand wrapper and its operations
"""

import os
import tempfile
import csv
import json
import subprocess
from unittest import mock
from unittest.mock import Mock, MagicMock, patch, call
import pytest
import logging

from ckanext.datapusher_plus.qsv_utils import QSVCommand
from ckanext.datapusher_plus import utils


class TestQSVCommand:
    """Test QSVCommand wrapper class"""
    
    @pytest.fixture
    def logger(self):
        """Create a test logger"""
        logger = logging.getLogger("test_qsv")
        logger.setLevel(logging.DEBUG)
        return logger
    
    @pytest.fixture
    def qsv(self, logger):
        """Create a QSVCommand instance"""
        with patch('ckanext.datapusher_plus.config.QSV_BIN', '/usr/local/bin/qsvdp'):
            return QSVCommand(logger=logger)
    
    def test_qsv_initialization(self, qsv):
        """Test QSVCommand initialization"""
        assert qsv.logger is not None
        assert qsv.qsv_bin == '/usr/local/bin/qsvdp'
    
    @patch('subprocess.run')
    def test_validate_command(self, mock_run, qsv):
        """Test CSV validation command"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'validate', 'test.csv'],
            returncode=0,
            stdout=b'',
            stderr=b''
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name'])
            writer.writerow(['1', 'test'])
            f.flush()
            
            result = qsv.validate(f.name)
            
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            assert 'validate' in args
            assert f.name in args
    
    @patch('subprocess.run')
    def test_stats_command(self, mock_run, qsv):
        """Test stats generation command"""
        stats_output = b"""field,type,min,max,cardinality
id,Integer,1,10,10
name,String,a,z,10
value,Float,0.1,99.9,10"""
        
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'stats'],
            returncode=0,
            stdout=stats_output,
            stderr=b''
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'value'])
            for i in range(10):
                writer.writerow([i, f'name_{i}', i * 10.5])
            f.flush()
            
            result = qsv.stats(
                f.name,
                infer_dates=True,
                cardinality=True,
                output_file='stats.csv'
            )
            
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            assert 'stats' in args
            assert '--infer-dates' in args
            assert '--cardinality' in args
    
    @patch('subprocess.run')
    def test_index_command(self, mock_run, qsv):
        """Test index creation command"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'index'],
            returncode=0,
            stdout=b'',
            stderr=b''
        )
        
        result = qsv.index('test.csv')
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'index' in args
        assert 'test.csv' in args
    
    @patch('subprocess.run')
    def test_count_command(self, mock_run, qsv):
        """Test record count command"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'count'],
            returncode=0,
            stdout=b'100',
            stderr=b''
        )
        
        result = qsv.count('test.csv')
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'count' in args
        assert result.stdout == b'100'
    
    @patch('subprocess.run')
    def test_headers_command(self, mock_run, qsv):
        """Test headers extraction command"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'headers'],
            returncode=0,
            stdout=b'id\nname\nvalue\ndate',
            stderr=b''
        )
        
        result = qsv.headers('test.csv', just_names=True)
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'headers' in args
        assert '--just-names' in args
    
    @patch('subprocess.run')
    def test_safenames_command(self, mock_run, qsv):
        """Test safenames command for sanitizing column names"""
        unsafe_result = {
            "unsafe_headers": ["column-name", "123column"],
            "safe_headers": ["column_name", "unsafe_123column"]
        }
        
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'safenames'],
            returncode=0,
            stdout=json.dumps(unsafe_result).encode(),
            stderr=b''
        )
        
        result = qsv.safenames(
            'test.csv',
            mode='json',
            reserved=['_id'],
            prefix='unsafe_'
        )
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'safenames' in args
        assert '--mode' in args
        assert 'json' in args
    
    @patch('subprocess.run')
    def test_frequency_command(self, mock_run, qsv):
        """Test frequency table generation"""
        freq_output = b"""field,value,count,percentage
category,A,50,50.0
category,B,30,30.0
category,C,20,20.0"""
        
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'frequency'],
            returncode=0,
            stdout=freq_output,
            stderr=b''
        )
        
        result = qsv.frequency('test.csv', limit=10)
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'frequency' in args
        assert '--limit' in args
        assert '10' in args
    
    @patch('subprocess.run')
    def test_slice_command(self, mock_run, qsv):
        """Test slice command for creating previews"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'slice'],
            returncode=0,
            stdout=b'',
            stderr=b''
        )
        
        # Test slice from beginning
        result = qsv.slice('test.csv', length=100, output_file='preview.csv')
        
        args = mock_run.call_args[0][0]
        assert 'slice' in args
        assert '--len' in args
        assert '100' in args
        
        # Test slice from end
        mock_run.reset_mock()
        result = qsv.slice('test.csv', start=-1, length=100, output_file='preview.csv')
        
        args = mock_run.call_args[0][0]
        assert 'slice' in args
        assert '--start' in args
        assert '-1' in args
    
    @patch('subprocess.run')
    def test_sortcheck_command(self, mock_run, qsv):
        """Test sortcheck command for detecting sorted data and duplicates"""
        sortcheck_result = {
            "sorted": True,
            "record_count": 1000,
            "unsorted_breaks": 0,
            "dupe_count": 5
        }
        
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'sortcheck'],
            returncode=0,
            stdout=json.dumps(sortcheck_result).encode(),
            stderr=b''
        )
        
        result = qsv.sortcheck('test.csv', json_output=True)
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'sortcheck' in args
        assert '--json' in args
    
    @patch('subprocess.run')
    def test_extdedup_command(self, mock_run, qsv):
        """Test deduplication command"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'extdedup'],
            returncode=0,
            stdout=b'',
            stderr=b'Removed 10 duplicate rows'
        )
        
        result = qsv.extdedup('input.csv', 'output.csv')
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'extdedup' in args
        assert 'input.csv' in args
        assert '--output' in args
        assert 'output.csv' in args
    
    @patch('subprocess.run')
    def test_excel_command(self, mock_run, qsv):
        """Test Excel to CSV conversion"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'excel'],
            returncode=0,
            stdout=b'',
            stderr=b'Exported sheet 0: 100 rows'
        )
        
        result = qsv.excel(
            'test.xlsx',
            sheet=0,
            trim=True,
            output_file='output.csv'
        )
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'excel' in args
        assert '--sheet' in args
        assert '0' in args
        assert '--trim' in args
    
    @patch('subprocess.run')
    def test_geoconvert_command(self, mock_run, qsv):
        """Test spatial file conversion"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'geoconvert'],
            returncode=0,
            stdout=b'',
            stderr=b'Converted 50 features'
        )
        
        result = qsv.geoconvert(
            'input.shp',
            'SHP',
            'csv',
            max_length=32767,
            output_file='output.csv'
        )
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'geoconvert' in args
        assert 'input.shp' in args
    
    @patch('subprocess.run')
    def test_datefmt_command(self, mock_run, qsv):
        """Test date formatting command"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'datefmt'],
            returncode=0,
            stdout=b'',
            stderr=b'Formatted 100 dates'
        )
        
        result = qsv.datefmt(
            'date_column',
            'input.csv',
            prefer_dmy=False,
            output_file='output.csv'
        )
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'datefmt' in args
        assert 'date_column' in args
    
    @patch('subprocess.run')
    def test_input_command(self, mock_run, qsv):
        """Test input normalization command"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'input'],
            returncode=0,
            stdout=b'',
            stderr=b'Normalized to UTF-8'
        )
        
        result = qsv.input('input.tsv', trim_headers=True, output_file='output.csv')
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'input' in args
        assert '--trim-headers' in args
    
    @patch('subprocess.run')
    def test_error_handling(self, mock_run, qsv):
        """Test error handling for failed commands"""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['qsvdp', 'validate'],
            returncode=1,
            stdout=b'',
            stderr=b'Invalid CSV: Row 5 has 3 columns, expected 4'
        )
        
        with pytest.raises(utils.JobError) as exc_info:
            qsv.validate('invalid.csv')
        
        assert 'Invalid CSV' in str(exc_info.value)
    
    @patch('subprocess.run')
    def test_command_timeout(self, mock_run, qsv):
        """Test command timeout handling"""
        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd=['qsvdp', 'stats'],
            timeout=30
        )
        
        with pytest.raises(utils.JobError) as exc_info:
            qsv.stats('huge_file.csv')
        
        assert 'timeout' in str(exc_info.value).lower()


class TestQSVIntegration:
    """Integration tests for QSV operations"""
    
    @pytest.fixture
    def sample_csv(self):
        """Create a sample CSV file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'value', 'date'])
            writer.writerow(['1', 'Alice', '100.5', '2024-01-01'])
            writer.writerow(['2', 'Bob', '200.75', '2024-01-02'])
            writer.writerow(['3', 'Charlie', '300.25', '2024-01-03'])
            f.flush()
            yield f.name
        os.unlink(f.name)
    
    def test_full_preprocessing_pipeline(self, sample_csv):
        """Test a full preprocessing pipeline"""
        logger = logging.getLogger("test_pipeline")
        
        with patch('subprocess.run') as mock_run:
            # Mock all subprocess calls
            mock_run.return_value = subprocess.CompletedProcess(
                args=['qsvdp'],
                returncode=0,
                stdout=b'Success',
                stderr=b''
            )
            
            qsv = QSVCommand(logger=logger)
            
            # 1. Validate
            qsv.validate(sample_csv)
            
            # 2. Create index
            qsv.index(sample_csv)
            
            # 3. Get count
            mock_run.return_value.stdout = b'3'
            count_result = qsv.count(sample_csv)
            
            # 4. Generate stats
            stats_output = tempfile.NamedTemporaryFile(suffix='.csv', delete=False).name
            qsv.stats(sample_csv, output_file=stats_output)
            
            # 5. Generate frequency table
            freq_output = tempfile.NamedTemporaryFile(suffix='.csv', delete=False).name
            qsv.frequency(sample_csv, output_file=freq_output)
            
            # Verify all commands were called
            assert mock_run.call_count >= 5
            
            # Cleanup
            for f in [stats_output, freq_output]:
                if os.path.exists(f):
                    os.unlink(f)