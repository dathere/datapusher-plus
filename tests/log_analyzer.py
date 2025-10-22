#!/usr/bin/env python3
"""
Advanced DataPusher Plus Analytics Engine
Provides enterprise-grade insights and predictive analysis
"""

import re
import csv
import sys
import statistics
import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import hashlib

def parse_worker_logs(log_file_path):
    """Parse worker logs and extract job information"""
    try:
        with open(log_file_path, 'r') as f:
            log_content = f.read()
    except FileNotFoundError:
        print(f"Log file not found: {log_file_path}")
        return []
    except Exception as e:
        print(f"Error reading log file: {e}")
        return []

    # Split log into individual job entries by looking for job start pattern
    # Pattern: timestamp INFO [job_id] Setting log level to INFO
    job_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) INFO\s+\[([a-f0-9-]{36})\] Setting log level to INFO'
    
    # Find all job starts
    job_starts = list(re.finditer(job_pattern, log_content))
    processed_jobs = []

    for i, match in enumerate(job_starts):
        job_start_pos = match.start()
        job_end_pos = job_starts[i + 1].start() if i + 1 < len(job_starts) else len(log_content)
        
        entry = log_content[job_start_pos:job_end_pos]
        
        # Extract timestamp and job ID from the match
        timestamp_str = match.group(1)
        job_id = match.group(2)

        # Convert timestamp to standard format
        try:
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
            timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            timestamp = timestamp_str

        # Extract file information
        file_url_match = re.search(r'Fetching from: (.+)', entry)
        file_url = file_url_match.group(1).strip() if file_url_match else "unknown"
        file_name = file_url.split('/')[-1] if file_url != "unknown" else "unknown"

        # Determine job status
        if "DATAPUSHER+ JOB DONE!" in entry:
            status = "SUCCESS"
        elif "ckanext.datapusher_plus.utils.JobError:" in entry:
            status = "ERROR"
        else:
            status = "INCOMPLETE"

        # Extract QSV version
        qsv_version_match = re.search(r'qsv version found: ([\d.]+)', entry)
        qsv_version = qsv_version_match.group(1) if qsv_version_match else ""

        # Extract file format
        file_format_match = re.search(r'File format: (\w+)', entry)
        file_format = file_format_match.group(1) if file_format_match else ""

        # Extract encoding
        encoding_match = re.search(r'Identified encoding of the file: (\w+)', entry)
        encoding = encoding_match.group(1) if encoding_match else ""

        # Check normalization
        normalized = "Successful" if "Normalized & transcoded" in entry else "Failed"

        # Check if valid CSV
        valid_csv = "TRUE" if "Well-formed, valid CSV file confirmed" in entry else "FALSE"

        # Check if sorted
        sorted_match = re.search(r'Sorted: (True|False)', entry)
        sorted_status = sorted_match.group(1).upper() if sorted_match else "UNKNOWN"

        # Check database safe headers
        unsafe_headers_match = re.search(r'"(\d+) unsafe" header names found', entry)
        if unsafe_headers_match:
            unsafe_count = int(unsafe_headers_match.group(1))
            db_safe_headers = f"{unsafe_count} unsafe headers found"
        elif "No unsafe header names found" in entry:
            db_safe_headers = "All headers safe"
        else:
            db_safe_headers = "Unknown"

        # Check analysis status
        analysis_match = re.search(r'ANALYSIS DONE! Analyzed and prepped in ([\d.]+) seconds', entry)
        analysis_status = "Successful" if analysis_match else "Failed"

        # Extract records detected
        records_match = re.search(r'(\d+)\s+records detected', entry)
        records_processed = int(records_match.group(1)) if records_match else 0

        # Extract timing information
        timings = {
            'total_time': 0.0,
            'download_time': 0.0,
            'analysis_time': 0.0,
            'copying_time': 0.0,
            'indexing_time': 0.0,
            'formulae_time': 0.0,
            'metadata_time': 0.0
        }

        # Parse timing breakdown from the job summary
        total_time_match = re.search(r'TOTAL ELAPSED TIME: ([\d.]+)', entry)
        if total_time_match:
            timings['total_time'] = float(total_time_match.group(1))

        download_match = re.search(r'Download: ([\d.]+)', entry)
        if download_match:
            timings['download_time'] = float(download_match.group(1))

        analysis_match = re.search(r'Analysis: ([\d.]+)', entry)
        if analysis_match:
            timings['analysis_time'] = float(analysis_match.group(1))

        copying_match = re.search(r'COPYing: ([\d.]+)', entry)
        if copying_match:
            timings['copying_time'] = float(copying_match.group(1))

        indexing_match = re.search(r'Indexing: ([\d.]+)', entry)
        if indexing_match:
            timings['indexing_time'] = float(indexing_match.group(1))

        formulae_match = re.search(r'Formulae processing: ([\d.]+)', entry)
        if formulae_match:
            timings['formulae_time'] = float(formulae_match.group(1))

        metadata_match = re.search(r'Resource metadata updates: ([\d.]+)', entry)
        if metadata_match:
            timings['metadata_time'] = float(metadata_match.group(1))

        # Extract rows copied
        rows_copied_match = re.search(r'Copied (\d+) rows to', entry)
        rows_copied = int(rows_copied_match.group(1)) if rows_copied_match else 0

        # Extract columns indexed
        indexed_match = re.search(r'Indexed (\d+) column/s', entry)
        columns_indexed = int(indexed_match.group(1)) if indexed_match else 0

        # Extract specific DataPusher Plus error
        error_type = ""
        error_message = ""

        if status == "ERROR":
            # Look for specific DataPusher Plus JobError
            dp_error_match = re.search(r'ckanext\.datapusher_plus\.utils\.JobError: (.+?)(?:\n|$)', entry)
            if dp_error_match:
                error_message = dp_error_match.group(1).strip()
                # Classify error type based on message content
                if "invalid Zip archive" in error_message or "EOCD" in error_message:
                    error_type = "CORRUPTED_EXCEL"
                elif "qsv command failed" in error_message:
                    error_type = "QSV_ERROR"
                elif "Only http, https, and ftp resources may be fetched" in error_message:
                    error_type = "INVALID_URL"
                else:
                    error_type = "DATAPUSHER_ERROR"
            else:
                error_type = "UNKNOWN_ERROR"
                error_message = "Unknown DataPusher error"

        # Only add jobs that have valid job IDs and meaningful data
        if job_id and job_id != "unknown":
            processed_jobs.append({
                'timestamp': timestamp,
                'job_id': job_id,
                'file_name': file_name,
                'status': status,
                'qsv_version': qsv_version,
                'file_format': file_format,
                'encoding': encoding,
                'normalized': normalized,
                'valid_csv': valid_csv,
                'sorted': sorted_status,
                'db_safe_headers': db_safe_headers,
                'analysis': analysis_status,
                'records': records_processed,
                'total_time': timings['total_time'],
                'download_time': timings['download_time'],
                'analysis_time': timings['analysis_time'],
                'copying_time': timings['copying_time'],
                'indexing_time': timings['indexing_time'],
                'formulae_time': timings['formulae_time'],
                'metadata_time': timings['metadata_time'],
                'rows_copied': rows_copied,
                'columns_indexed': columns_indexed,
                'error_type': error_type,
                'error_message': error_message.replace('"', '""') if error_message else ""  # Escape quotes for CSV
            })

    return processed_jobs

def write_worker_analysis(jobs, output_file):
    """Write job analysis to CSV file"""
    fieldnames = ['timestamp', 'job_id', 'file_name', 'status', 'qsv_version', 'file_format', 
                  'encoding', 'normalized', 'valid_csv', 'sorted', 'db_safe_headers', 'analysis',
                  'records', 'total_time', 'download_time', 'analysis_time', 'copying_time', 
                  'indexing_time', 'formulae_time', 'metadata_time', 'rows_copied', 'columns_indexed',
                  'error_type', 'error_message', 'data_quality_score', 'processing_efficiency']
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(jobs)

def generate_performance_insights(jobs):
    """Generate performance insights from job data"""
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    error_jobs = [job for job in jobs if job['status'] == 'ERROR']
    
    insights = []
    
    if successful_jobs:
        # Calculate performance metrics
        total_times = [job['total_time'] for job in successful_jobs if job['total_time']]
        download_times = [job['download_time'] for job in successful_jobs if job['download_time']]
        analysis_times = [job['analysis_time'] for job in successful_jobs if job['analysis_time']]
        copying_times = [job['copying_time'] for job in successful_jobs if job['copying_time']]
        
        # Calculate data metrics
        total_records = sum(job['records'] for job in successful_jobs)
        total_rows_copied = sum(job['rows_copied'] for job in successful_jobs)
        total_columns_indexed = sum(job['columns_indexed'] for job in successful_jobs)
        
        insights.append(f"Total Records Processed: {total_records:,}")
        insights.append(f"Total Rows Imported: {total_rows_copied:,}")
        insights.append(f"Total Columns Indexed: {total_columns_indexed}")
        
        if total_times:
            avg_total = statistics.mean(total_times)
            fastest = min(total_times)
            slowest = max(total_times)
            insights.append(f"Average Processing Time: {avg_total:.2f}s")
            insights.append(f"Fastest File: {fastest:.2f}s")
            insights.append(f"Slowest File: {slowest:.2f}s")
            
            if total_records > 0:
                throughput = total_records / sum(total_times)
                insights.append(f"Processing Throughput: {throughput:,.0f} records/sec")
        
        if download_times:
            avg_download = statistics.mean(download_times)
            insights.append(f"Average Download Time: {avg_download:.2f}s")
        
        if analysis_times:
            avg_analysis = statistics.mean(analysis_times)
            insights.append(f"Average Analysis Time: {avg_analysis:.2f}s")
        
        if copying_times:
            avg_copying = statistics.mean(copying_times)
            insights.append(f"Average Copy Time: {avg_copying:.2f}s")

        # QSV version analysis
        qsv_versions = [job['qsv_version'] for job in successful_jobs if job['qsv_version']]
        if qsv_versions:
            unique_versions = list(set(qsv_versions))
            insights.append(f"QSV Versions Used: {', '.join(unique_versions)}")

        # File format analysis
        formats = [job['file_format'] for job in successful_jobs if job['file_format']]
        if formats:
            format_counts = {}
            for fmt in formats:
                format_counts[fmt] = format_counts.get(fmt, 0) + 1
            format_summary = ', '.join([f"{fmt}({count})" for fmt, count in format_counts.items()])
            insights.append(f"File Formats Processed: {format_summary}")
    
    # Error analysis
    if error_jobs:
        error_types = {}
        for job in error_jobs:
            error_type = job['error_type']
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        most_common_error = max(error_types, key=error_types.get)
        insights.append(f"Most Common Error: {most_common_error} ({error_types[most_common_error]} occurrences)")
        
        if 'CORRUPTED_EXCEL' in error_types:
            insights.append(f"Corrupted Excel Files: {error_types['CORRUPTED_EXCEL']}")
        
        if 'QSV_ERROR' in error_types:
            insights.append(f"QSV Processing Errors: {error_types['QSV_ERROR']}")
    
    return insights

def get_worker_insight_for_file(jobs, target_file):
    """Get worker insight string for a specific file"""
    for job in jobs:
        if target_file in job['file_name'] or job['file_name'] in target_file:
            if job['status'] == 'SUCCESS':
                records = job['records']
                total_time = job['total_time']
                phases = []
                if job['download_time'] > 0.1:
                    phases.append(f"DL:{job['download_time']:.1f}s")
                if job['analysis_time'] > 0.1:
                    phases.append(f"AN:{job['analysis_time']:.1f}s")
                if job['copying_time'] > 0.1:
                    phases.append(f"CP:{job['copying_time']:.1f}s")
                
                phase_info = "|".join(phases[:2])  # Limit to 2 phases
                if records > 0:
                    return f"{records}rec|{total_time:.1f}s|{phase_info}"
                else:
                    return f"{total_time:.1f}s|{phase_info}"
            elif job['status'] == 'ERROR':
                return f"ERROR:{job['error_type']}"
            break
    return "No worker data"

# Enhanced Analytics Functions
def calculate_data_quality_score(job):
    """Calculate a composite data quality score (0-100)"""
    score = 100
    
    # Penalize based on issues
    if job['valid_csv'] != 'TRUE':
        score -= 30
    if job['sorted'] == 'FALSE':
        score -= 10
    if 'unsafe headers' in job['db_safe_headers'].lower():
        unsafe_count = int(re.search(r'(\d+)', job['db_safe_headers']).group(1)) if re.search(r'(\d+)', job['db_safe_headers']) else 0
        score -= min(unsafe_count * 5, 25)
    if job['normalized'] != 'Successful':
        score -= 20
    if job['analysis'] != 'Successful':
        score -= 25
    
    # Bonus for good characteristics
    if job['encoding'] == 'UTF-8':
        score += 5
    if int(job['records']) > 1000:
        score += 5
    
    return max(0, min(100, score))

def detect_performance_anomalies(jobs):
    """Detect performance anomalies using statistical analysis"""
    if len(jobs) < 3:
        return []
    
    anomalies = []
    total_times = [job['total_time'] for job in jobs if job['status'] == 'SUCCESS']
    
    if len(total_times) >= 3:
        mean_time = statistics.mean(total_times)
        stdev_time = statistics.stdev(total_times)
        threshold = mean_time + (2 * stdev_time)
        
        for job in jobs:
            if job['status'] == 'SUCCESS' and job['total_time'] > threshold:
                anomalies.append({
                    'file': job['file_name'],
                    'job_id': job['job_id'],
                    'actual_time': job['total_time'],
                    'expected_time': mean_time,
                    'deviation_factor': job['total_time'] / mean_time,
                    'type': 'SLOW_PROCESSING'
                })
    
    return anomalies

def analyze_failure_patterns(jobs):
    """Advanced failure pattern analysis"""
    error_jobs = [job for job in jobs if job['status'] == 'ERROR']
    patterns = {
        'by_file_format': Counter(),
        'by_time_of_day': Counter(),
        'by_file_size_proxy': Counter(),  # Using records as proxy
        'sequential_failures': [],
        'recurring_files': Counter()
    }
    
    for job in error_jobs:
        patterns['by_file_format'][job['file_format']] += 1
        
        # Time-based analysis
        try:
            dt = datetime.strptime(job['timestamp'], '%Y-%m-%d %H:%M:%S')
            hour_bucket = f"{dt.hour:02d}:00-{dt.hour:02d}:59"
            patterns['by_time_of_day'][hour_bucket] += 1
        except:
            pass
        
        # File size analysis (using records as proxy)
        records = int(job['records']) if job['records'] else 0
        size_bucket = 'small' if records < 100 else 'medium' if records < 10000 else 'large'
        patterns['by_file_size_proxy'][size_bucket] += 1
        
        patterns['recurring_files'][job['file_name']] += 1
    
    # Detect sequential failures
    error_jobs_sorted = sorted(error_jobs, key=lambda x: x['timestamp'])
    consecutive_count = 0
    for i, job in enumerate(error_jobs_sorted):
        if i > 0:
            prev_time = datetime.strptime(error_jobs_sorted[i-1]['timestamp'], '%Y-%m-%d %H:%M:%S')
            curr_time = datetime.strptime(job['timestamp'], '%Y-%m-%d %H:%M:%S')
            if (curr_time - prev_time).seconds < 300:  # Within 5 minutes
                consecutive_count += 1
            else:
                if consecutive_count > 0:
                    patterns['sequential_failures'].append(consecutive_count + 1)
                consecutive_count = 0
    
    return patterns

def calculate_processing_efficiency_metrics(jobs):
    """Calculate advanced efficiency metrics"""
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    if not successful_jobs:
        return {}
    
    metrics = {}
    
    # Records per second throughput
    total_records = sum(int(job['records']) for job in successful_jobs)
    total_time = sum(job['total_time'] for job in successful_jobs)
    metrics['overall_throughput'] = total_records / total_time if total_time > 0 else 0
    
    # Phase efficiency analysis
    phase_times = {
        'download': [job['download_time'] for job in successful_jobs],
        'analysis': [job['analysis_time'] for job in successful_jobs],
        'copying': [job['copying_time'] for job in successful_jobs],
        'indexing': [job['indexing_time'] for job in successful_jobs],
        'formulae': [job['formulae_time'] for job in successful_jobs],
        'metadata': [job['metadata_time'] for job in successful_jobs]
    }
    
    for phase, times in phase_times.items():
        if times:
            metrics[f'{phase}_avg'] = statistics.mean(times)
            metrics[f'{phase}_efficiency'] = sum(times) / total_time * 100  # % of total time
    
    # Resource utilization scoring
    for job in successful_jobs:
        records = int(job['records'])
        if records > 0:
            job['records_per_second'] = records / job['total_time']
            job['time_per_1k_records'] = job['total_time'] / (records / 1000) if records >= 1000 else job['total_time']
    
    return metrics

def generate_predictive_insights(jobs):
    """Generate predictive insights and recommendations"""
    insights = []
    
    # Failure prediction based on patterns
    error_jobs = [job for job in jobs if job['status'] == 'ERROR']
    if error_jobs:
        error_formats = Counter(job['file_format'] for job in error_jobs)
        total_jobs_by_format = Counter(job['file_format'] for job in jobs)
        
        for fmt, error_count in error_formats.items():
            total_count = total_jobs_by_format[fmt]
            failure_rate = error_count / total_count
            if failure_rate > 0.3:  # 30% failure rate
                insights.append({
                    'type': 'HIGH_RISK_FORMAT',
                    'format': fmt,
                    'failure_rate': failure_rate,
                    'recommendation': f'Review {fmt} file processing pipeline - {failure_rate:.1%} failure rate detected'
                })
    
    # Performance degradation detection
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    if len(successful_jobs) >= 6:
        # Compare first half vs second half performance
        mid_point = len(successful_jobs) // 2
        first_half = successful_jobs[:mid_point]
        second_half = successful_jobs[mid_point:]
        
        avg_first = statistics.mean(job['total_time'] for job in first_half)
        avg_second = statistics.mean(job['total_time'] for job in second_half)
        
        if avg_second > avg_first * 1.3:  # 30% slower
            insights.append({
                'type': 'PERFORMANCE_DEGRADATION',
                'degradation_factor': avg_second / avg_first,
                'recommendation': 'System performance degrading over time - investigate resource constraints'
            })
    
    # Data quality trend analysis
    quality_scores = [calculate_data_quality_score(job) for job in successful_jobs]
    if quality_scores and statistics.mean(quality_scores) < 80:
        insights.append({
            'type': 'DATA_QUALITY_CONCERN',
            'avg_quality_score': statistics.mean(quality_scores),
            'recommendation': 'Multiple data quality issues detected - implement data validation pipeline'
        })
    
    return insights

def generate_business_impact_metrics(jobs):
    """Calculate business-relevant metrics"""
    metrics = {}
    
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    total_jobs = len(jobs)
    
    # Availability metrics
    metrics['system_availability'] = len(successful_jobs) / total_jobs if total_jobs > 0 else 0
    metrics['mttr'] = calculate_mean_time_to_recovery(jobs)  # Simplified
    
    # Data pipeline health
    total_records = sum(int(job['records']) for job in successful_jobs)
    total_processing_time = sum(job['total_time'] for job in successful_jobs)
    
    metrics['data_pipeline_efficiency'] = total_records / total_processing_time if total_processing_time > 0 else 0
    metrics['cost_per_1k_records'] = estimate_processing_cost(successful_jobs)
    
    # Quality impact
    quality_scores = [calculate_data_quality_score(job) for job in successful_jobs]
    metrics['avg_data_quality'] = statistics.mean(quality_scores) if quality_scores else 0
    metrics['quality_sla_compliance'] = sum(1 for score in quality_scores if score >= 85) / len(quality_scores) if quality_scores else 0
    
    return metrics

def calculate_mean_time_to_recovery(jobs):
    """Simplified MTTR calculation"""
    # This would need more sophisticated logic in production
    error_jobs = [job for job in jobs if job['status'] == 'ERROR']
    if not error_jobs:
        return 0
    return statistics.mean(job['total_time'] for job in error_jobs)

def estimate_processing_cost(jobs):
    """Estimate processing cost based on resource usage"""
    # Simplified cost model - in reality would integrate with cloud billing APIs
    total_cpu_seconds = sum(job['total_time'] for job in jobs)
    # Assuming $0.10 per CPU hour as rough estimate
    return (total_cpu_seconds / 3600) * 0.10

def generate_security_insights(jobs):
    """Security and compliance insights"""
    insights = []
    
    # Detect suspicious patterns
    file_hashes = {}
    for job in jobs:
        # Simple hash of filename + file format for duplicate detection
        file_sig = hashlib.md5(f"{job['file_name']}{job['file_format']}".encode()).hexdigest()
        if file_sig in file_hashes:
            file_hashes[file_sig].append(job)
        else:
            file_hashes[file_sig] = [job]
    
    # Flag potential security issues
    for file_sig, job_list in file_hashes.items():
        if len(job_list) > 5:  # Same file processed many times
            insights.append({
                'type': 'SUSPICIOUS_ACTIVITY',
                'pattern': 'REPEATED_FILE_PROCESSING',
                'file': job_list[0]['file_name'],
                'count': len(job_list),
                'recommendation': 'Investigate repeated processing of same file'
            })
    
    # Data compliance scoring
    encoding_compliance = sum(1 for job in jobs if job['encoding'] == 'UTF-8') / len(jobs) if jobs else 0
    header_compliance = sum(1 for job in jobs if 'All headers safe' in job['db_safe_headers']) / len(jobs) if jobs else 0
    
    compliance_score = (encoding_compliance + header_compliance) / 2 * 100
    insights.append({
        'type': 'COMPLIANCE_SCORE',
        'score': compliance_score,
        'encoding_compliance': encoding_compliance * 100,
        'header_safety_compliance': header_compliance * 100
    })
    
    return insights

def enhanced_parse_worker_logs(log_file_path):
    """Enhanced parsing with additional intelligence"""
    # Use the existing parse_worker_logs function
    jobs = parse_worker_logs(log_file_path)
    
    # Add calculated fields
    for job in jobs:
        if job['status'] == 'SUCCESS':
            job['data_quality_score'] = calculate_data_quality_score(job)
            job['processing_efficiency'] = int(job['records']) / job['total_time'] if job['total_time'] > 0 else 0
        else:
            # Add default values for failed jobs
            job['data_quality_score'] = 0
            job['processing_efficiency'] = 0
    
    return jobs

def write_enhanced_analysis(jobs, output_file):
    """Write comprehensive analysis including new metrics"""
    # Standard analysis
    write_worker_analysis(jobs, output_file)
    
    # Generate additional analysis files
    base_path = Path(output_file).parent
    
    # Performance insights
    performance_metrics = calculate_processing_efficiency_metrics(jobs)
    with open(base_path / 'performance_metrics.json', 'w') as f:
        json.dump(performance_metrics, f, indent=2)
    
    # Business metrics
    business_metrics = generate_business_impact_metrics(jobs)
    with open(base_path / 'business_metrics.json', 'w') as f:
        json.dump(business_metrics, f, indent=2)
    
    # Failure analysis
    failure_patterns = analyze_failure_patterns(jobs)
    with open(base_path / 'failure_analysis.json', 'w') as f:
        json.dump(failure_patterns, f, indent=2, default=str)
    
    # Predictive insights
    predictions = generate_predictive_insights(jobs)
    with open(base_path / 'predictive_insights.json', 'w') as f:
        json.dump(predictions, f, indent=2)
    
    # Security insights
    security = generate_security_insights(jobs)
    with open(base_path / 'security_analysis.json', 'w') as f:
        json.dump(security, f, indent=2)
    
    # Anomalies
    anomalies = detect_performance_anomalies(jobs)
    with open(base_path / 'anomalies.json', 'w') as f:
        json.dump(anomalies, f, indent=2)

def generate_executive_summary(jobs):
    """Generate C-level executive summary"""
    total_jobs = len(jobs)
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    
    summary = {
        'executive_summary': {
            'system_health': 'HEALTHY' if len(successful_jobs)/total_jobs >= 0.95 else 'DEGRADED' if len(successful_jobs)/total_jobs >= 0.80 else 'CRITICAL',
            'availability_sla': len(successful_jobs)/total_jobs * 100,
            'total_data_processed': f"{sum(int(job['records']) for job in successful_jobs):,} records",
            'average_processing_time': f"{statistics.mean([job['total_time'] for job in successful_jobs]):.2f}s" if successful_jobs else "N/A",
            'cost_efficiency_score': min(100, max(0, 100 - estimate_processing_cost(successful_jobs) * 1000)),  # Scaled for display
            'data_quality_grade': get_quality_grade(jobs),
            'key_recommendations': generate_top_recommendations(jobs)
        }
    }
    
    return summary

def get_quality_grade(jobs):
    """Convert quality scores to letter grades"""
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    if not successful_jobs:
        return 'F'
    
    quality_scores = [calculate_data_quality_score(job) for job in successful_jobs]
    avg_score = statistics.mean(quality_scores)
    
    if avg_score >= 90: return 'A'
    elif avg_score >= 80: return 'B'
    elif avg_score >= 70: return 'C'
    elif avg_score >= 60: return 'D'
    else: return 'F'

def generate_top_recommendations(jobs):
    """Generate top 3 actionable recommendations"""
    recommendations = []
    
    # Analyze patterns and generate smart recommendations
    error_jobs = [job for job in jobs if job['status'] == 'ERROR']
    if len(error_jobs) / len(jobs) > 0.1:
        recommendations.append("Implement pre-processing validation to reduce 10%+ failure rate")
    
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    if successful_jobs:
        avg_time = statistics.mean(job['total_time'] for job in successful_jobs)
        if avg_time > 5:
            recommendations.append("Optimize processing pipeline - average 5+ second processing time")
        
        quality_scores = [calculate_data_quality_score(job) for job in successful_jobs]
        if statistics.mean(quality_scores) < 80:
            recommendations.append("Implement data quality gates - current average below 80%")
    
    return recommendations[:3]

def load_jobs_from_csv(csv_file):
    """Load jobs from CSV with proper type conversion"""
    jobs = []
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            jobs = list(reader)
            # Convert numeric fields
            for job in jobs:
                for field in ['total_time', 'download_time', 'analysis_time', 'copying_time', 
                              'indexing_time', 'formulae_time', 'metadata_time']:
                    job[field] = float(job[field]) if job[field] else 0.0
                for field in ['records', 'rows_copied', 'columns_indexed']:
                    job[field] = int(job[field]) if job[field] else 0
    except FileNotFoundError:
        print("Worker analysis file not found")
        sys.exit(1)
    return jobs

def main():
    if len(sys.argv) < 2:
        print("Usage: python log_analyzer.py <command> [args...]")
        print("Commands:")
        print("  analyze <log_file> <output_csv>")
        print("  insights <worker_csv>")
        print("  file-insight <worker_csv> <filename>")
        print("  executive-summary <worker_csv>")
        print("  anomalies <worker_csv>")
        print("  business-metrics <worker_csv>")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "analyze":
        if len(sys.argv) < 4:
            print("Usage: python log_analyzer.py analyze <log_file> <output_csv>")
            sys.exit(1)
        
        log_file = sys.argv[2]
        output_csv = sys.argv[3]
        
        jobs = enhanced_parse_worker_logs(log_file)
        write_enhanced_analysis(jobs, output_csv)
        print(f"Enhanced analysis complete: {len(jobs)} jobs processed")
        
    elif command == "insights":
        if len(sys.argv) < 3:
            print("Usage: python log_analyzer.py insights <worker_csv>")
            sys.exit(1)
        
        worker_csv = sys.argv[2]
        
        jobs = load_jobs_from_csv(worker_csv)
        insights = generate_performance_insights(jobs)
        for insight in insights:
            print(insight)
    
    elif command == "file-insight":
        if len(sys.argv) < 4:
            print("Usage: python log_analyzer.py file-insight <worker_csv> <filename>")
            sys.exit(1)
        
        worker_csv = sys.argv[2]
        filename = sys.argv[3]
        
        jobs = load_jobs_from_csv(worker_csv)
        insight = get_worker_insight_for_file(jobs, filename)
        print(insight)
        
    elif command == "executive-summary":
        if len(sys.argv) < 3:
            print("Usage: python log_analyzer.py executive-summary <worker_csv>")
            sys.exit(1)
        
        jobs = load_jobs_from_csv(sys.argv[2])
        summary = generate_executive_summary(jobs)
        print(json.dumps(summary, indent=2))
        
    elif command == "anomalies":
        if len(sys.argv) < 3:
            print("Usage: python log_analyzer.py anomalies <worker_csv>")
            sys.exit(1)
            
        jobs = load_jobs_from_csv(sys.argv[2])
        anomalies = detect_performance_anomalies(jobs)
        for anomaly in anomalies:
            print(f"ANOMALY: {anomaly['file']} took {anomaly['actual_time']:.2f}s ({anomaly['deviation_factor']:.1f}x expected)")
    
    elif command == "business-metrics":
        if len(sys.argv) < 3:
            print("Usage: python log_analyzer.py business-metrics <worker_csv>")
            sys.exit(1)
            
        jobs = load_jobs_from_csv(sys.argv[2])
        metrics = generate_business_impact_metrics(jobs)
        for metric, value in metrics.items():
            if isinstance(value, float):
                print(f"{metric}: {value:.3f}")
            else:
                print(f"{metric}: {value}")
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()
