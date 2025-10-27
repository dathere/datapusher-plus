
**Automated CI Workflow**: [`main.yml`](https://github.com/dathere/datapusher-plus/actions/workflows/main.yml)
- Full integration test with CKAN 2.11, PostgreSQL, Solr, and Redis
- Tests file uploads, processing pipeline, and datastore imports
- Generates analytics via `log_analyzer.py`

> [!TIP]
>**Standalone Testing**: [datapusher-plus_testing](https://github.com/dathere/datapusher-plus_testing)
>- Dedicated repository for custom test scenarios
>- Same workflow, user-defined test files in `/custom` directory
>- Manual trigger via GitHub Actions workflow_dispatch
## Technical Reference
### Time Measurements
- All `time` values in the worker_analysis.csv are measured in **seconds**

### Data Quality Scoring Algorithm
Base score: 100, with penalties applied:
- Invalid CSV: -30 points
- Unsorted data: -10 points
- Unsafe headers: -5 points per unsafe header (max -25)
- Failed normalization: -20 points
- Failed analysis: -25 points
- UTF-8 encoding: +5 points
- `>1000 records: +5 points`

### Performance Anomaly Detection
- Uses statistical analysis (mean + 2 standard deviations)
- Identifies jobs with processing times significantly above normal
- Requires minimum 3 successful jobs for analysis


### CSV Output Schema

#### Primary Fields
| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | String | Job start timestamp (YYYY-MM-DD HH:MM:SS) |
| `job_id` | String | UUID of the processing job |
| `file_name` | String | Name of the processed file |
| `status` | String | SUCCESS, ERROR, or INCOMPLETE |
| `qsv_version` | String | Version of QSV tool used |
| `file_format` | String | Detected file format (CSV, XLSX, etc.) |
| `encoding` | String | File character encoding |
| `normalized` | String | "Successful" or "Failed" |
| `valid_csv` | String | "TRUE" or "FALSE" |
| `sorted` | String | "TRUE", "FALSE", or "UNKNOWN" |
| `db_safe_headers` | String | Header safety status |
| `analysis` | String | "Successful" or "Failed" |
| `records` | Integer | Number of records detected |

#### Timing Fields (all in seconds)
| Column | Type | Description |
|--------|------|-------------|
| `total_time` | Float | Total processing time |
| `download_time` | Float | File download time |
| `analysis_time` | Float | Analysis phase time |
| `copying_time` | Float | Database copy time |
| `indexing_time` | Float | Index creation time |
| `formulae_time` | Float | Formula processing time |
| `metadata_time` | Float | Metadata update time |
