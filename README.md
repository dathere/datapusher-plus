[CKAN Service Provider]: https://github.com/ckan/ckan-service-provider
[Messytables]: https://github.com/okfn/messytables
[qsv]: https://github.com/dathere/qsv#qsv-ultra-fast-csv-data-wrangling-toolkit

# DataPusher+

> NOTE: v2 is a major revamp. Documentation is currently WIP.

DataPusher+ is a fork of [Datapusher](https://github.com/ckan/datapusher) that combines the speed and robustness of [ckanext-xloader](https://github.com/ckan/ckanext-xloader) with the data type guessing of Datapusher - super-powered with the ability to infer, calculate & suggest metadata using Jinja2 formulas defined in the scheming configuration file.

The Formulas have access to not just the `package` and `resource` fields (in the same namespaces), it also has access to the following information in these additional namespaces that can be used in Jinja2 expressions:
* `dpps` - with the "s" for stats.<br/>Each field will have an extensive list of summary statistics (by default: 
type, is_ascii, sum, min/max, range, sort_order, sortiness, min_length, max_length, sum_length, avg_length, stddev_length, variance_length, cv_length, mean, sem, geometric_mean, harmonic_mean, stddev, variance, cv, nullcount, max_precision, sparsity, cardinality, uniqueness_ratio.) Check [here](https://github.com/dathere/qsv/wiki/Supplemental#stats-command-output-explanation) for all other available statistics.
* `dppf` - with the "f" for frequency table.<br/>Each field will have its frequency table available sorted in descending order the top N (configurable, default 10) values, with a corresponding count & percentage. "Other (COUNT)" will be used as a "basket" for other values with COUNT set to the count of other values beyond the top N. ID fields will be indicated by "<ALL_UNIQUE>" in the table.
* `dpp` - additional inferred/calculated metadata.<br/>
  * `ORIGINAL_FILE_SIZE` (bytes)
  * `PREVIEW_FILE_SIZE` (bytes)
  * `RECORD_COUNT` (int)
  * `PREVIEW_RECORD_COUNT` (int)
  * `IS_SORTED` (bool)
  * `DEDUPED` (bool)
  * `DUPE_COUNT` (int: -1 if there are no dupes)
  * Date/DateTime metadata<br/>
    DP+ can infer date/datetime columns - supporting 19 different formats. As it is a relatively expensive operation, it will only do so for candidate columns with names that fit a configurable pattern.
      * `DATE_FIELDS` - a list of inferred date columns
      * `NO_DATE_FIELDS` (bool)
      * `DATETIME_FIELDS` - a list of inferred datetime columns
      * `NO_DATETIME_FIELDS` (bool)
  * Latitude/Longitude metadata<br/>
    DP+ can infer the latitude and longitude columns based on the column's characteristics. A column is inferred to be a latitude/longitude column if:
      * its in a comma-separated priority-order list of lat/long name patterns
      * for latitude, if its of type "Float" with a range of -90.0 to 90.0, and
      * for longitude, if its a "Float" with a range of -180.0 to 180.0.
    * `LAT_FIELD` and `LON_FIELD` - lat/long columns
    * `NO_LAT_LONG_FIELDS` (bool)

Beyond the extensive list of built-in Jinja2 [filters](https://jinja.palletsprojects.com/en/stable/templates/#list-of-builtin-filters)/[functions](https://jinja.palletsprojects.com/en/stable/templates/#list-of-global-functions), DP+ also supports an extensive list of additional [custom filters/functions](https://github.com/dathere/datapusher-plus/blob/607e7c5e5d75c5dc7ac55d684522c7972bc33d1d/ckanext/datapusher_plus/jinja2_helpers.py#L171).

There are two Formula types that are indicated by adding these keywords to the scheming yaml file:
 * `formula` - the formula will be evaluated at resource creation/update time and the result is assigned to the corresponding package/resource field immediately.
 * `suggest_formula` - the formula will be evaluated at resource creation/update time and the result is stored in the `dpp_suggestions` package field as a compound JSON object. `dpp_suggestions` contains all the suggestion for both package and resource fields. This field is parsed to show "Suggestions" during metadata entry for the associated package/resource field using the Suggestion UI (indicated by a function symbol next to the metadata field name).

 Formulas that fail to evaluate will return with the `#ERROR!:` (reminiscent of Excel's `#VALUE!` function error) prefix followed by a detailed Jinja2 error message.

In addition, Datapusher+ is no longer a webservice, but a full-fledged CKAN extension. It drops usage of the deprecated [CKAN Service Provider][], with the unmaintained [Messytables] replaced by the blazing-fast [qsv] data-wrangling engine.

[TNRIS](https://tnris.org)/[TWDB](https://www.twdb.texas.gov/) provided the use cases that informed and supported the development
of Datapusher+, specifically, to support a [Resource-first upload workflow](docs/RESOURCE_FIRST_WORKFLOW.md#Resource-first-Upload-Workflow).

For a more detailed overview, see the [CKAN Monthly Live Jan 2023 presentation](https://docs.google.com/presentation/d/e/2PACX-1vT0BfmrrtaEINRGg4UI_m7B02_X6HlFr4yN_DXmgX9goVtgu2DNmZjl-SowL9ZA2ibQhDjScRRJh95q/pub?start=false&loop=false&delayms=3000).

It features:

* **"Bullet-proof", ultra-fast data type inferencing with qsv**

  Unlike [Messytables][] which scans only the the first few rows to guess the type of
  a column, [qsv][] scans the entire table so its data type inferences are guaranteed[^1].

  Despite this, qsv is still exponentially faster even if it scans the whole file, not
  only inferring data types, it also calculates [summary statistics](https://github.com/dathere/qsv/blob/b0fbd0e575e2e80f57f94ce916438edf9dc32859/src/cmd/stats.rs#L2-L18) as well. For example,
  [scanning a 2.7 million row, 124MB CSV file for types and stats took 0.16 seconds](https://github.com/dathere/qsv/blob/master/docs/whirlwind_tour.md#a-whirlwind-tour)[^2].

  It is very fast as qsv is written in [Rust](https://www.rust-lang.org/), is multithreaded,
  and uses all kinds of [performance techniques](https://github.com/dathere/qsv/blob/master/docs/PERFORMANCE.md#performance-tuning)
  especially designed for data-wrangling.

* **Exponentially faster loading speed**

  Similar to xloader, we use PostgreSQL COPY to directly pipe the data into the datastore,
  short-circuiting the additional processing/transformation/API calls used by Datapusher Plus.

  But unlike xloader, we load everything using the proper data types and not as text, so there's
  no need to reload the data again after adjusting the Data Dictionary, as you would with xloader.

* **Far more Storage Efficient AND Performant Datastore with easier to compose SQL queries**

  As we create the Datastore tables using the most efficient PostgreSQL data type for each column
  using qsv's guaranteed type inferences - the Datastore is not only more storage efficient, it is
  also far more more performant for loading AND querying.
  
  With its "smartint" data type (with qsv inferring the most efficient integer data type for the range of
  values in the column); comprehensive date format inferencing (supporting [19 date formats](https://github.com/jqnatividad/belt/tree/main/dateparser#accepted-date-formats), with each
  format having several variants & with configurable DMY/MDY preference parsing) & auto-formatting dates to
  RFC3339 format so they are stored as Postgres timestamps; cardinality-aware, configurable auto-indexing;
  automatic sanitization of column names to valid PostgreSQL column identifiers; auto PostgreSQL vacuuming &
  analysis of resources after loading; and more - DP+ enables the Datastore to tap into PostgreSQL's full power.

  Configurable auto-aliasing of resources also makes it easier to compose SQL queries, as you can
  use more intuitive resource aliases instead of cryptic resource IDs.

* **Production-ready Robustness**

  In production, the number one source of support issues is Datapusher - primarily, because of
  data quality issues and Datapusher's inability to correctly infer data types, gracefully handle
  errors[^3], and provide the Data Publisher actionable information to correct the data.

  Datapusher+'s design directly addresses all these issues.

* **More informative datastore loading messages**

  Datapusher+ messages are designed to be more verbose and actionable, so the data publisher's
  user experience is far better and makes it possible to have a resource-first upload workflow.

* **Extended preprocessing with qsv**

  qsv is leveraged by Datapusher+ to:

  * create "Smarter" Data Dictionaries, with:
    * guaranteed data type inferences
    * optional ability to automatically choose the best integer PostgreSQL data type ("smartint") based on the range of the numeric column ([PostgreSQL's int, bigint and numeric types](https://www.postgresql.org/docs/12/datatype-numeric.html)) for optimal storage/indexing efficiency and SQL query performance.
    * sanitized column names (guaranteeing valid PostgreSQL column identifiers) while preserving the original column name as a label, which is used to label columns in DataTables_view.
    * an optional "summary stats" resource as an extension of the Data Dictionary, with comprehensive summary statistics for each column - sum, min/max/range, min/max length, mean, stddev, variance, nullcount, sparsity, quartiles, IQR, lower/upper fences, skewness, median, mode/s, antimode/s & cardinality.
  * convert Excel & OpenOffice/LibreOffice Calc (ODS) files to CSV, with the ability to choose which sheet to use by default (e.g. 0 is the first sheet, -1 is the last sheet, -2 the second to last sheet, etc.)
  * convert various date formats ([19 date formats are recognized](https://github.com/jqnatividad/belt/tree/main/dateparser#accepted-date-formats) with each format having several variants; ~80 date format permutations in total) to a standard [RFC 3339](https://www.rfc-editor.org/rfc/rfc3339) format
  * enable random access of a CSV by creating a CSV index - which also enables parallel processing of different parts of a CSV simultaneously (a major reason type inferencing and stats calculation is so fast)
  * instantaneously count the number of rows with a CSV index
  * validate if an uploaded CSV conforms to the [RFC-4180](https://datatracker.ietf.org/doc/html/rfc4180) standard
  * normalizes and transcodes CSV/TSV dialects into a standard UTF-8 encoded RFC-4180 CSV format
  * optionally create a preview subset, with the ability to only download the first `n` preview rows of a file, and not the entire file (e.g. only download first 1,000 rows of 3 gb CSV file - especially good for harvesting/cataloging external sites where you only want to harvest the metadata and a small sample of each file).
  * optionally create a preview subset from the end of a file (e.g. last 1,000 rows, good for time-series/sensor data)
  * auto-index columns based on its cardinality/format (unique indices created for columns with all unique values, auto-index columns whose cardinality is below a given threshold; auto-index date columns)
  * check for duplicates, and optionally deduplicate rows
  * optionally screen for Personally Identifiable Information (PII), with an option to "quarantine" the PII-candidate rows in a separate resource, while still creating the screened resource.
  * optional ability to specify a custom PII screening regex set, instead of the [default PII screening regex set](https://github.com/dathere/datapusher-plus/blob/master/default-pii-regexes.txt).

  Even with all these pre-processing tasks, qsv typically takes less than 5 seconds to finish all its analysis tasks, even for a 100mb CSV file.

  Future versions of Datapusher+ will further leverage qsv's 80+ commands to do additional
  preprocessing, data-wrangling and validation. The Roadmap is available [here](https://github.com/dathere/datapusher-plus/issues/5).
  Ideas, suggestions and your feedback are most welcome!

[^1]: [Why use qsv instead of a "proper" python data analysis library like pandas?](https://github.com/dathere/datapusher-plus/discussions/15)
[^2]: It takes 0.16 seconds with an index to run `qsv stats` against the [qsv whirlwind tour sample file](https://raw.githubusercontent.com/wiki/dathere/qsv/files/wcp.zip) on a Ryzen 4800H (8 physical/16 logical cores) with 32 gb memory and a 1 TB SSD.
Without an index, it takes 1.3 seconds.
[^3]: Imagine you have a 1M row CSV, and the last row has an invalid value for a numeric column (e.g. "N/A" instead of a number).
      After spending hours pushing the data very slowly, legacy datapusher will abort on the last row and the ENTIRE job is invalid.
      Ok, that's bad, but what makes it worse is that the old table has been deleted already, and Datapusher doesn't tell you what
      caused the job to fail! YIKES!!!!

## Requirements:
Datapusher+ requires:
* CKAN 2.10+
* Python 3.10+
* tested and developed on Ubuntu 22.04.5
* [`ckan.datastore.sqlsearch.enabled`](https://docs.ckan.org/en/2.10/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_search_sql) set to `true` if you want to use the `temporal_resolution` and `guess_accrual_periodicity` Formula helpers

## Development Installation

Datapusher+ from version 1.0.0 onwards will be installed as a extension of CKAN, and will be available as a CKAN plugin. This will allow for easier integration with CKAN and other CKAN extensions.

1. Install the required packages.

    ```bash
    sudo apt install python3-virtualenv python3-dev python3-pip python3-wheel build-essential libxslt1-dev libxml2-dev zlib1g-dev git libffi-dev libpq-dev uchardet
    ```

2. Activate the CKAN virtual environment using at least python 3.10.

    ```bash
    . /usr/lib/ckan/default/bin/activate
    ```

3. Install the extension using following commands:

    ```bash
   pip install -e "git+https://github.com/dathere/datapusher-plus.git@2.0.0#egg=datapusher-plus"
    ```

4. Install the dependencies.

    ```bash
    pip install -r requirements.txt
    ```

5. Install [qsv](https://github.com/dathere/qsv).

    ## Option 1: Debian Package Installation (Easiest)

    If you are running Debian based Linux distribution on x86_64, you can quickly install qsv using the following commands:

    Add the qsv repository to your sources list:

      ```bash
      echo "deb [signed-by=/etc/apt/trusted.gpg.d/qsv-deb.gpg] https://dathere.github.io/qsv-deb-releases ./" > qsv.list
      ```

    Import trusted GPG key:

      ```bash
    wget -O - https://dathere.github.io/qsv-deb-releases/qsv-deb.gpg | sudo apt-key add -
      ```

    Install qsv:

      ```bash
      sudo apt update
      sudo apt install qsv
      ```

    ## Option 2: Install Prebuilt qsv Binaries (Easy)
    [Download the appropriate precompiled binaries](https://github.com/dathere/qsv/releases/latest) for your platform and copy it to the appropriate directory, e.g. for Ubuntu LTS 22.04 or 24.04:

    ```bash
    wget https://github.com/dathere/qsv/releases/download/4.0.0/qsv-4.0.0-x86_64-unknown-linux-gnu.zip
    unzip qsv-4.0.0-x86_64-unknown-linux-gnu.zip
    rm qsv-4.0.0-x86_64-unknown-linux-gnu.zip
    sudo mv qsv* /usr/local/bin
    ```

    If you get glibc errors when starting qsv, your Linux distro may not have the required version of the GNU C Library. If so, use the `qsv-4.0.0-unknown-linux-musl.zip` archive as it is statically linked with the MUSL C Library.


    > ℹ️ **NOTE:** qsv's prebuilt binaries have the ability to self-update to the latest version. Just run qsv with the `--update` option and it will check for the latest version and update itself as required.
    > ```
    > sudo qsvdp --update
    > ```

    ## Option 3: Build qsv from source 
    Finally, you can build qsvdp from source. It has the additional benefit that the resulting binary will take advantage of all the machine's CPU features, making qsv and DP+ even faster, but may take up to 30 minutes to compile.
    
    ```bash
    git clone https://github.com/dathere/qsv.git
    cd qsv

    # install Rust, if it's not installed
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

    # build qsvdp
    CARGO_BUILD_RUSTFLAGS='-C target-cpu=native' cargo build --release --locked --bin qsvdp -F datapusher_plus
    sudo cp target/release/qsvdp /usr/local/bin
    cargo clean
    ```

6. Create an API token for the DP+ Service account. 
   Replace `CKAN_ADMIN` with an existing CKAN user with sysadmin privileges.

    ```
    ckan config-tool /etc/ckan/default/ckan.ini "ckanext.datapusher_plus.api_token=$(ckan -c /etc/ckan/default/ckan.ini user token add CKAN_ADMIN dpplus | tail -n 1 | tr -d '\t')"
    ```

## Configuring

### CKAN Configuration

Add `datapusher_plus` to the plugins in your CKAN configuration file
(generally located at `/etc/ckan/default/ckan.ini`):

```ini
ckan.plugins = <other plugins> datapusher_plus
```


>```ini
> ckanext.datapusher_plus.use_proxy = false
> ckanext.datapusher_plus.download_proxy = 
> ckanext.datapusher_plus.ssl_verify = false
> # supports INFO, DEBUG, TRACE - use DEBUG or TRACE when debugging scheming Formulas
> ckanext.datapusher_plus.upload_log_level = INFO
> ckanext.datapusher_plus.formats = csv tsv tab ssv xls xlsx xlsxb xlsm ods geojson shp qgis zip
> ckanext.datapusher_plus.pii_screening = false
> ckanext.datapusher_plus.pii_found_abort = false
> ckanext.datapusher_plus.pii_regex_resource_id_or_alias =
> ckanext.datapusher_plus.pii_show_candidates = false
> ckanext.datapusher_plus.pii_quick_screen = false
> ckanext.datapusher_plus.qsv_bin = /usr/local/bin/qsvdp
> ckanext.datapusher_plus.preview_rows = 100
> ckanext.datapusher_plus.download_timeout = 300
> ckanext.datapusher_plus.max_content_length = 1256000000000
> ckanext.datapusher_plus.chunk_size = 16384
> ckanext.datapusher_plus.default_excel_sheet = 0
> ckanext.datapusher_plus.sort_and_dupe_check = true
> ckanext.datapusher_plus.dedup = false
> ckanext.datapusher_plus.unsafe_prefix = unsafe_
> ckanext.datapusher_plus.reserved_colnames = _id
> ckanext.datapusher_plus.prefer_dmy = false
> ckanext.datapusher_plus.ignore_file_hash = true
> ckanext.datapusher_plus.auto_index_threshold = 3
> ckanext.datapusher_plus.auto_index_dates = true
> ckanext.datapusher_plus.auto_unique_index = true
> ckanext.datapusher_plus.summary_stats_options =
> ckanext.datapusher_plus.add_summary_stats_resource = false
> ckanext.datapusher_plus.summary_stats_with_preview = false
> ckanext.datapusher_plus.qsv_stats_string_max_length = 32767
> ckanext.datapusher_plus.qsv_dates_whitelist = date,time,due,open,close,created
> ckanext.datapusher_plus.qsv_freq_limit = 10
> ckanext.datapusher_plus.auto_alias = true
> ckanext.datapusher_plus.auto_alias_unique = false
> ckanext.datapusher_plus.copy_readbuffer_size = 1048576
> ckanext.datapusher_plus.type_mapping = {"String": "text", "Integer": "numeric","Float": "numeric","DateTime": "timestamp","Date": "date","NULL": "text"}
> ckanext.datapusher_plus.auto_spatial_simplication = true
> ckanext.datapusher_plus.spatial_simplication_relative_tolerance = 0.1
> ckanext.datapusher_plus.latitude_fields = latitude,lat
> ckanext.datapusher_plus.longitude_fields = longitude,long,lon
> ckanext.datapusher_plus.jinja2_bytecode_cache_dir = /tmp/jinja2_butecode_cache
> ckanext.datapusher_plus.auto_unzip_one_file = true
> ckanext.datapusher_plus.api_token = <CKAN service account token for CKAN user with sysadmin privileges>
>ckanext.datapusher_plus.describeGPT_api_key = <Token for OpenAI API compatible service>
>```
>
>and add this entry to your CKAN's `resource_formats.json` file.
>
>```json
> ["TAB", "Tab Separated Values File", "text/tab-separated-values", []],
>```

## Usage

Any file that has one of the supported formats (defined in `ckanext.datapusher_plus.formats`) will be attempted to be loaded into the DataStore.

You can also manually trigger resources to be resubmitted. When editing a resource in CKAN (clicking the "Manage" button on a resource page), a new tab named "DataStore" will appear. This will contain a log of the last attempted upload and a button to retry the upload. Once a resource has been "pushed" into the Datastore, a "Data Dictionary" tab will also be available where the data pusblisher can fine-tune the inferred data dictionary.

![DataPusher+ UI](images/datapusher-plus-scn1.png)
![DataPusher+ UI 2](images/datapusher-plus-scn2.png)

### Command line

Run the following command to submit all resources to datapusher, although it will skip files whose hash of the data file has not changed:

``` bash
    ckan -c /etc/ckan/default/ckan.ini datapusher_plus resubmit
```

To Resubmit a specific resource, whether or not the hash of the data file has changed:

``` bash
    ckan -c /etc/ckan/default/ckan.ini datapusher_plus submit {dataset_id}
```

## License

This material is copyright (c) 2025, datHere, Open Knowledge Foundation and other contributors

It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
whose full text may be found at:

http://www.fsf.org/licensing/licenses/agpl-3.0.html
