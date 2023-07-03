[CKAN Service Provider]: https://github.com/ckan/ckan-service-provider
[Messytables]: https://github.com/okfn/messytables
[qsv]: https://github.com/jqnatividad/qsv#qsv-ultra-fast-csv-data-wrangling-toolkit

# DataPusher+

DataPusher+ is a fork of [Datapusher](https://github.com/ckan/datapusher) that combines the speed and robustness of
[ckanext-xloader](https://github.com/ckan/ckanext-xloader) with the data type guessing of Datapusher.

Datapusher+ is built using [CKAN Service Provider][], with [Messytables] replaced by [qsv].

[TNRIS](https://tnris.org)/[TWDB](https://www.twdb.texas.gov/) provided the use cases that informed and supported the development
of Datapusher+, specifically, to support a [Resource-first upload workflow](docs/RESOURCE_FIRST_WORKFLOW.md#Resource-first-Upload-Workflow).

For a more detailed overview, see the [CKAN Monthly Live Jan 2023 presentation](https://docs.google.com/presentation/d/e/2PACX-1vT0BfmrrtaEINRGg4UI_m7B02_X6HlFr4yN_DXmgX9goVtgu2DNmZjl-SowL9ZA2ibQhDjScRRJh95q/pub?start=false&loop=false&delayms=3000).

It features:

* **"Bullet-proof", ultra-fast data type inferencing with qsv**

  Unlike [Messytables][] which scans only the the first few rows to guess the type of
  a column, [qsv][] scans the entire table so its data type inferences are guaranteed[^1].

  Despite this, qsv is still exponentially faster even if it scans the whole file, not
  only inferring data types, it also calculates [summary statistics](https://github.com/jqnatividad/qsv/blob/b0fbd0e575e2e80f57f94ce916438edf9dc32859/src/cmd/stats.rs#L2-L18) as well. For example,
  [scanning a 2.7 million row, 124MB CSV file for types and stats took 0.16 seconds](https://github.com/jqnatividad/qsv/blob/master/docs/whirlwind_tour.md#a-whirlwind-tour)[^2].

  It is very fast as qsv is written in [Rust](https://www.rust-lang.org/), is multithreaded,
  and uses all kinds of [performance techniques](https://github.com/jqnatividad/qsv/blob/master/docs/PERFORMANCE.md#performance-tuning)
  especially designed for data-wrangling.

* **Exponentially faster loading speed**

  Similar to xloader, we use PostgreSQL COPY to directly pipe the data into the datastore,
  short-circuiting the additional processing/transformation/API calls used by Datapusher.

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
[^2]: It takes 0.16 seconds with an index to run `qsv stats` against the [qsv whirlwind tour sample file](https://raw.githubusercontent.com/wiki/jqnatividad/qsv/files/wcp.zip) on a Ryzen 4800H (8 physical/16 logical cores) with 32 gb memory and a 1 TB SSD.
Without an index, it takes 1.3 seconds.
[^3]: Imagine you have a 1M row CSV, and the last row has an invalid value for a numeric column (e.g. "N/A" instead of a number).
      After spending hours pushing the data very slowly, legacy datapusher will abort on the last row and the ENTIRE job is invalid.
      Ok, that's bad, but what makes it worse is that the old table has been deleted already, and Datapusher doesn't tell you what
      caused the job to fail! YIKES!!!!

## Development Installation

Datapusher+ is a drop-in replacement for Datapusher, so it's installed the same way.

1. Install the required packages.

    ```bash
    sudo apt install python3-virtualenv python3-dev python3-pip python3-wheel build-essential libxslt1-dev libxml2-dev zlib1g-dev git libffi-dev libpq-dev file
    ```

2. Create a virtual environment for Datapusher+ using at least python 3.8.

    ```bash
    cd /usr/lib/ckan
    sudo python3.8 -m venv dpplus_venv
    sudo chown -R $(whoami) dpplus_venv
    . dpplus_venv/bin/activate
    cd dpplus_venv
    ```

    > ℹ️ **NOTE:** DP+ requires at least python 3.8 as it makes extensive use of new capabilities introduced in 3.7/3.8
    > to the [subprocess module](https://docs.python.org/3.8/library/subprocess.html).
    > If you're using Ubuntu 18.04 or earlier, follow the procedure below to install python 3.8:
    >
    > ```bash
    > sudo add-apt-repository ppa:deadsnakes/ppa
    > # we use 3.8 here, but you can get a higher version by changing the version suffix of the packages below
    > sudo apt install python3.8 python3.8-venv python3.8-dev
    > # install additional dependencies
    > sudo apt install build-essential libxslt1-dev libxml2-dev zlib1g-dev git libffi-dev
    > ```
    >
    > Note that DP+ still works with CKAN<=2.8, which uses older versions of python.

3. Get the code.

    ```bash
    mkdir src
    cd src
    git clone --branch 0.11.0 https://github.com/datHere/datapusher-plus
    cd datapusher-plus
    ```

4. Install the dependencies.

    ```bash
    pip install wheel
    pip install -r requirements-dev.txt
    pip install -e .
    ```

5. Install [qsv](https://github.com/jqnatividad/qsv).

    [Download the appropriate precompiled binaries](https://github.com/jqnatividad/qsv/releases/latest) for your platform and copy
    it to the appropriate directory, e.g. for Linux:

    ```bash
    wget https://github.com/jqnatividad/qsv/releases/download/0.108.0/qsv-0.108.0-x86_64-unknown-linux-gnu.zip
    unzip qsv-0.108.0-x86_64-unknown-linux-gnu.zip
    rm qsv-0.108.0-x86_64-unknown-linux-gnu.zip
    sudo mv qsv* /usr/local/bin
    ```

    Alternatively, if you want to install qsv from source, follow
    the instructions [here](https://github.com/jqnatividad/qsv#installation). Note that when compiling from source,
    you may want to look into the [Performance Tuning](https://github.com/jqnatividad/qsv#performance-tuning)
    section to squeeze even more performance from qsv.

    Also, if you get glibc errors when starting qsv, your Linux distro may not have the required version of the GNU C Library
    (This will be the case when running Ubuntu 18.04 or older).
    If so, use the `qsvdp_glibc-2.31` binary as its linked to an older version of glibc. If that still fails, the use the
    `unknown-linux-musl.zip` archive as it is statically linked with the MUSL C Library.

    If you already have qsv, update it to the latest release by using the --update option.

    `qsvdp --update`

    > ℹ️ **NOTE:** qsv is a general purpose CSV data-wrangling toolkit that gets regular updates. To update to the latest version, just run
    qsv with the `--update` option and it will check for the latest version and update as required.

6. Configure the Datapusher+ database.

   Make sure to create the `datapusher` PostgreSQL user and the `datapusher_jobs` database
   (see [DataPusher+ Database Setup](#datapusher-database-setup)).

7. Copy the `datapusher/dot-env.template` to `datapusher/.env` and [modify your configuration](#datapusher-configuration).

    ```bash
    cd /usr/lib/ckan/dpplus_env/src/datapusher-plus/datapusher
    cp dot-env.template .env
    # configure your installation as required
    nano .env
    ```

8. Run Datapusher+ in the `dpplus_venv` virtual environment.

    ```bash
    python main.py config.py
    ```

    By default, DP+ should be running at the following port:

    http://localhost:8800/

## Production Deployment

There are two ways to deploy Datapusher+:

1. Manual Deployment

    These instructions set up the DataPusher web service on
    [uWSGI](https://uwsgi-docs.readthedocs.io/en/latest/) running on port 8800, but
    can be easily adapted to other WSGI servers like Gunicorn. You'll probably need
    to set up Nginx as a reverse proxy in front of it and something like Supervisor
    to keep the process up.

    ```bash
    # Install requirements for DataPusher+. Be sure to have at least Python 3.8
    sudo apt install python3-virtualenv python3-dev python3-pip python3-wheel build-essential libxslt1-dev libxml2-dev zlib1g-dev git libffi-dev libpq-dev file

    # Install qsv, if required
    wget https://github.com/jqnatividad/qsv/releases/download/0.108.0/qsv-0.108.0-x86_64-unknown-linux-gnu.zip -P /tmp
    unzip /tmp/qsv-0.108.0-x86_64-unknown-linux-gnu.zip -d /tmp
    rm /tmp/qsv-0.108.0-x86_64-unknown-linux-gnu.zip
    sudo mv /tmp/qsv* /usr/local/bin

    # if qsv is already installed, be sure to update it to the latest release
    sudo qsvdp --update

    # if you get a glibc error when running `qsvdp --update`
    # you're on an old distro (e.g. Ubuntu 18.04) without the required version of the glibc libraries.
    # If so, try running the qsvdp_glibc-2.31 binary instead. If it runs, you can use it instead of the default qsvdp binary.
    # If that still doesnt work, use the statically linked MUSL version instead
    # https://github.com/jqnatividad/qsv/releases/download/0.108.0/qsv-0.108.0-x86_64-unknown-linux-musl.zip

    # find out the locale settings
    locale

    # ONLY IF LANG is not "en_US.UTF-8", set locale
    export LC_ALL="en_US.UTF-8"
    export LC_CTYPE="en_US.UTF-8"
    sudo dpkg-reconfigure locales

    # Create a virtualenv for DataPusher+. DP+ requires at least python 3.8.
    sudo python3.8 -m venv /usr/lib/ckan/dpplus_venv
    sudo chown -R $(whoami) dpplus_venv

    # install datapusher-plus in the virtual environment
    . /usr/lib/ckan/dpplus_venv/bin/activate
    pip install wheel
    pip install datapusher-plus

    # create an .env file and tune DP+ settings. Tune the uwsgi.ini file as well
    sudo mkdir -p /etc/ckan/datapusher-plus
    sudo curl https://raw.githubusercontent.com/dathere/datapusher-plus/master/datapusher/dot-env.template -o /etc/ckan/datapusher-plus/.env
    sudo curl https://raw.githubusercontent.com/dathere/datapusher-plus/master/deployment/datapusher-uwsgi.ini -o /etc/ckan/datapusher-plus/uwsgi.ini

    # Be sure to initialize the database if required. (See Database Setup section below)
    # Be sure to edit the .env file and set the right database connect strings!

    # Create a user to run the web service (if necessary)
    sudo addgroup www-data
    sudo adduser -G www-data www-data
    ```

    At this point you can run DataPusher+ with the following command:

    ```bash
    /usr/lib/ckan/dpplus_venv/bin/uwsgi --enable-threads -i /etc/ckan/datapusher-plus/uwsgi.ini
    ```

    You might need to change the `uid` and `guid` in the `uwsgi.ini` file when using a different user.

    To deploy it using supervisor:

    ```bash
    sudo curl https://raw.githubusercontent.com/dathere/datapusher-plus/master/deployment/datapusher-uwsgi.conf -o /etc/supervisor/conf.d/datapusher-uwsgi.conf
    sudo service supervisor restart
    ```

2. Dockerized Deployment

    As Datapusher+ is quite involved as evinced by the above procedure, a containerized installation
    will make it far easier not only to deploy DP+ to production, but also to experiment with.

    Instructions to set up the DP+ Docker instance can be found [here](https://github.com/dathere/datapusher-plus-docker).

    The DP+ Docker will also expose additional features and administrative interface to manage
    not only Datapusher+ jobs, but also to manage the CKAN Datastore.

## Configuring


### CKAN Configuration

Add `datapusher` to the plugins in your CKAN configuration file
(generally located at `/etc/ckan/default/ckan.ini`):

```ini
ckan.plugins = <other plugins> datapusher
```

In order to tell CKAN where this webservice is located, the following must be
added to the `[app:main]` section of your CKAN configuration file :

```ini
ckan.datapusher.url = http://127.0.0.1:8800/
```

There are other CKAN configuration options that allow to customize the CKAN - DataPusher
integration. Please refer to the [DataPusher Settings](https://docs.ckan.org/en/latest/maintaining/configuration.html#datapusher-settings) section in the CKAN documentation for more details.

> ℹ️ **NOTE:** DP+ recognizes some additional TSV and spreadsheet subformats - `xlsm` and `xlsb` for Excel Spreadsheets,
> and `tab` for TSV files. To process these subformats, set `ckan.datapusher.formats` as follows in your CKAN.INI file:
>
>```ini
> ckan.datapusher.formats = csv xls xlsx xlsm xlsb tsv tab application/csv application/vnd.ms-excel application/vnd.openxmlformats-officedocument.spreadsheetml.sheet ods application/vnd.oasis.opendocument.spreadsheet
>```
>
>and add this entry to your CKAN's `resource_formats.json` file.
>
>```json
> ["TAB", "Tab Separated Values File", "text/tab-separated-values", []],
>```


### DataPusher+ Configuration

The DataPusher+ instance is configured in the `.env` file located in the working directory of DP+
(`/etc/ckan/datapusher-plus` when running a production deployment. The `datapusher-plus/datapusher`
source directory when running a development installation.)

See [dot-env.template](datapusher/dot-env.template) for a summary of configuration options available.


### DataPusher+ Database Setup

DP+ requires a dedicated PostgreSQL account named `datapusher` to connect to the CKAN Datastore.

To create the `datapusher` user and give it the required privileges to the `datastore_default` database:

```bash
su - postgres
psql -d datastore_default
CREATE ROLE datapusher LOGIN PASSWORD 'YOURPASSWORD';
GRANT CREATE, CONNECT, TEMPORARY, SUPERUSER ON DATABASE datastore_default TO datapusher;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public TO datapusher;
\q
```

DP+ also requires its own job_store database to keep track of all the DP+ jobs. In the original Datapusher,
this was a sqlite database by default. Though DP+ can still use a sqlite database, we are discouraging its use.

To setup the `datapusher_jobs` database and its user:

```bash
sudo -u postgres createuser -S -D -R -P datapusher_jobs
sudo -u postgres createdb -O datapusher_jobs datapusher_jobs -E utf-8
```

## Usage

Any file that has one of the supported formats (defined in [`ckan.datapusher.formats`](https://docs.ckan.org/en/latest/maintaining/configuration.html#ckan-datapusher-formats)) will be attempted to be loaded
into the DataStore.

You can also manually trigger resources to be resubmitted. When editing a resource in CKAN (clicking the "Manage" button on a resource page), a new tab named "DataStore" will appear. This will contain a log of the last attempted upload and a button to retry the upload. Once a resource has been "pushed" into the Datastore, a "Data Dictionary" tab will also be available where the data pusblisher can fine-tune the inferred data dictionary.

![DataPusher+ UI](images/datapusher-plus-scn1.png)
![DataPusher+ UI 2](images/datapusher-plus-scn2.png)

### Command line

Run the following command to submit all resources to datapusher, although it will skip files whose hash of the data file has not changed:

    ckan -c /etc/ckan/default/ckan.ini datapusher resubmit

On CKAN<=2.8:

    paster --plugin=ckan datapusher resubmit -c /etc/ckan/default/ckan.ini

To Resubmit a specific resource, whether or not the hash of the data file has changed::

    ckan -c /etc/ckan/default/ckan.ini datapusher submit {dataset_id}

On CKAN<=2.8:

    paster --plugin=ckan datapusher submit <pkgname> -c /etc/ckan/default/ckan.ini

### Testing
To test Datapusher-plus, you can use the following test script available on GitHub: [test script](https://github.com/dathere/testing-datapusher-plus).



### Uninstalling Datapusher+

Should you need to remove Datapusher+, and you followed either the Development or Production Installation procedures above:

```bash
# if you're running inside the dpplus_venv virtual environment, deactivate it first
deactivate

# remove the DP+ python virtual environment
sudo rm -rf /usr/lib/ckan/dpplus_venv

# remove the supervisor DP+ configuration
sudo rm -rf /etc/supervisor/conf.d/datapusher-uwsgi.conf

# remove the DP+ production deployment directory
sudo rm -rf /etc/ckan/datapusher-plus

# remove qsv binary variants
sudo rm /usr/local/bin/qsv /usr/local/bin/qsvdp /usr/local/bin/qsvlite /usr/local/bin/qsv_nightly /usr/local/bin/qsvdp_nightly /usr/local/bin/qsvlite_nightly

# restart the supervisor, without the Datapusher+ service
sudo service supervisor reload

# ========= DATABASE objects ============
# OPTIONAL: backup the datapusher_jobs database first if 
# you want to retain the DP+ job history
sudo -u postgres pg_dump --format=custom -d datapusher_jobs > datapusher_jobs.dump

# to remove the Datapusher+ job database and the datapusher_jobs user/role
sudo -u postgres dropdb datapusher_jobs
sudo -u postgres dropuser datapusher_jobs

# to drop the datapusher user which DP+ uses to write to the CKAN Datastore
sudo -u postgres dropuser datapusher
```

To ensure the Datapusher+ service is not automatically invoked when tabular resources are uploaded, remove `datapusher` from `ckan.plugins` in your `ckan.ini` file.

Also remove/comment out the following `ckan.datapusher` entries in your `ckan.ini`:

* `ckan.datapusher.formats`
* `ckan.datapusher.url`
* `ckan.datapusher.callback_url_base`
* `ckan.datapusher.assume_task_stale_after`

Note that resources which has been pushed previously will still be available on the CKAN Datastore.
You will have to delete these resources separately using the UI or the CKAN [resource_delete](https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.delete.resource_delete) API.

If you're no longer using the CKAN Datastore:

* Edit your `ckan.ini` and remove/comment `datastore` from `ckan.plugins`.
* Remove/comment out the `ckan.datastore.write_url` and `ckan.datastore.read_url` entries.

To confirm the uninstallation is successful, upload a new tabular resource and check if:

* tabular Resource Views (e.g. datatables_view, recline_view, etc.) are no longer available
* the **Datastore** and **Data Dictionary** tabs are no longer available
* the **Download** button on the resource page will no longer offer alternate download formats (CSV, TSV, JSON, XML)
* the **Datastore API** button will no longer display on tabular resources

## License

This material is copyright (c) 2020 Open Knowledge Foundation and other contributors

It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
whose full text may be found at:

http://www.fsf.org/licensing/licenses/agpl-3.0.html
