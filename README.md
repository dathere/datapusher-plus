[CKAN Service Provider]: https://github.com/ckan/ckan-service-provider
[Messytables]: https://github.com/okfn/messytables
[qsv]: https://github.com/jqnatividad/qsv

# DataPusher+

DataPusher+ is a fork of [Datapusher](https://github.com/ckan/datapusher) that combines the speed and robustness of
[ckanext-xloader](https://github.com/ckan/ckanext-xloader) with the data type guessing of Datapusher.

Datapusher+ is built using [CKAN Service Provider][], with [Messytables] replaced by [qsv].

[TNRIS](https://tnris.org)/[TWDB](https://www.twdb.texas.gov/) provided the use cases that informed and supported the development
of Datapusher+, specifically, to support a [Resource-first upload workflow](#Resource-first-Upload-Workflow).

It features:

* **"Bullet-proof", ultra-fast data type inferencing with qsv**

  Unlike [Messytables][] which scans only the the first few rows to guess the type of
  a column, [qsv][] scans the entire table so its data type inferences are guaranteed[^1].

  Despite this, qsv is still exponentially faster even if it scans the whole file, not
  only inferring data types, it also calculates some descriptive statistics as well. For example,
  [scanning a 2.7 million row, 124MB CSV file for types and stats took 0.16 seconds](https://github.com/jqnatividad/qsv/blob/master/docs/whirlwind_tour.md#a-whirlwind-tour)[^2].

  It is very fast as qsv is written in [Rust](https://www.rust-lang.org/), is multithreaded,
  and uses all kinds of [performance techniques](https://github.com/jqnatividad/qsv#performance-tuning)
  especially designed for data-wrangling.

* **Exponentially faster loading speed**

  Similar to xloader, we use PostgreSQL COPY to directly pipe the data into the datastore,
  short-circuiting the additional processing/transformation/API calls used by Datapusher.

  But unlike xloader, we load everything using the proper data types and not as text, so there's
  no need to reload the data again after adjusting the Data Dictionary, as you would with xloader.

* **Production-ready Robustness**

  In production, the number one source of support issues is Datapusher - primarily, because of
  data quality issues and Datapusher's inability to correctly infer data types, gracefully handle
  errors[^3], and provide the Data Publisher actionable information to correct the data.

  Datapusher+'s design directly addresses all these issues.

* **More informative datastore loading messages**

  Datapusher+ messages are designed to be more verbose and actionable, so the data publisher's
  user experience is far better and makes it possible to have a resource-first upload workflow.

* **Extended data-wrangling with qsv**

  Apart from bullet-proof data type inferences, qsv is leveraged by Datapusher+ to convert XLS/ODS files;
  count the number of rows; transcode to UTF-8 if required; validate if a CSV conforms to the [RFC 4180 standard](https://datatracker.ietf.org/doc/html/rfc4180);
  and optionally create a preview subset in this initial version.

  Future versions of Datapusher+ will further leverage qsv's 70+ commands to do additional
  data wrangling, preprocessing and validation. The Roadmap is available [here](https://github.com/dathere/datapusher-plus/issues/5).
  Ideas, suggestions and your feedback are most welcome!

[^1]: [Why use qsv instead of a "proper" python data analysis library like pandas?](https://github.com/dathere/datapusher-plus/discussions/15)
[^2]: It takes 0.16 seconds with an index to run `qsv stats` against the [qsv whirlwind tour sample file](https://raw.githubusercontent.com/wiki/jqnatividad/qsv/files/wcp.zip) on a Ryzen 4800H (8 physical/16 logical cores) with 32 gb memory and a 1 TB SSD.
Without an index, it takes 1.3 seconds.
[^3]: Imagine you have a 1M row CSV, and the last row has an invalid value for a numeric column (e.g. "N/A" instead of a number). 
      After spending hours pushing the data very slowly, legacy datapusher will abort on the last row and the ENTIRE job is invalid. 
      Ok, that's bad, but what makes it worse is that the old table has been deleted already, and Datapusher doesn't tell you what 
      caused the job to fail! YIKES!!!!

## Resource-first Upload Workflow

In traditional CKAN, the dataset package upload workflow is as follows:

1. Enter package metadata
2. Upload resource/s
3. Check if the datapusher uploaded the dataset correctly.
   - With the Datapusher,this make take a while, and when it fails, it doesn't really give you
     actionable information on why it failed.
   - With xloader, its 10x faster. But then, that speed comes at the cost of all columns defined as text,
     and the Data Publisher will need to manually change the data types in the Data Dictionary and
     reload the data again.

In [TNRIS/TWDB's extensive user research](https://internetofwater.org/blog/building-the-texas-water-data-hub-from-the-ground-up/),
one of the key usability gaps they found with CKAN is this workflow. Why can't the data publisher
upload the primary resource first, before entering the metadata? And more importantly, why can't some of the metadata
be automatically inferred and populated based on the attributes of the dataset?

This is why qsv's speed is critical for a Resource-first upload workflow. By the time the data publisher
uploads the resource and starts populating the rest of the form a few seconds later, a lot of inferred metadata
(Data Dictionary for this initial version) should be available for pre-populating the rest of the form.

See this [discussion](https://github.com/ckan/ckan/discussions/6689) and this [issue](https://github.com/ckan/ideas/issues/150)
about the "Multi-pass Datapusher" from May 2015 for additional context.

## Development installation

Datapusher+ is a drop-in replacement for Datapusher, so it's installed the same way.

Create a virtual environment for Datapusher+ using at least python 3.7:

    python -m venv dpplus_venv
    . dpplus_venv/bin/activate
    cd dpplus_venv

> ℹ️ **NOTE:** Even though DP+ requires python 3.7, it can still work with CKAN<=2.8

Install the required packages::

    sudo apt-get install python-dev python-virtualenv build-essential libxslt1-dev libxml2-dev zlib1g-dev git libffi-dev

Get the code::

    git clone https://github.com/datHere/datapusher-plus
    cd datapusher-plus

Install the dependencies::

    pip install -r requirements-dev.txt
    pip install -e .

Install qsv::
[Download the appropriate precompiled binaries](https://github.com/jqnatividad/qsv/releases/latest) for your platform and copy
it to the appropriate directory, e.g. for Linux:

    wget https://github.com/jqnatividad/qsv/releases/download/0.45.2/qsv-0.45.2-x86_64-unknown-linux-gnu.zip
    unzip qsv-0.45.2-x86_64-unknown-linux-gnu.zip
    sudo mv qsv /usr/local/bin
    sudo mv qsvlite /usr/local/bin
    sudo mv qsvdp /usr/local/bin

Alternatively, if you want to install qsv from source, follow
the instructions [here](https://github.com/jqnatividad/qsv#installation). Note that when compiling from source,
you may want to look into the [Performance Tuning](https://github.com/jqnatividad/qsv#performance-tuning)
section to squeeze even more performance from qsv.


> ℹ️ **NOTE:** qsv is a general purpose CSV data-wrangling toolkit that gets regular updates. To update to the latest version, just run
`sudo qsv`/`sudo qsvlite` and it will check the repo for the latest version and update as required.


Copy `datapusher/settings.py` to a new file like `settings_local.py` and modify your configuration as required.

    cp datapusher/settings.py settings_local.py
    nano settings_local.py

Run the DataPusher::

    python datapusher/main.py settings_local.py

By default DataPusher should be running at the following port:

    http://localhost:8800/

To run the tests:

    pytest

## Production deployment


These instructions assume you already have CKAN installed on this server in the
default location described in the CKAN install documentation
(`/usr/lib/ckan/default`).  If this is correct you should be able to run the
following commands directly, if not you will need to adapt the previous path to
your needs.

These instructions set up the DataPusher web service on
[uWSGI](https://uwsgi-docs.readthedocs.io/en/latest/) running on port 8800, but
can be easily adapted to other WSGI servers like Gunicorn. You'll probably need
to set up Nginx as a reverse proxy in front of it and something like Supervisor
to keep the process up.


    # Install requirements for the DataPusher
    sudo apt install python3-venv python3-dev build-essential libxslt1-dev libxml2-dev libffi-dev

    # Create a virtualenv for datapusher
    sudo python3 -m venv /usr/lib/ckan/datapusher-plus

    # Install DataPusher-plus, uwsgi and psycopg2 for production
    sudo /usr/lib/ckan/datapusher-plus/bin/pip install datapusher-plus uwsgi psycopg2-binary

    # generate a settings file and tune it, as well as a uwsgi ini file
    sudo mkdir -p /etc/ckan/datapusher
    sudo curl https://github.com/dathere/datapusher-plus/blob/master/datapusher/settings.py -o /etc/ckan/datapusher/settings.py
    sudo curl https://github.com/dathere/datapusher-plus/blob/master/deployment/datapusher-uwsgi.ini -o /etc/ckan/datapusher/uwsgi.ini

    # Initialize the database
    /usr/lib/ckan/datapusher-plus/bin/datapusher_initdb /etc/ckan/datapusher/settings.py

    # Create a user to run the web service (if necessary)
    sudo addgroup www-data
    sudo adduser -G www-data www-data

At this point you can run DataPusher-plus with the following command:

    /usr/lib/ckan/datapusher-plus/bin/uwsgi -i /etc/ckan/datapusher/uwsgi.ini

You might need to change the `uid` and `guid` settings when using a different
user.

### High Availability Setup

 [Similar to Datapusher](https://github.com/ckan/datapusher#high-availability-setup).
## Configuring


### CKAN Configuration

Add `datapusher` to the plugins in your CKAN configuration file
(generally located at `/etc/ckan/default/production.ini` or `/etc/ckan/default/ckan.ini`):

    ckan.plugins = <other plugins> datapusher

In order to tell CKAN where this webservice is located, the following must be
added to the `[app:main]` section of your CKAN configuration file :

    ckan.datapusher.url = http://127.0.0.1:8800/

There are other CKAN configuration options that allow to customize the CKAN - DataPusher
integration. Please refer to the [DataPusher Settings](https://docs.ckan.org/en/latest/maintaining/configuration.html#datapusher-settings) section in the CKAN documentation for more details.


### DataPusher+ Configuration

The DataPusher instance is configured in the `deployment/datapusher_settings.py`
file. The location of this file can be adjusted using the `JOB_CONFIG`
environment variable which should provide an absolute path to a python-formatted
config file.

Here's a summary of the options available.

| Name | Default | Description |
| -- | -- | -- |
| HOST | '0.0.0.0' | Web server host |
| PORT | 8800 | Web server port |
| SQLALCHEMY_DATABASE_URI | 'sqlite:////tmp/job_store.db' | SQLAlchemy Database URL. See note about database backend below. |
| MAX_CONTENT_LENGTH | '1024000' | Max size of files to process in bytes |
| CHUNK_SIZE | '16384' | Chunk size when processing the data file |
| DOWNLOAD_TIMEOUT | '30' | Download timeout for requesting the file |
| SSL_VERIFY | False | Do not validate SSL certificates when requesting the data file (*Warning*: Do not use this setting in production) |
| TYPES | 'String', 'Float', 'Integer', 'DateTime', 'Date', 'NULL' | These are the types that qsv can infer. |
| TYPE_MAPPING | {'String': 'text', 'Integer': 'numeric', 'Float': 'numeric', 'DateTime': 'timestamp', 'Date': 'timestamp', 'NULL': 'text'} | Internal qsv type mapping to PostgreSQL types |
| LOG_FILE | `/tmp/ckan_service.log` | Where to write the logs. Use an empty string to disable |
| STDERR | `True` | Log to stderr? |
| QSV_BIN | /usr/local/bin/qsvdp | The location of the qsv binary to use. qsvdp is the DP+ optimized version of qsv. It only has the commands used by DP+, has the self-update engine removed, and is 6x smaller than qsv and 3x smaller than qsvlite. |
| PREVIEW_ROWS | 1000 | The number of rows to insert to the data store. Set to 0 to insert all rows |
| QSV_DEDUP | `True` | Automatically deduplicate rows? |
| DEFAULT_EXCEL_SHEET | 0 | The zero-based index of the Excel sheet to export to CSV and insert into the Datastore. Negative values are accepted, i.e. -1 is the last sheet, -2 is 2nd to the last, etc. |
| AUTO_ALIAS | `True` | Automatically create a resource alias - RESOURCE_NAME-PACKAGE_NAME-OWNER_ORG, that's easier to use in API calls and with scheming datastore_choices helper |
| WRITE_ENGINE_URL | | The Postgres connection string to use to write to the Datastore using Postgres COPY. This should be **similar** to your `ckan.datastore.write_url`, except you'll need to specify a new role with SUPERUSER privileges, |

> NOTE: To do native PostgreSQL operations like TRUNCATE, VACUUM and COPY, a new
> postgres role on the datastore_default database
> needs to be created with SUPERUSER privileges.

```
su - postgres
psql -d datastore_default
CREATE ROLE datapusher LOGIN SUPERUSER PASSWORD 'thepassword';
\q
```

All of the configuration options above can be also provided as environment
variables prepending the name with `DATAPUSHER_`, eg
`DATAPUSHER_SQLALCHEMY_DATABASE_URI`, `DATAPUSHER_PORT`, etc. For variables with
boolean values you must use `1` or `0`.


By default, DataPusher uses SQLite as the database backend for jobs information. This is fine for local development and sites with low activity, but for sites that need more performance, Postgres should be used as the backend for the jobs database (eg `SQLALCHEMY_DATABASE_URI=postgresql://datapusher_jobs:YOURPASSWORD@localhost/datapusher_jobs`. See also [High Availability Setup](#high-availability-setup). If SQLite is used, its probably a good idea to store the database in a location other than `/tmp`. This will prevent the database being dropped, causing out of sync errors in the CKAN side. A good place to store it is the CKAN storage folder (if DataPusher is installed in the same server), generally in `/var/lib/ckan/`.


## Usage

Any file that has one of the supported formats (defined in [`ckan.datapusher.formats`](https://docs.ckan.org/en/latest/maintaining/configuration.html#ckan-datapusher-formats)) will be attempted to be loaded
into the DataStore.

You can also manually trigger resources to be resubmitted. When editing a resource in CKAN (clicking the "Manage" button on a resource page), a new tab named "DataStore" will appear. This will contain a log of the last attempted upload and a button to retry the upload.

![DataPusher UI](images/datapusher-plus-scn1.png)
![DataPusher UI 2](images/datapusher-plus-scn2.png)

### Command line

Run the following command to submit all resources to datapusher, although it will skip files whose hash of the data file has not changed:

    ckan -c /etc/ckan/default/ckan.ini datapusher resubmit

On CKAN<=2.8:

    paster --plugin=ckan datapusher resubmit -c /etc/ckan/default/ckan.ini

To Resubmit a specific resource, whether or not the hash of the data file has changed::

    ckan -c /etc/ckan/default/ckan.ini datapusher submit {dataset_id}

On CKAN<=2.8:

    paster --plugin=ckan datapusher submit <pkgname> -c /etc/ckan/default/ckan.ini


## License

This material is copyright (c) 2020 Open Knowledge Foundation and other contributors

It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
whose full text may be found at:

http://www.fsf.org/licensing/licenses/agpl-3.0.html
