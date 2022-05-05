# -*- coding: utf-8 -*-

import json
import requests
try:
    from urllib.parse import urlsplit
except ImportError:
    from urlparse import urlsplit

import itertools
import datetime
import locale
import logging
import decimal
import hashlib
import time
import tempfile
import subprocess
import csv
import os
import psycopg2
import six
from pathlib import Path
from datasize import DataSize

import ckanserviceprovider.job as job
import ckanserviceprovider.util as util
from ckanserviceprovider import web

if locale.getdefaultlocale()[0]:
    lang, encoding = locale.getdefaultlocale()
    locale.setlocale(locale.LC_ALL, locale=(lang, encoding))
else:
    locale.setlocale(locale.LC_ALL, '')

MAX_CONTENT_LENGTH = web.app.config.get('MAX_CONTENT_LENGTH') or 10485760
QSV_BIN = web.app.config.get('QSV_BIN') or '/usr/local/bin/qsvdp'
PREVIEW_ROWS = web.app.config.get('PREVIEW_ROWS') or 10000
DEFAULT_EXCEL_SHEET = web.app.config.get('DEFAULT_EXCEL_SHEET') or 0
CHUNK_SIZE = web.app.config.get('CHUNK_SIZE') or 16384
DOWNLOAD_TIMEOUT = web.app.config.get('DOWNLOAD_TIMEOUT') or 30
WRITE_ENGINE_URL = web.app.config.get(
    'WRITE_ENGINE_URL') or 'postgresql://datapusher:thepassword@localhost/datastore_default'

if not WRITE_ENGINE_URL:
    raise util.JobError('WRITE_ENGINE_URL is required.')

USE_PROXY = 'DOWNLOAD_PROXY' in web.app.config
if USE_PROXY:
    DOWNLOAD_PROXY = web.app.config.get('DOWNLOAD_PROXY')

if web.app.config.get('QSV_DEDUP') in ['False', 'FALSE', '0', False, 0]:
    QSV_DEDUP = False
else:
    QSV_DEDUP = True

if web.app.config.get('AUTO_ALIAS') in ['False', 'FALSE', '0', False, 0]:
    AUTO_ALIAS = False
else:
    AUTO_ALIAS = True

if web.app.config.get('SSL_VERIFY') in ['False', 'FALSE', '0', False, 0]:
    SSL_VERIFY = False
else:
    SSL_VERIFY = True

if not SSL_VERIFY:
    requests.packages.urllib3.disable_warnings()

_TYPE_MAPPING = {
    'String': 'text',
    # 'int' may not be big enough,
    # and type detection may not realize it needs to be big
    'Integer': 'numeric',
    'Float': 'numeric',
    'DateTime': 'timestamp',
    'Date': 'timestamp',
    'NULL': 'text',
}

_TYPES = ['String', 'Float', 'Integer', 'DateTime', 'Date', 'NULL']

TYPE_MAPPING = web.app.config.get('TYPE_MAPPING', _TYPE_MAPPING)
TYPES = web.app.config.get('TYPES', _TYPES)

# if a field has any of these anywhere in their name, date inferencing
# is turned on when scanning for data types, which is a relatively expensive op
# if DATELIKE_FIELDNAMES is empty, date inferencing will always be on
_DATELIKE_FIELDNAMES = ['date', 'time', 'open', 'close', 'due']

DATELIKE_FIELDNAMES = web.app.config.get(
    'DATELIKE_FIELDNAMES', _DATELIKE_FIELDNAMES)

DATELIKE_FIELDNAMES = [field.lower() for field in DATELIKE_FIELDNAMES]

DATASTORE_URLS = {
    'datastore_delete': '{ckan_url}/api/action/datastore_delete',
    'resource_update': '{ckan_url}/api/action/resource_update'
}


class HTTPError(util.JobError):
    """Exception that's raised if a job fails due to an HTTP problem."""

    def __init__(self, message, status_code, request_url, response):
        """Initialise a new HTTPError.

        :param message: A human-readable error message
        :type message: string

        :param status_code: The status code of the errored HTTP response,
            e.g. 500
        :type status_code: int

        :param request_url: The URL that was requested
        :type request_url: string

        :param response: The body of the errored HTTP response as unicode
            (if you have a requests.Response object then response.text will
            give you this)
        :type response: unicode

        """
        super(HTTPError, self).__init__(message)
        self.status_code = status_code
        self.request_url = request_url
        self.response = response

    def as_dict(self):
        """Return a JSON-serializable dictionary representation of this error.

        Suitable for ckanserviceprovider to return to the client site as the
        value for the "error" key in the job dict.

        """
        if self.response and len(self.response) > 200:
            response = self.response[:200]
        else:
            response = self.response
        return {
            "message": self.message,
            "HTTP status code": self.status_code,
            "Requested URL": self.request_url,
            "Response": response,
        }

    def __str__(self):
        return '{} status={} url={} response={}'.format(
            self.message, self.status_code, self.request_url, self.response) \
            .encode('ascii', 'replace')


def get_url(action, ckan_url):
    """
    Get url for ckan action
    """
    if not urlsplit(ckan_url).scheme:
        ckan_url = 'http://' + ckan_url.lstrip('/')
    ckan_url = ckan_url.rstrip('/')
    return '{ckan_url}/api/3/action/{action}'.format(
        ckan_url=ckan_url, action=action)


def check_response(response, request_url, who, good_status=(201, 200),
                   ignore_no_success=False):
    """
    Checks the response and raises exceptions if something went terribly wrong

    :param who: A short name that indicated where the error occurred
                (for example "CKAN")
    :param good_status: Status codes that should not raise an exception

    """
    if not response.status_code:
        raise HTTPError(
            'DataPusher received an HTTP response with no status code',
            status_code=None, request_url=request_url, response=response.text)

    message = '{who} bad response. Status code: {code} {reason}. At: {url}.'
    try:
        if response.status_code not in good_status:
            json_response = response.json()
            if not ignore_no_success or json_response.get('success'):
                try:
                    message = json_response["error"]["message"]
                except Exception:
                    message = message.format(
                        who=who, code=response.status_code,
                        reason=response.reason, url=request_url)
                raise HTTPError(
                    message, status_code=response.status_code,
                    request_url=request_url, response=response.text)
    except ValueError:
        message = message.format(
            who=who, code=response.status_code, reason=response.reason,
            url=request_url, resp=response.text[:200])
        raise HTTPError(
            message, status_code=response.status_code, request_url=request_url,
            response=response.text)


class DatastoreEncoder(json.JSONEncoder):
    # Custom JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def delete_datastore_resource(resource_id, api_key, ckan_url):
    try:
        delete_url = get_url('datastore_delete', ckan_url)
        response = requests.post(delete_url,
                                 verify=SSL_VERIFY,
                                 data=json.dumps({'id': resource_id,
                                                  'force': True}),
                                 headers={'Content-Type': 'application/json',
                                          'Authorization': api_key}
                                 )
        check_response(response, delete_url, 'CKAN',
                       good_status=(201, 200, 404), ignore_no_success=True)
    except requests.exceptions.RequestException:
        raise util.JobError('Deleting existing datastore failed.')


def datastore_resource_exists(resource_id, api_key, ckan_url):
    try:
        search_url = get_url('datastore_search', ckan_url)
        response = requests.post(search_url,
                                 verify=SSL_VERIFY,
                                 data=json.dumps({'id': resource_id,
                                                  'limit': 0}),
                                 headers={'Content-Type': 'application/json',
                                          'Authorization': api_key}
                                 )
        if response.status_code == 404:
            return False
        elif response.status_code == 200:
            return response.json().get('result', {'fields': []})
        else:
            raise HTTPError(
                'Error getting datastore resource.',
                response.status_code, search_url, response,
            )
    except requests.exceptions.RequestException as e:
        raise util.JobError(
            'Error getting datastore resource ({!s}).'.format(e))


def send_resource_to_datastore(resource, headers, api_key, ckan_url,
                               records, aliases, calculate_record_count, ):
    """
    Stores records in CKAN datastore
    """
    request = {'resource_id': resource['id'],
               'fields': headers,
               'force': True,
               'records': records,
               'aliases': aliases,
               'calculate_record_count': calculate_record_count}

    url = get_url('datastore_create', ckan_url)
    r = requests.post(url,
                      verify=SSL_VERIFY,
                      data=json.dumps(request, cls=DatastoreEncoder),
                      headers={'Content-Type': 'application/json',
                               'Authorization': api_key}
                      )
    check_response(r, url, 'CKAN DataStore')


def update_resource(resource, api_key, ckan_url):
    """
    Update webstore_url and webstore_last_updated in CKAN
    """

    resource['url_type'] = 'datapusher'

    url = get_url('resource_update', ckan_url)
    r = requests.post(
        url,
        verify=SSL_VERIFY,
        data=json.dumps(resource),
        headers={'Content-Type': 'application/json',
                 'Authorization': api_key}
    )

    check_response(r, url, 'CKAN')


def get_resource(resource_id, ckan_url, api_key):
    """
    Gets available information about the resource from CKAN
    """
    url = get_url('resource_show', ckan_url)
    r = requests.post(url,
                      verify=SSL_VERIFY,
                      data=json.dumps({'id': resource_id}),
                      headers={'Content-Type': 'application/json',
                               'Authorization': api_key}
                      )
    check_response(r, url, 'CKAN')

    return r.json()['result']


def get_package(package_id, ckan_url, api_key):
    """
    Gets available information about a package from CKAN
    """
    url = get_url('package_show', ckan_url)
    r = requests.post(url,
                      verify=SSL_VERIFY,
                      data=json.dumps({'id': package_id}),
                      headers={'Content-Type': 'application/json',
                               'Authorization': api_key}
                      )
    check_response(r, url, 'CKAN')

    return r.json()['result']


def validate_input(input):
    # Especially validate metadata which is provided by the user
    if 'metadata' not in input:
        raise util.JobError('Metadata missing')

    data = input['metadata']

    if 'resource_id' not in data:
        raise util.JobError('No id provided.')
    if 'ckan_url' not in data:
        raise util.JobError('No ckan_url provided.')
    if not input.get('api_key'):
        raise util.JobError('No CKAN API key provided')


@job.asynchronous
def push_to_datastore(task_id, input, dry_run=False):
    '''Download and parse a resource push its data into CKAN's DataStore.

    An asynchronous job that gets a resource from CKAN, downloads the
    resource's data file and, if the data file has changed since last time,
    parses the data and posts it into CKAN's DataStore.

    :param dry_run: Fetch and parse the data file but don't actually post the
        data to the DataStore, instead return the data headers and rows that
        would have been posted.
    :type dry_run: boolean

    '''
    handler = util.StoringHandler(task_id, input)
    logger = logging.getLogger(task_id)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    validate_input(input)

    data = input['metadata']

    ckan_url = data['ckan_url']
    resource_id = data['resource_id']
    api_key = input.get('api_key')

    try:
        resource = get_resource(resource_id, ckan_url, api_key)
    except util.JobError:
        # try again in 5 seconds just incase CKAN is slow at adding resource
        time.sleep(5)
        resource = get_resource(resource_id, ckan_url, api_key)

    # check if the resource url_type is a datastore
    if resource.get('url_type') == 'datastore':
        logger.info('Dump files are managed with the Datastore API')
        return

    # check scheme
    url = resource.get('url')
    scheme = urlsplit(url).scheme
    if scheme not in ('http', 'https', 'ftp'):
        raise util.JobError(
            'Only http, https, and ftp resources may be fetched.'
        )

    timer_start = time.perf_counter()

    # fetch the resource data
    logger.info('Fetching from: {0}...'.format(url))
    headers = {}
    if resource.get('url_type') == 'upload':
        # If this is an uploaded file to CKAN, authenticate the request,
        # otherwise we won't get file from private resources
        headers['Authorization'] = api_key
    try:
        kwargs = {'headers': headers, 'timeout': DOWNLOAD_TIMEOUT,
                  'verify': SSL_VERIFY, 'stream': True}
        if USE_PROXY:
            kwargs['proxies'] = {
                'http': DOWNLOAD_PROXY, 'https': DOWNLOAD_PROXY}
        response = requests.get(url, **kwargs)
        response.raise_for_status()

        cl = response.headers.get('content-length')
        try:
            if cl and int(cl) > MAX_CONTENT_LENGTH and not PREVIEW_ROWS:
                raise util.JobError(
                    'Resource too large to download: {cl} > max ({max_cl}).'
                    .format(cl=cl, max_cl=MAX_CONTENT_LENGTH))
        except ValueError:
            pass

        tmp = tempfile.NamedTemporaryFile(
            suffix='.' + resource.get('format').lower())
        length = 0
        m = hashlib.md5()
        for chunk in response.iter_content(CHUNK_SIZE):
            length += len(chunk)
            if length > MAX_CONTENT_LENGTH and not PREVIEW_ROWS:
                raise util.JobError(
                    'Resource too large to process: {cl} > max ({max_cl}).'
                    .format(cl=length, max_cl=MAX_CONTENT_LENGTH))
            tmp.write(chunk)
            m.update(chunk)

        ct = response.headers.get('content-type', '').split(';', 1)[0]

    except requests.HTTPError as e:
        raise HTTPError(
            "DataPusher received a bad HTTP response when trying to download "
            "the data file", status_code=e.response.status_code,
            request_url=url, response=e.response.content)
    except requests.RequestException as e:
        raise HTTPError(
            message=str(e), status_code=None,
            request_url=url, response=None)

    file_hash = m.hexdigest()
    tmp.seek(0)

    if (resource.get('hash') == file_hash and not data.get('ignore_hash')):
        logger.info("The file hash hasn't changed: {hash}.".format(
            hash=file_hash))
        return

    resource['hash'] = file_hash

    # Start Analysis using qsv instead of messytables, as 1) its type inferences are bullet-proof
    # not guesses as it scans the entire file, 2) its super-fast, and 3) it has
    # addl data-wrangling capabilities we use in datapusher+ - slice, input, count, headers, etc.
    fetch_elapsed = time.perf_counter() - timer_start
    logger.info('Fetched {:.2MB} file in {:,.2f} seconds. Analyzing with qsv...'.format(
        DataSize(cl), fetch_elapsed))
    analysis_start = time.perf_counter()

    # check content type or file extension if its a spreadsheet
    spreadsheet_extensions = ['XLS', 'XLSX', 'ODS', 'XLSM', 'XLSB']
    format = resource.get('format').upper()
    if format in spreadsheet_extensions:
        # if so, export it as a csv file
        logger.info('Converting {} sheet {} to CSV...'.format(
            format, DEFAULT_EXCEL_SHEET))
        # first, we need a temporary spreadsheet filename with the right file extension
        # we only need the filename though, that's why we remove/delete it
        # and create a hardlink to the file we got from CKAN
        qsv_spreadsheet = tempfile.NamedTemporaryFile(suffix='.' + format)
        os.remove(qsv_spreadsheet.name)
        os.link(tmp.name, qsv_spreadsheet.name)

        # run `qsv excel` and export it to a CSV
        qsv_excel_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        try:
            qsv_excel = subprocess.run(
                [QSV_BIN, 'excel', qsv_spreadsheet.name, '--sheet', str(DEFAULT_EXCEL_SHEET), '--output', qsv_excel_csv.name], check=True)
        except subprocess.CalledProcessError as e:
            tmp.close()
            qsv_excel_csv.close()
            raise util.JobError(
                'Cannot export spreadsheet to CSV: {}'.format(e)
            )
        qsv_spreadsheet.close()
        tmp = qsv_excel_csv
    else:
        # its a CSV/TSV? Check if its valid and
        # transcode it to UTF-8 at the same time
        qsv_input_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        logger.info('Validating/Transcoding {}...'.format(format))
        try:
            qsv_input = subprocess.run(
                [QSV_BIN, 'input', tmp.name, '--output', qsv_input_csv.name], check=True)
        except subprocess.CalledProcessError as e:
            tmp.close()
            qsv_input_csv.close()
            raise util.JobError(
                'Invalid CSV file: {}'.format(e)
            )
        tmp = qsv_input_csv

    # do we need to dedup?
    if QSV_DEDUP:
        qsv_dedup_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        logger.info('Checking for duplicate rows...')
        try:
            qsv_dedup = subprocess.run(
                [QSV_BIN, 'dedup', tmp.name, '--output', qsv_dedup_csv.name], capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            tmp.close()
            qsv_dedup_csv.close()
            raise util.JobError(
                'Check for duplicates error: {}'.format(e)
            )
        dupe_count = int(str(qsv_dedup.stderr).strip())
        if dupe_count > 0:
            tmp = qsv_dedup_csv
            logger.info(
                '{:,} duplicates found and removed...'.format(dupe_count))
        else:
            logger.info('No duplicates found...')

    # index csv for speed - count, stats and slice
    # are all accelerated/multithreaded when an index is present
    try:
        subprocess.run(
            [QSV_BIN, 'index', tmp.name], capture_output=True)
    except subprocess.CalledProcessError as e:
        tmp.close()
        raise util.JobError(
            'Cannot index CSV: {}'.format(e)
        )

    # get record count
    try:
        qsv_count = subprocess.run(
            [QSV_BIN, 'count', tmp.name], capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        tmp.close()
        raise util.JobError(
            'Cannot count records in CSV: {}'.format(e)
        )
    record_count = int(str(qsv_count.stdout).strip())
    logger.info('{:,} records detected...'.format(record_count))

    # if DATELIKE_FIELDNAMES is not empty, scan CSV headers for date-like field,
    # otherwise, always --infer-dates when scanning for types
    inferdates_flag = True
    if DATELIKE_FIELDNAMES:
        try:
            qsv_headers = subprocess.run(
                [QSV_BIN, 'headers', tmp.name], capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            tmp.close()
            raise util.JobError(
                'Cannot scan CSV headers: {}'.format(e)
            )
        header_fields = str(qsv_headers.stdout).strip().lower()
        if not any(datelike_fieldname in header_fields for datelike_fieldname in DATELIKE_FIELDNAMES):
            inferdates_flag = False

    # run qsv stats to get data types and descriptive statistics
    headers = []
    types = []
    qsv_stats_csv = tempfile.NamedTemporaryFile(suffix='.csv')
    qsv_stats_cmd = [QSV_BIN, 'stats', tmp.name,
                     '--output', qsv_stats_csv.name]
    if inferdates_flag:
        qsv_stats_cmd.append('--infer-dates')
        logger.info('Date-like fields detected. Date inferencing enabled...')
    try:
        qsv_stats = subprocess.run(qsv_stats_cmd, check=True)
    except subprocess.CalledProcessError as e:
        tmp.close()
        qsv_stats_csv.close()
        raise util.JobError(
            'Cannot infer data types and compile statistics: {}'.format(e)
        )
    with open(qsv_stats_csv.name, mode='r') as inp:
        reader = csv.reader(inp)
        next(reader)  # skip first element, which is a header
        for rows in reader:
            headers.append(rows[0])
            types.append(rows[1])

    existing = datastore_resource_exists(resource_id, api_key, ckan_url)
    existing_info = None
    if existing:
        existing_info = dict((f['id'], f['info'])
                             for f in existing.get('fields', []) if 'info' in f)

    # override with types user requested
    if existing_info:
        types = [{
            'text': 'String',
            'numeric': 'Decimal',
            'timestamp': 'DateTime',
        }.get(existing_info.get(h, {}).get('type_override'), t)
            for t, h in zip(types, headers)]

    '''
    Delete existing datastore resource before proceeding. Otherwise
    'datastore_create' will append to the existing datastore. And if
    the fields have significantly changed, it may also fail.
    '''
    if existing:
        logger.info('Deleting "{res_id}" from datastore.'.format(
            res_id=resource_id))
        delete_datastore_resource(resource_id, api_key, ckan_url)

    headers_dicts = [dict(id=field[0], type=TYPE_MAPPING[str(field[1])])
                     for field in zip(headers, types)]

    # Maintain data dictionaries from matching column names
    if existing_info:
        for h in headers_dicts:
            if h['id'] in existing_info:
                h['info'] = existing_info[h['id']]
                # create columns with types user requested
                type_override = existing_info[h['id']].get('type_override')
                if type_override in list(_TYPE_MAPPING.values()):
                    h['type'] = type_override

    logger.info('Determined headers and types: {headers}...'.format(
        headers=headers_dicts))

    # if rowcount > PREVIEW_ROWS create a preview using qsv slice
    rows_to_copy = record_count
    if PREVIEW_ROWS > 0 and record_count > PREVIEW_ROWS:
        logger.info(
            'Preparing {:,}-row preview...'.format(PREVIEW_ROWS))
        qsv_slice_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        try:
            qsv_slice = subprocess.run(
                [QSV_BIN, 'slice', '--len', str(PREVIEW_ROWS), tmp.name, '--output', qsv_slice_csv.name], check=True)
        except subprocess.CalledProcessError as e:
            tmp.close()
            qsv_slice_csv.close()
            raise util.JobError(
                'Cannot create a preview slice: {}'.format(e)
            )
        rows_to_copy = PREVIEW_ROWS
        tmp = qsv_slice_csv

    analysis_elapsed = time.perf_counter() - analysis_start
    logger.info(
        'Analyzed and prepped in {:,.2f} seconds.'.format(analysis_elapsed))

    if dry_run:
        return headers_dicts

    logger.info('Copying {:,} rows to database...'.format(rows_to_copy))
    copy_start = time.perf_counter()

    # first, let's create an empty datastore table w/ guessed types
    send_resource_to_datastore(resource, headers_dicts, api_key, ckan_url,
                               records=None, aliases=None, calculate_record_count=False)

    record_count = 0
    try:
        raw_connection = psycopg2.connect(WRITE_ENGINE_URL)
    except psycopg2.Error as e:
        tmp.close()
        raise util.JobError(
            'Could not connect to the Datastore: {}'.format(e)
        )
    else:
        cur = raw_connection.cursor()
        # truncate table to use copy freeze option and further increase
        # performance as there is no need for WAL logs to be maintained
        # https://www.postgresql.org/docs/9.1/populate.html#POPULATE-COPY-FROM
        cur.execute('TRUNCATE TABLE \"{resource_id}\";'.format(
            resource_id=resource_id))

        copy_sql = ("COPY \"{resource_id}\" ({column_names}) FROM STDIN "
                    "WITH (FORMAT CSV, FREEZE 1, "
                    "HEADER 1, ENCODING 'UTF8');").format(
                        resource_id=resource_id,
                        column_names=', '.join(['"{}"'.format(h['id'])
                                                for h in headers_dicts]))
        logger.info(copy_sql)
        with open(tmp.name, 'rb') as f:
            try:
                cur.copy_expert(copy_sql, f)
            except psycopg2.Error as e:
                logger.warning("Postgres COPY failed: {}".format(e))
            else:
                record_count = cur.rowcount

        raw_connection.commit()
        # this is needed to issue a VACUUM ANALYZE
        raw_connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = raw_connection.cursor()
        logger.info('Vacuum Analyzing table...')
        cur.execute('VACUUM ANALYZE \"{resource_id}\";'.format(
            resource_id=resource_id))

    copy_elapsed = time.perf_counter() - copy_start
    logger.info('...copying done. Copied {n} rows to "{res_id}" in {copy_elapsed} seconds.'.format(
        n='{:,}'.format(record_count), res_id=resource_id, copy_elapsed='{:,.2f}'.format(copy_elapsed)))

    resource['datastore_active'] = True
    update_resource(resource, api_key, ckan_url)

    if AUTO_ALIAS:
        # get package info, so we can construct the alias
        package = get_package(resource['package_id'], ckan_url, api_key)

        resource_name = resource.get('name')
        package_name = package.get('name')
        owner_org = package.get('organization')
        if owner_org:
            owner_org_name = owner_org.get('name')
        if resource_name and package_name and owner_org_name:
            alias = f"{resource_name}-{package_name}-{owner_org_name}"[:59]
            # check if the alias exist, if it does
            # add a sequence suffix so the new alias can be created
            cur.execute('SELECT COUNT(*) FROM _table_metadata where name like \'{}%\';'.format(
                alias))
            alias_count = cur.fetchone()[0]
            if alias_count:
                alias_sequence = alias_count + 1
                while True:
                    # we do this, so we're certain the new alias does not exist
                    # just in case they deleted an older alias with a lower sequence #
                    alias = f'{alias}-{alias_sequence:03}'
                    cur.execute('SELECT COUNT(*) FROM _table_metadata where name like \'{}%\';'.format(
                        alias))
                    alias_exists = cur.fetchone()[0]
                    if not alias_exists:
                        break
                    alias_sequence += 1
        else:
            alias = None
    raw_connection.close()

    # tell CKAN to calculate_record_count and set alias if set
    send_resource_to_datastore(resource, headers_dicts, api_key, ckan_url,
                               records=None, aliases=alias, calculate_record_count=True)
    if alias:
        logger.info('Created alias: {}'.format(alias))

    # cleanup temporary files
    if os.path.exists(tmp.name + ".idx"):
        os.remove(tmp.name + ".idx")
    tmp.close()
    if 'qsv_slice_csv' in globals():
        qsv_slice_csv.close()
    if 'qsv_excel_csv' in globals():
        qsv_excel_csv.close()
    if 'qsv_input_csv' in globals():
        qsv_input_csv.close()
    if 'qsv_dedup_csv' in globals():
        qsv_dedup_csv.close()

    total_elapsed = time.perf_counter() - timer_start
    logger.info(
        'Datapusher+ job done. Total elapsed time: {:,.2f} seconds.'.format(total_elapsed))
