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
from pathlib import Path
from datasize import DataSize

import ckanserviceprovider.job as job
import ckanserviceprovider.util as util
from ckanserviceprovider import web
from datapusher.config import config


if locale.getdefaultlocale()[0]:
    lang, encoding = locale.getdefaultlocale()
    locale.setlocale(locale.LC_ALL, locale=(lang, encoding))
else:
    locale.setlocale(locale.LC_ALL, '')

USE_PROXY = 'DOWNLOAD_PROXY' in config
if USE_PROXY:
    DOWNLOAD_PROXY = config.get('DOWNLOAD_PROXY')

if not config.get('SSL_VERIFY'):
    requests.packages.urllib3.disable_warnings()

POSTGRES_INT_MAX = 2147483647
POSTGRES_INT_MIN = -2147483648
POSTGRES_BIGINT_MAX = 9223372036854775807
POSTGRES_BIGINT_MIN = -9223372036854775808

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
        ckan_url = 'http://' + ckan_url.lstrip('/')  # DevSkim: ignore DS137138
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
                                 verify=config.get('SSL_VERIFY'),
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
                                 verify=config.get('SSL_VERIFY'),
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
                      verify=config.get('SSL_VERIFY'),
                      data=json.dumps(request, cls=DatastoreEncoder),
                      headers={'Content-Type': 'application/json',
                               'Authorization': api_key}
                      )
    check_response(r, url, 'CKAN DataStore')


def update_resource(resource, api_key, ckan_url):
    """
    Update webstore_url and webstore_last_updated in CKAN
    """

    url = get_url('resource_update', ckan_url)
    r = requests.post(
        url,
        verify=config.get('SSL_VERIFY'),
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
                      verify=config.get('SSL_VERIFY'),
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
                      verify=config.get('SSL_VERIFY'),
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

    # check if QSV_BIN exists
    qsv_path = Path(config.get('QSV_BIN'))
    if not qsv_path.is_file():
        raise util.JobError(
            '{} not found.'.format(config.get('QSV_BIN'))
        )

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
        kwargs = {'headers': headers, 'timeout': config.get('DOWNLOAD_TIMEOUT'),
                  'verify': config.get('SSL_VERIFY'), 'stream': True}
        if USE_PROXY:
            kwargs['proxies'] = {
                'http': DOWNLOAD_PROXY, 'https': DOWNLOAD_PROXY}
        response = requests.get(url, **kwargs)
        response.raise_for_status()

        cl = response.headers.get('content-length')
        max_content_length = int(config.get('MAX_CONTENT_LENGTH'))
        preview_rows = config.get('PREVIEW_ROWS')
        try:
            if cl and int(cl) > max_content_length and not preview_rows:
                raise util.JobError(
                    'Resource too large to download: {cl} > max ({max_cl}).'
                    .format(cl=cl, max_cl=max_content_length))
        except ValueError:
            pass

        tmp = tempfile.NamedTemporaryFile(
            suffix='.' + resource.get('format').lower())
        length = 0
        m = hashlib.md5()
        for chunk in response.iter_content(int(config.get('CHUNK_SIZE'))):
            length += len(chunk)
            if length > max_content_length and not preview_rows:
                raise util.JobError(
                    'Resource too large to process: {cl} > max ({max_cl}).'
                    .format(cl=length, max_cl=max_content_length))
            tmp.write(chunk)
            m.update(chunk)

        ct = response.headers.get('content-type', '').split(';', 1)[0]

    except requests.HTTPError as e:
        raise HTTPError(
            "DataPusher+ received a bad HTTP response when trying to download "
            "the data file", status_code=e.response.status_code,
            request_url=url, response=e.response.content)
    except requests.RequestException as e:
        raise HTTPError(
            message=str(e), status_code=None,
            request_url=url, response=None)

    file_hash = m.hexdigest()
    tmp.seek(0)

    if (resource.get('hash') == file_hash and not data.get('ignore_hash')):
        logger.warning("Upload skipped as the file hash hasn't changed: {hash}.".format(
            hash=file_hash))
        return

    resource['hash'] = file_hash

    def cleanup_tempfiles():
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
        if 'qsv_headers' in globals():
            qsv_headers.close()
        if 'qsv_safenames_csv' in globals():
            qsv_safenames_csv.close()
        if 'qsv_applydp_csv' in globals():
            qsv_applydp_csv.close()

    '''
    Start Analysis using qsv instead of messytables, as 1) its type inferences are bullet-proof
    not guesses as it scans the entire file, 2) its super-fast, and 3) it has
    addl data-wrangling capabilities we use in datapusher+ - slice, input, count, headers, etc.
    '''
    fetch_elapsed = time.perf_counter() - timer_start
    logger.info('Fetched {:.2MB} file in {:,.2f} seconds. Analyzing with qsv...'.format(
        DataSize(length), fetch_elapsed))
    analysis_start = time.perf_counter()

    qsv_bin = config.get('QSV_BIN')

    # check content type or file extension if its a spreadsheet
    spreadsheet_extensions = ['XLS', 'XLSX', 'ODS', 'XLSM', 'XLSB']
    format = resource.get('format').upper()
    if format in spreadsheet_extensions:
        # if so, export it as a csv file
        logger.info('Converting {} to CSV...'.format(format))
        '''
        first, we need a temporary spreadsheet filename with the right file extension
        we only need the filename though, that's why we remove it
        and create a hardlink to the file we got from CKAN
        '''
        qsv_spreadsheet = tempfile.NamedTemporaryFile(suffix='.' + format)
        os.remove(qsv_spreadsheet.name)
        os.link(tmp.name, qsv_spreadsheet.name)

        # run `qsv excel` and export it to a CSV
        # use --trim option to trim column names and the data
        qsv_excel_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        default_excel_sheet = config.get('DEFAULT_EXCEL_SHEET')
        try:
            qsv_excel = subprocess.run(
                [qsv_bin, 'excel', qsv_spreadsheet.name, '--sheet', str(default_excel_sheet),
                 '--trim', '--output', qsv_excel_csv.name],
                check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            cleanup_tempfiles()
            logger.error('Upload aborted. Cannot export spreadsheet to CSV: {}'.format(e))
            return
        qsv_spreadsheet.close()
        excel_export_msg = qsv_excel.stderr
        logger.info("{}...".format(excel_export_msg))
        tmp = qsv_excel_csv
    else:
        '''
        its a CSV/TSV/TAB file, not a spreadsheet. Do two-stage validation: 
        
        1) Normalize & transcode to UTF-8 using `qsv input`. We need to normalize as it could be
        a CSV/TSV/TAB dialect with differing delimiters, quoting, etc. 
        Using qsv input's --output option also auto-transcodes to UTF-8.
        
        2) Run an RFC4180 check with `qsv validate` against the normalized, UTF-8 encoded CSV file.
        If it passes validation, we can now handle it with confidence downstream as a "normal" CSV.
        
        Note that we only change the workfile, the resource file itself is unchanged.
        '''
        
        # normalize to CSV, and transcode to UTF-8 if required
        qsv_input_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        logger.info('Normalizing/Transcoding {}...'.format(format))
        try:
            qsv_input = subprocess.run(
                [qsv_bin, 'input', tmp.name, '--output', qsv_input_csv.name], check=True)
        except subprocess.CalledProcessError as e:
            # return as we can't push an invalid CSV file
            cleanup_tempfiles()
            logger.error("Job aborted as the file cannot be normalized/transcoded: {}.".format(e))
            return
        tmp = qsv_input_csv
        logger.info('Normalized & transcoded...')
        
        # validation phase
        logger.info('Validating {}...'.format(format))
        try:
            qsv_validate = subprocess.run(
                [qsv_bin, 'validate', tmp.name], check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            # return as we can't push an invalid CSV file
            cleanup_tempfiles()
            validate_error_msg = qsv_validate.stderr
            logger.error("Invalid file! Job aborted: {}.".format(validate_error_msg))
            return
        logger.info('Valid file...')

    # do we need to dedup?
    # note that deduping also ends up creating a sorted CSV
    if config.get('QSV_DEDUP'):
        qsv_dedup_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        logger.info('Checking for duplicate rows...')
        try:
            qsv_dedup = subprocess.run(
                [qsv_bin, 'dedup', tmp.name, '--output', qsv_dedup_csv.name], 
                capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            cleanup_tempfiles()
            raise util.JobError(
                'Check for duplicates error: {}'.format(e)
            )
        dupe_count = int(str(qsv_dedup.stderr).strip())
        if dupe_count > 0:
            tmp = qsv_dedup_csv
            logger.warning(
                '{:,} duplicates found and removed...'.format(dupe_count))
        else:
            logger.info('No duplicates found...')
            
    # get existing header names, so we can use them for data dictionary labels
    # should we need to change the column name to make it "db-safe"
    try:
        qsv_headers = subprocess.run(
            [qsv_bin, 'headers', '--just-names', tmp.name], 
            capture_output=True, check=True, text=True)
    except subprocess.CalledProcessError as e:
        cleanup_tempfiles()
        raise util.JobError(
            'Cannot scan CSV headers: {}'.format(e)
        )
    original_headers = str(qsv_headers.stdout).strip()
    original_header_dict = {idx: ele for idx, ele in 
                            enumerate(original_headers.splitlines())}

    # now, ensure our column/header names identifiers are "safe names"
    # i.e. valid postgres identifiers
    qsv_safenames_csv = tempfile.NamedTemporaryFile(suffix='.csv')
    logger.info('Checking for safe column names...')
    try:
        qsv_safenames = subprocess.run(
            [qsv_bin, 'safenames', tmp.name, '--mode', 'verify', '--output', 
             qsv_safenames_csv.name], capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        cleanup_tempfiles()
        raise util.JobError(
            'Safenames error: {}'.format(e)
        )
    unsafeheader_count = int(str(qsv_safenames.stderr).strip())
    if unsafeheader_count > 0:
        logger.info('Sanitizing header names...')
        qsv_safenames = subprocess.run(
            [qsv_bin, 'safenames', tmp.name, '--mode', 'conditional', 
             '--output', qsv_safenames_csv.name], 
            capture_output=True, text=True)
        tmp = qsv_safenames_csv
    else:
        logger.info('No unsafe header names found...')

    # at this stage, we have a "clean" CSV ready for type inferencing

    # first, index csv for speed - count, stats and slice
    # are all accelerated/multithreaded when an index is present
    try:
        subprocess.run(
            [qsv_bin, 'index', tmp.name], check=True)
    except subprocess.CalledProcessError as e:
        cleanup_tempfiles
        raise util.JobError(
            'Cannot index CSV: {}'.format(e)
        )

    # get record count, this is instantaneous with an index
    try:
        qsv_count = subprocess.run(
            [qsv_bin, 'count', tmp.name], capture_output=True, check=True, text=True)
    except subprocess.CalledProcessError as e:
        cleanup_tempfiles()
        raise util.JobError(
            'Cannot count records in CSV: {}'.format(e)
        )
    record_count = int(str(qsv_count.stdout).strip())
    
    # its empty, nothing to do
    if record_count == 0:
        cleanup_tempfiles()
        logger.warning('Upload skipped as there are zero records.')
        return
    
    # log how many records we detected
    unique_qualifier = ''
    if config.get('QSV_DEDUP'):
        unique_qualifier = 'unique'
    logger.info('{:,} {} records detected...'.format(
        record_count, unique_qualifier))

    # run qsv stats to get data types and descriptive statistics
    headers = []
    types = []
    headers_min = []
    headers_max = []
    qsv_stats_csv = tempfile.NamedTemporaryFile(suffix='.csv')
    qsv_stats_cmd = [qsv_bin, 'stats', tmp.name, '--infer-dates', '--dates-whitelist', 
                     'all', '--output', qsv_stats_csv.name]
    try:
        qsv_stats = subprocess.run(qsv_stats_cmd, check=True)
    except subprocess.CalledProcessError as e:
        cleanup_tempfiles()
        qsv_stats_csv.close()
        raise util.JobError(
            'Cannot infer data types and compile statistics: {}'.format(e)
        )
    with open(qsv_stats_csv.name, mode='r') as inp:
        reader = csv.DictReader(inp)
        for row in reader:
            headers.append(row['field'])
            types.append(row['type'])
            headers_min.append(row['min'])
            headers_max.append(row['max'])

    existing = datastore_resource_exists(resource_id, api_key, ckan_url)
    existing_info = None
    if existing:
        existing_info = dict((f['id'], f['info'])
                             for f in existing.get('fields', []) if 'info' in f)

    # override with types user requested
    if existing_info:
        types = [{
            'text': 'String',
            'numeric': 'Float',
            'timestamp': 'DateTime',
        }.get(existing_info.get(h, {}).get('type_override'), t)
            for t, h in zip(types, headers)]

    # Delete existing datastore resource before proceeding.
    if existing:
        logger.info('Deleting "{res_id}" from datastore.'.format(
            res_id=resource_id))
        delete_datastore_resource(resource_id, api_key, ckan_url)

    # 1st pass of building headers_dict
    type_mapping = config.get('TYPE_MAPPING')
    temp_headers_dicts = [dict(id=field[0], type=type_mapping[str(field[1])])
                          for field in zip(headers, types)]

    # 2nd pass header_dicts, checking for smartint types
    # and set labels to original column names in case we made the names "db-safe"
    # as the labels are used by DataTables_view to label columns
    # we also take note of datetime/timestamp fields, so we can normalize them
    # to RFC3339 format
    datetimecols_list = []
    headers_dicts = []
    for idx, header in enumerate(temp_headers_dicts):
        if header['type'] == 'smartint':
            if int(headers_max[idx]) <= POSTGRES_INT_MAX and int(headers_min[idx]) >= POSTGRES_INT_MIN:
                header_type = 'integer'
            elif int(headers_max[idx]) <= POSTGRES_BIGINT_MAX and int(headers_min[idx]) >= POSTGRES_BIGINT_MIN:
                header_type = 'bigint'
            else:
                header_type = 'numeric'
        else:
            header_type = header['type']
        if header_type == 'timestamp':
            datetimecols_list.append(header['id'])
        info_dict = dict(label=original_header_dict[idx])
        headers_dicts.append(dict(id=header['id'], type=header_type, info=info_dict))

    # Maintain data dictionaries from matching column names
    if existing_info:
        for h in headers_dicts:
            if h['id'] in existing_info:
                h['info'] = existing_info[h['id']]
                # create columns with types user requested
                type_override = existing_info[h['id']].get('type_override')
                if type_override in list(type_mapping.values()):
                    h['type'] = type_override

    logger.info('Determined headers and types: {headers}...'.format(
        headers=headers_dicts))

    # if rowcount > PREVIEW_ROWS create a preview using qsv slice
    rows_to_copy = record_count
    if preview_rows > 0 and record_count > preview_rows:
        logger.info(
            'Preparing {:,}-row preview...'.format(preview_rows))
        qsv_slice_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        try:
            qsv_slice = subprocess.run(
                [qsv_bin, 'slice', '--len', str(preview_rows), tmp.name, 
                 '--output', qsv_slice_csv.name], check=True)
        except subprocess.CalledProcessError as e:
            cleanup_tempfiles()
            raise util.JobError(
                'Cannot create a preview slice: {}'.format(e)
            )
        rows_to_copy = preview_rows
        tmp = qsv_slice_csv
        
    # if there are any datetime fields, normalize them to RFC3339 format
    # so we can readily insert them as timestamps into postgresql with COPY
    if datetimecols_list:
        qsv_applydp_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        datecols = ','.join(datetimecols_list)
        logger.info('Formatting dates \"{}\" to ISO 8601/RFC 3339 format...'.format(datecols))
        try:
            qsv_applydp = subprocess.run(
                [qsv_bin, 'applydp', 'datefmt', datecols, tmp.name, '--output', 
                qsv_applydp_csv.name], check=True)
        except subprocess.CalledProcessError as e:
            cleanup_tempfiles()
            raise util.JobError(
                'Applydp error: {}'.format(e)
            )
        tmp = qsv_applydp_csv

    analysis_elapsed = time.perf_counter() - analysis_start
    logger.info(
        'Analyzed and prepped in {:,.2f} seconds.'.format(analysis_elapsed))

    # at this stage, the resource is ready for COPYing to the Datastore

    if dry_run:
        return headers_dicts

    logger.info('Copying {:,} rows to database...'.format(rows_to_copy))
    copy_start = time.perf_counter()

    # first, let's create an empty datastore table w/ guessed types
    send_resource_to_datastore(resource, headers_dicts, api_key, ckan_url,
                               records=None, aliases=None, calculate_record_count=False)

    copied_count = 0
    try:
        raw_connection = psycopg2.connect(config.get('WRITE_ENGINE_URL'))
    except psycopg2.Error as e:
        cleanup_tempfiles()
        raise util.JobError(
            'Could not connect to the Datastore: {}'.format(e)
        )
    else:
        cur = raw_connection.cursor()
        '''
        truncate table to use copy freeze option and further increase
        performance as there is no need for WAL logs to be maintained
        https://www.postgresql.org/docs/9.1/populate.html#POPULATE-COPY-FROM
        '''
        try:
            cur.execute('TRUNCATE TABLE \"{resource_id}\";'.format(
                resource_id=resource_id))
        except psycopg2.Error as e:
            logger.warning("Could not TRUNCATE: {}".format(e))

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
                cleanup_tempfiles()
                raise util.JobError(
                    'Postgres COPY failed: {}'.format(e)
                )
            else:
                copied_count = cur.rowcount

        raw_connection.commit()
        # this is needed to issue a VACUUM ANALYZE
        raw_connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = raw_connection.cursor()
        cur.execute('VACUUM ANALYZE \"{resource_id}\";'.format(
            resource_id=resource_id))

    copy_elapsed = time.perf_counter() - copy_start
    logger.info('...copying done. Copied {n} rows to "{res_id}" in {copy_elapsed} seconds.'.format(
        n='{:,}'.format(copied_count), res_id=resource_id, copy_elapsed='{:,.2f}'.format(copy_elapsed)))

    resource['datastore_active'] = True
    resource['total_record_count'] = record_count
    if preview_rows < record_count:
        resource['preview'] = True
        resource['preview_rows'] = copied_count
    update_resource(resource, api_key, ckan_url)

    # aliases are human-readable, and make it easier to use than resource id hash
    # in using the Datastore API and in SQL queries
    alias_unique_flag = config.get('AUTO_ALIAS_UNIQUE')
    if config.get('AUTO_ALIAS'):
        # get package info, so we can construct the alias
        package = get_package(resource['package_id'], ckan_url, api_key)

        resource_name = resource.get('name')
        package_name = package.get('name')
        owner_org = package.get('organization')
        owner_org_name = ''
        if owner_org:
            owner_org_name = owner_org.get('name')
        if resource_name and package_name and owner_org_name:
            # we limit it to 59, so we still have space for sequence suffix
            # postgres max identifier length is 63
            alias = f"{resource_name}-{package_name}-{owner_org_name}"[:59]
            # if AUTO_ALIAS_UNIQUE is true, check if the alias already exist, if it does
            # add a sequence suffix so the new alias can be created
            if alias_unique_flag:
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
            logger.info(
                'Cannot create alias: {}-{}-{}'.format(resource_name, package_name, owner_org))
            alias = None
    raw_connection.close()

    # tell CKAN to calculate_record_count and set alias if set
    send_resource_to_datastore(resource, headers_dicts, api_key, ckan_url,
                               records=None, aliases=alias, calculate_record_count=True)
    
    # TODO: Create indices automatically based on statistics
    # e.g. columns with Cardinality = rowcount has all unique values and a unique index can be created
    # and if CREATE_INDEX setting is set
    
    if alias:
        logger.info('Created alias: {}'.format(alias))

    cleanup_tempfiles()

    total_elapsed = time.perf_counter() - timer_start
    logger.info(
        'Datapusher+ job done. Total elapsed time: {:,.2f} seconds.'.format(total_elapsed))
