# NOTE: This module name and function name are defined by the rq package and our own door43-enqueue-job package
# This code adapted by RJH June 2018 from tx-manager/client_webhook/ClientWebhook/process_webhook

# TODO: Check to see which files brought in from tx-manager aren't actually needed (won't have .pyc files if not invoked)


# Python imports
import os
import shutil
import tempfile
import logging
import ssl
import urllib.request as urllib2
import json
import hashlib
from datetime import datetime, timedelta
from time import time

# Library (PyPi) imports
from rq import get_current_job
from statsd import StatsClient # Graphite front-end

# Local imports
from rq_settings import prefix
from general_tools.file_utils import unzip, add_contents_to_zip, write_file, remove_tree
from general_tools.url_utils import download_file
from resource_container.ResourceContainer import RC
from preprocessors.preprocessors import do_preprocess
from models.manifest import TxManifest
from models.job import TxJob
from models.module import TxModule
from global_settings.global_settings import GlobalSettings



OUR_NAME = 'DCS_job_handler'
GlobalSettings(prefix=prefix)
converter_callback = f'{GlobalSettings.api_url}/client/callback/converter'
linter_callback = f'{GlobalSettings.api_url}/client/callback/linter'


# Get the Graphite URL from the environment, otherwise use a local test instance
graphite_url = os.getenv('GRAPHITE_URL','localhost')
stats_client = StatsClient(host=graphite_url, port=8125, prefix=OUR_NAME)


def send_request_to_converter(job, converter):
    """
    :param TxJob job:
    :param TxModule converter:
    :return bool:
    """
    payload = {
        'identifier': job.identifier,
        'source_url': job.source,
        'resource_id': job.resource_type,
        'cdn_bucket': job.cdn_bucket,
        'cdn_file': job.cdn_file,
        'options': job.options,
        'convert_callback': converter_callback
    }
    return send_payload_to_converter(payload, converter)
# end of send_request_to_converter function


def send_payload_to_converter(payload, converter):
    """
    :param dict payload:
    :param TxModule converter:
    :return bool:
    """
    # TODO: Make this use urllib2 to make a async POST to the API. Currently invokes Lambda directly XXXXXXXXXXXXXXXXX
    payload = {
        'data': payload,
        'vars': {
            'prefix': GlobalSettings.prefix
        }
    }
    converter_name = converter.name
    if not isinstance(converter_name,str): # bytes in Python3 -- not sure where it gets set
        converter_name = converter_name.decode()
    print("converter_name", repr(converter_name))
    GlobalSettings.logger.debug(f'Sending Payload to converter {converter_name}:')
    GlobalSettings.logger.debug(payload)
    converter_function = f'{GlobalSettings.prefix}tx_convert_{converter_name}'
    print("send_payload_to_converter: converter_function is {!r} payload={}".format(converter_function,payload))
    stats_client.incr('ConvertersInvoked')
    # TODO: Put an alternative function call in here RJH
    response = GlobalSettings.lambda_handler().invoke(function_name=converter_function, payload=payload, asyncFlag=True)
    GlobalSettings.logger.debug('finished.')
    return response
# end of send_payload_to_converter function


def send_request_to_linter(job, linter, commit_url, commit_data, extra_payload=None):
    """
    :param TxJob job:
    :param TxModule linter:
    :param string commit_url:
    :param dict extra_payload:
    :return bool:
    """
    payload = {
        'identifier': job.identifier,
        'resource_id': job.resource_type,
        'cdn_bucket': job.cdn_bucket,
        'cdn_file': job.cdn_file,
        'options': job.options,
        'lint_callback': linter_callback,
        'commit_data': commit_data
    }
    if extra_payload:
        payload.update(extra_payload)
    if job.input_format == 'usfm' or job.resource_type == 'obs':
        # Need to give the massaged source since it maybe was in chunks originally
        payload['source_url'] = job.source
    else:
        payload['source_url'] = commit_url.replace('commit', 'archive') + '.zip'
    return send_payload_to_linter(payload, linter)
# end of send_request_to_linter function


def send_payload_to_linter(payload, linter):
    """
    :param dict payload:
    :param TxModule linter:
    :return bool:
    """
    # TODO: Make this use urllib2 to make a async POST to the API. Currently invokes Lambda directly
    payload = {
        'data': payload,
        'vars': {
            'prefix': GlobalSettings.prefix
        }
    }
    linter_name = linter.name
    if not isinstance(linter_name,str): # bytes in Python3 -- not sure where it gets set
        linter_name = linter_name.decode()
    print("linter_name",repr(linter_name))
    GlobalSettings.logger.debug(f'Sending payload to linter {linter_name}:')
    GlobalSettings.logger.debug(payload)
    linter_function = f'{GlobalSettings.prefix}tx_lint_{linter_name}'
    print("send_payload_to_linter: linter_function is {!r}, payload={}".format(linter_function,payload))
    stats_client.incr('LintersInvoked')
    # TODO: Put an alternative function call in here RJH
    response = GlobalSettings.lambda_handler().invoke(function_name=linter_function, payload=payload, asyncFlag=True)
    GlobalSettings.logger.debug('finished.')
    return response
# end of send_payload_to_linter function


def update_project_json(base_temp_dir_name, commit_id, job, repo_name, repo_owner):
    """
    :param string commit_id:
    :param TxJob job:
    :param string repo_name:
    :param string repo_owner:
    :return:
    """
    project_json_key = f'u/{repo_owner}/{repo_name}/project.json'
    project_json = GlobalSettings.cdn_s3_handler().get_json(project_json_key)
    project_json['user'] = repo_owner
    project_json['repo'] = repo_name
    project_json['repo_url'] = 'https://git.door43.org/{0}/{1}'.format(repo_owner, repo_name)
    commit = {
        'id': commit_id,
        'created_at': job.created_at,
        'status': job.status,
        'success': job.success,
        'started_at': None,
        'ended_at': None
    }
    if 'commits' not in project_json:
        project_json['commits'] = []
    commits = []
    for c in project_json['commits']:
        if c['id'] != commit_id:
            commits.append(c)
    commits.append(commit)
    project_json['commits'] = commits
    project_file = os.path.join(base_temp_dir_name, 'project.json')
    write_file(project_file, project_json)
    GlobalSettings.cdn_s3_handler().upload_file(project_file, project_json_key)
# end of update_project_json function


def upload_build_log_to_s3(base_temp_dir_name, build_log, s3_commit_key, part=''):
    """
    :param dict build_log:
    :param string s3_commit_key:
    :param string part:
    :return:
    """
    build_log_file = os.path.join(base_temp_dir_name, 'build_log.json')
    write_file(build_log_file, build_log)
    upload_key = f'{s3_commit_key}/{part}build_log.json'
    GlobalSettings.logger.debug('Saving build log to {}/{}'.format(GlobalSettings.cdn_bucket_name,upload_key))
    GlobalSettings.cdn_s3_handler().upload_file(build_log_file, upload_key, cache_time=0)
    # GlobalSettings.logger.debug('build log contains: ' + json.dumps(build_log_json))
#end of upload_build_log_to_s3


def create_build_log(commit_id, commit_message, commit_url, compare_url, job, pusher_username, repo_name, repo_owner):
    """
    :param string commit_id:
    :param string commit_message:
    :param string commit_url:
    :param string compare_url:
    :param TxJob job:
    :param string pusher_username:
    :param string repo_name:
    :param string repo_owner:
    :return dict:
    """
    build_log_json = dict(job)
    build_log_json['repo_name'] = repo_name
    build_log_json['repo_owner'] = repo_owner
    build_log_json['commit_id'] = commit_id
    build_log_json['committed_by'] = pusher_username
    build_log_json['commit_url'] = commit_url
    build_log_json['compare_url'] = compare_url
    build_log_json['commit_message'] = commit_message

    return build_log_json
# end of create_build_log function


def clear_commit_directory_in_cdn(s3_commit_key):
    """
    Clear out the commit directory in the cdn bucket for this project revision.
    """
    for obj in GlobalSettings.cdn_s3_handler().get_objects(prefix=s3_commit_key):
        GlobalSettings.logger.debug('Removing file: ' + obj.key)
        GlobalSettings.cdn_s3_handler().delete_file(obj.key)
# end of clear_commit_directory_in_cdn function


def get_converter_module(job):
    """
    :param TxJob job:
    :return TxModule:
    """
    converters = TxModule.query().filter(TxModule.type=='converter') \
        .filter(TxModule.input_format.contains(job.input_format)) \
        .filter(TxModule.output_format.contains(job.output_format))
    converter = converters.filter(TxModule.resource_types.contains(job.resource_type)).first()
    if not converter:
        converter = converters.filter(TxModule.resource_types.contains('other')).first()
    return converter
# end if get_converter_module function


def get_linter_module(job):
    """
    :param TxJob job:
    :return TxModule:
    """
    linters = TxModule.query().filter(TxModule.type=='linter') \
        .filter(TxModule.input_format.contains(job.input_format))
    linter = linters.filter(TxModule.resource_types.contains(job.resource_type)).first()
    if not linter:
        linter = linters.filter(TxModule.resource_types.contains('other')).first()
    return linter
# end of get_linter_module function


def get_unique_job_id():
    """
    :return string:
    """
    job_id = hashlib.sha256(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f").encode('utf-8')).hexdigest()
    while TxJob.get(job_id):
        job_id = hashlib.sha256(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f").encode('utf-8')).hexdigest()
    return job_id
# end of get_unique_job_id()


def upload_zip_file(commit_id, zip_filepath):
    file_key = f'preconvert/{commit_id}.zip'
    GlobalSettings.logger.debug('Uploading {0} to {1}/{2}...'.format(zip_filepath, GlobalSettings.pre_convert_bucket_name, file_key))
    try:
        GlobalSettings.pre_convert_s3_handler().upload_file(zip_filepath, file_key, cache_time=0)
    except Exception as e:
        GlobalSettings.logger.error('Failed to upload zipped repo up to server')
        GlobalSettings.logger.exception(e)
    finally:
        GlobalSettings.logger.debug('finished.')
    return file_key
# end of upload_zip_file function


def get_repo_files(base_temp_dir_name, commit_url, repo_name):
    temp_dir = tempfile.mkdtemp(dir=base_temp_dir_name, prefix='{0}_'.format(repo_name))
    download_repo(base_temp_dir_name,commit_url, temp_dir)
    repo_dir = os.path.join(temp_dir, repo_name.lower())
    if not os.path.isdir(repo_dir):
        repo_dir = temp_dir
    return repo_dir
# end of get_repo_files function


def download_repo(base_temp_dir_name, commit_url, repo_dir):
    """
    Downloads and unzips a git repository from Github or git.door43.org
    :param str|unicode commit_url: The URL of the repository to download
    :param str|unicode repo_dir:   The directory where the downloaded file should be unzipped
    :return: None
    """
    repo_zip_url = commit_url.replace('commit', 'archive') + '.zip'
    repo_zip_file = os.path.join(base_temp_dir_name, repo_zip_url.rpartition(os.path.sep)[2])

    try:
        GlobalSettings.logger.debug('Downloading {0}...'.format(repo_zip_url))

        # if the file already exists, remove it, we want a fresh copy
        if os.path.isfile(repo_zip_file):
            os.remove(repo_zip_file)

        download_file(repo_zip_url, repo_zip_file)
    finally:
        GlobalSettings.logger.debug('Downloading finished.')

    try:
        GlobalSettings.logger.debug('Unzipping {0}...'.format(repo_zip_file))
        # NOTE: This is unsafe if the zipfile comes from an untrusted source
        unzip(repo_zip_file, repo_dir)
    finally:
        GlobalSettings.logger.debug('Unzipping finished.')

    # clean up the downloaded zip file
    if os.path.isfile(repo_zip_file):
        os.remove(repo_zip_file)
#end of download_repo function


def process_job(prefix, queued_json_payload):
    """
    prefixable_vars = ['api_url', 'pre_convert_bucket_name', 'cdn_bucket_name', 'door43_bucket_name', 'language_stats_table_name',
                       #'linter_messaging_name', 'db_name', 'db_user']
    """
    print("Processing {0}job: {1}".format(prefix+' ' if prefix else '', queued_json_payload))

    # Setup a temp folder to use
    source_url_base = 'https://s3-{0}.amazonaws.com/{1}'.format(GlobalSettings.aws_region_name, GlobalSettings.pre_convert_bucket_name)
    # Move everything down one directory level for simple delete
    intermediate_dir_name = OUR_NAME
    base_temp_dir_name = os.path.join(tempfile.gettempdir(), intermediate_dir_name)
    try:
        os.makedirs(base_temp_dir_name)
    except:
        pass
    print("source_url_base", repr(source_url_base), "base_temp_dir_name", repr(base_temp_dir_name))

    # Get the commit_id, commit_url
    commit_id = queued_json_payload['after']
    commit = None
    for commit in queued_json_payload['commits']:
        if commit['id'] == commit_id:
            break
    commit_id = commit_id[:10]  # Only use the short form
    commit_url = commit['url']
    print("commit_id", repr(commit_id), "commit_url", repr(commit_url))

    # Gather other details from the commit that we will note for the job(s)
    user_name = queued_json_payload['repository']['owner']['username']
    repo_name = queued_json_payload['repository']['name']
    print("user_name", repr(user_name), "repo_name", repr(repo_name))
    compare_url = queued_json_payload['compare_url']
    commit_message = commit['message']
    print("compare_url", repr(compare_url), "commit_message", repr(commit_message))

    if 'pusher' in queued_json_payload:
        pusher = queued_json_payload['pusher']
    else:
        pusher = {'username': commit['author']['username']}
    pusher_username = pusher['username']
    print("pusher", repr(pusher), "pusher_username", repr(pusher_username))

    # Download and unzip the repo files
    repo_dir = get_repo_files(base_temp_dir_name, commit_url, repo_name)

    # Get the resource container
    rc = RC(repo_dir, repo_name)

    # Save manifest to manifest table
    manifest_data = {
        'repo_name': repo_name,
        'user_name': user_name,
        'lang_code': rc.resource.language.identifier,
        'resource_id': rc.resource.identifier,
        'resource_type': rc.resource.type,
        'title': rc.resource.title,
        'manifest': json.dumps(rc.as_dict()),
        'last_updated': datetime.utcnow()
    }
    print("client_webhook got manifest_data:", manifest_data ) # RJH


    # First see if manifest already exists in DB and update it if it is
    print("client_webhook getting manifest for {!r} with user {!r}".format(repo_name,user_name)) # RJH
    tx_manifest = TxManifest.get(repo_name=repo_name, user_name=user_name)
    if tx_manifest:
        for key, value in manifest_data.items():
            setattr(tx_manifest, key, value)
        GlobalSettings.logger.debug('Updating manifest in manifest table: {0}'.format(manifest_data))
        tx_manifest.update()
    else:
        tx_manifest = TxManifest(**manifest_data)
        GlobalSettings.logger.debug('Inserting manifest into manifest table: {0}'.format(tx_manifest))
        tx_manifest.insert()

    # Preprocess the files
    preprocess_dir = tempfile.mkdtemp(dir=base_temp_dir_name, prefix='preprocess_')
    results, preprocessor = do_preprocess(rc, repo_dir, preprocess_dir)

    # Zip up the massaged files
    zip_filepath = tempfile.mktemp(dir=base_temp_dir_name, suffix='.zip')
    GlobalSettings.logger.debug('Zipping files from {0} to {1}...'.format(preprocess_dir, zip_filepath))
    add_contents_to_zip(zip_filepath, preprocess_dir)
    GlobalSettings.logger.debug('Zipping finished.')

    # Upload zipped file to the S3 bucket
    file_key = upload_zip_file(commit_id, zip_filepath)

    #print("Webhook.process_job setting up TxJob with username={0}...".format(user.username))
    print("Webhook.process_job setting up TxJob...")
    job = TxJob()
    job.job_id = get_unique_job_id()
    job.identifier = job.job_id
    job.user_name = user_name
    job.repo_name = repo_name
    job.commit_id = commit_id
    job.manifests_id = tx_manifest.id
    job.created_at = datetime.utcnow()
    # Seems never used (RJH)
    #job.user = user.username  # Username of the token, not necessarily the repo's owner
    job.input_format = rc.resource.file_ext
    job.resource_type = rc.resource.identifier
    job.source = source_url_base + "/" + file_key
    job.cdn_bucket = GlobalSettings.cdn_bucket_name
    job.cdn_file = 'tx/job/{0}.zip'.format(job.job_id)
    job.output = 'https://{0}/{1}'.format(GlobalSettings.cdn_bucket_name, job.cdn_file)
    job.callback = GlobalSettings.api_url + '/client/callback'
    job.output_format = 'html'
    job.links = {
        "href": "{0}/tx/job/{1}".format(GlobalSettings.api_url, job.job_id),
        "rel": "self",
        "method": "GET"
    }
    job.success = False


    converter = get_converter_module(job)
    linter = get_linter_module(job)

    if converter:
        job.convert_module = converter.name
        job.started_at = datetime.utcnow()
        job.expires_at = job.started_at + timedelta(days=1)
        job.eta = job.started_at + timedelta(minutes=5)
        job.status = 'started'
        job.message = 'Conversion started...'
        job.log_message('Started job for {0}/{1}/{2}'.format(job.user_name, job.repo_name, job.commit_id))
    else:
        job.error_message('No converter was found to convert {0} from {1} to {2}'.format(job.resource_type,
                                                                                            job.input_format,
                                                                                            job.output_format))
        job.message = 'No converter found'
        job.status = 'failed'

    if linter:
        job.lint_module = linter.name
    else:
        GlobalSettings.logger.debug('No linter was found to lint {0}'.format(job.resource_type))

    job.insert()

    # Get S3 bucket/dir ready
    s3_commit_key = 'u/{0}/{1}/{2}'.format(job.user_name, job.repo_name, job.commit_id)
    clear_commit_directory_in_cdn(s3_commit_key)

    # Create a build log
    build_log_json = create_build_log(commit_id, commit_message, commit_url, compare_url, job,
                                            pusher_username, repo_name, user_name)
    # Upload an initial build_log
    upload_build_log_to_s3(base_temp_dir_name, build_log_json, s3_commit_key)

    # Update the project.json file
    update_project_json(base_temp_dir_name, commit_id, job, repo_name, user_name)

    # Convert and lint
    if converter:
        if not preprocessor.is_multiple_jobs():
            send_request_to_converter(job, converter)
            if linter:
                extra_payload = { 's3_results_key': s3_commit_key }
                send_request_to_linter(job, linter, commit_url, queued_json_payload, extra_payload=extra_payload)
        else:
            # -----------------------------
            # multiple book project
            # -----------------------------
            books = preprocessor.get_book_list()
            GlobalSettings.logger.debug('Splitting job into separate parts for books: ' + ','.join(books))
            book_count = len(books)
            build_log_json['multiple'] = True
            build_log_json['build_logs'] = []
            for i in range(0, len(books)):
                book = books[i]
                GlobalSettings.logger.debug('Adding job for {0}, part {1} of {2}'.format(book, i, book_count))
                # Send job request to tx-manager
                if i == 0:
                    book_job = job  # use the original job created above for the first book
                    book_job.identifier = '{0}/{1}/{2}/{3}'.format(job.job_id, book_count, i, book)
                else:
                    book_job = job.clone()  # copy the original job for this book's job
                    book_job.job_id = get_unique_job_id()
                    book_job.identifier = '{0}/{1}/{2}/{3}'.format(book_job.job_id, book_count, i, book)
                    book_job.cdn_file = 'tx/job/{0}.zip'.format(book_job.job_id)
                    book_job.output = 'https://{0}/{1}'.format(GlobalSettings.cdn_bucket_name, book_job.cdn_file)
                    book_job.links = {
                        "href": "{0}/tx/job/{1}".format(GlobalSettings.api_url, book_job.job_id),
                        "rel": "self",
                        "method": "GET"
                    }
                    book_job.insert()

                book_job.source = build_multipart_source(file_key, book)
                book_job.update()
                book_build_log = create_build_log(commit_id, commit_message, commit_url, compare_url, book_job,
                                                        pusher_username, repo_name, user_name)
                if len(book) > 0:
                    part = str(i)
                    book_build_log['book'] = book
                    book_build_log['part'] = part
                build_log_json['build_logs'].append(book_build_log)
                upload_build_log_to_s3(base_temp_dir_name, book_build_log, s3_commit_key, str(i) + "/")
                send_request_to_converter(book_job, converter)
                if linter:
                    extra_payload = {
                        'single_file': book,
                        's3_results_key': f'{s3_commit_key}/{i}'
                    }
                    send_request_to_linter(book_job, linter, commit_url, extra_payload)

    remove_tree(base_temp_dir_name)  # cleanup
    print("process_job() is returning:", build_log_json )
    return build_log_json
#end of process_job function


def job(queued_json_payload):
    """
    This function is called by the rq package to process a job in the queue(s).

    The job is removed from the queue before the job is started,
        but if the job throws an exception or times out (timeout specified in enqueue process)
            then the job gets added to the 'failed' queue.
    """
    start_time = time()
    stats_client.incr('JobsStarted')

    current_job = get_current_job()
    #print("Current job: {}".format(current_job)) # Mostly just displays the job number and payload
    #print("dir",dir(current_job))
    #print("id",current_job.id) # Displays job number
    #print("origin",current_job.origin) # Displays queue name
    #print("meta",current_job.meta) # Empty dict

    #print("Got a job from {0} queue: {1}".format(current_job.origin, queued_json_payload))
    print(f"\nGot job {current_job.id} from {current_job.origin} queue")
    queue_prefix = 'dev-' if current_job.origin.startswith('dev-') else ''
    assert queue_prefix == prefix
    process_job(queue_prefix, queued_json_payload)

    elapsed_seconds = round(time() - start_time)
    stats_client.gauge('JobTimeSeconds', elapsed_seconds)
    stats_client.incr('JobsCompleted')
    print(f"  Ok, job completed in {elapsed_seconds} seconds!")
# end of job function

# end of webhook.py
