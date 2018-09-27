# NOTE: This module name and function name are defined by the rq package and our own door43-enqueue-job package
# This code adapted by RJH June 2018 from tx-manager/client_webhook/ClientWebhook/process_webhook

# NOTE: rq_settings.py is executed at program start-up, reads some environment variables, and sets queue name, etc.
#       job() function (at bottom here) is executed by rq package when there is an available entry in the named queue.

# Python imports
import os
#import shutil
import tempfile
import logging
#import ssl
#import urllib.request as urllib2
from urllib import error as urllib_error
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import hashlib
from datetime import datetime
from time import time

# Library (PyPi) imports
from rq import get_current_job
from statsd import StatsClient # Graphite front-end

# Local imports
from rq_settings import prefix, REDIS_JOB_LIST
from general_tools.file_utils import unzip, add_contents_to_zip, write_file, remove_tree
from general_tools.url_utils import download_file
from resource_container.ResourceContainer import RC
from preprocessors.preprocessors import do_preprocess
from models.manifest import TxManifest
#from models.job import TxJob
#from models.module import TxModule
from global_settings.global_settings import GlobalSettings



OUR_NAME = 'DCS_job_handler'

 # Enable DEBUG logging for dev- instances (but less logging for production)
logging.basicConfig(level=logging.DEBUG if prefix else logging.ERROR)

our_adjusted_name = prefix + OUR_NAME
GlobalSettings(prefix=prefix)
if prefix not in ('', 'dev-'):
    GlobalSettings.logger.critical(f"Unexpected prefix: {prefix!r} -- expected '' or 'dev-'")

TX_POST_URL = f'https://git.door43.org/{prefix}tx/'
OUR_CALLBACK_URL = f'https://git.door43.org/{prefix}client/webhook/tx-callback/'


# Get the Graphite URL from the environment, otherwise use a local test instance
graphite_url = os.getenv('GRAPHITE_HOSTNAME', 'localhost')
stats_client = StatsClient(host=graphite_url, port=8125, prefix=our_adjusted_name)



def update_project_json(base_temp_dir_name, commit_id, upj_job_dict, repo_name, repo_owner):
    """
    :param string commit_id:
    :param dict upj_job_dict:
    :param string repo_name:
    :param string repo_owner:
    :return:
    """
    project_json_key = f'u/{repo_owner}/{repo_name}/project.json'
    project_json = GlobalSettings.cdn_s3_handler().get_json(project_json_key)
    project_json['user'] = repo_owner
    project_json['repo'] = repo_name
    project_json['repo_url'] = f'https://git.door43.org/{repo_owner}/{repo_name}'
    commit = {
        'id': commit_id,
        'created_at': upj_job_dict['created_at'],
        'status': upj_job_dict['status'],
        'success': upj_job_dict['success'],
        'started_at': None,
        'ended_at': None
    }
    # Get all other previous commits, and then add this one
    if 'commits' in project_json:
        commits = [c for c in project_json['commits'] if c['id'] != commit_id]
        commits.append(commit)
    else:
        commits = [commit]
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
    GlobalSettings.logger.debug(f'Saving build log to {GlobalSettings.cdn_bucket_name}/{upload_key}')
    GlobalSettings.cdn_s3_handler().upload_file(build_log_file, upload_key, cache_time=0)
    # GlobalSettings.logger.debug('build log contains: ' + json.dumps(build_log_json))
#end of upload_build_log_to_s3


def create_build_log(commit_id, commit_message, commit_url, compare_url, cbl_job, pusher_username, repo_name, repo_owner):
    """
    :param string commit_id:
    :param string commit_message:
    :param string commit_url:
    :param string compare_url:
    :param TxJob cbl_job:
    :param string pusher_username:
    :param string repo_name:
    :param string repo_owner:
    :return dict:
    """
    build_log_json = dict(cbl_job)
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
        GlobalSettings.logger.debug(f"Removing s3 cdn file: {obj.key}")
        GlobalSettings.cdn_s3_handler().delete_file(obj.key)
# end of clear_commit_directory_in_cdn function


def get_unique_job_id():
    """
    :return string:
    """
    job_id = hashlib.sha256(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f").encode('utf-8')).hexdigest()
    #while TxJob.get(job_id):
        #job_id = hashlib.sha256(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f").encode('utf-8')).hexdigest()
    return job_id
# end of get_unique_job_id()


def upload_zip_file(commit_id, zip_filepath):
    """
    """
    file_key = f'preconvert/{commit_id}.zip'
    GlobalSettings.logger.debug(f'Uploading {zip_filepath} to {GlobalSettings.pre_convert_bucket_name}/{file_key}...')
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
    """
    """
    temp_dir = tempfile.mkdtemp(dir=base_temp_dir_name, prefix=f'{repo_name}_')
    download_repo(base_temp_dir_name, commit_url, temp_dir)
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
        GlobalSettings.logger.debug(f'Downloading {repo_zip_url}...')

        # if the file already exists, remove it, we want a fresh copy
        if os.path.isfile(repo_zip_file):
            os.remove(repo_zip_file)

        download_file(repo_zip_url, repo_zip_file)
    finally:
        GlobalSettings.logger.debug('Downloading finished.')

    try:
        GlobalSettings.logger.debug(f'Unzipping {repo_zip_file}...')
        # NOTE: This is unsafe if the zipfile comes from an untrusted source
        unzip(repo_zip_file, repo_dir)
    finally:
        GlobalSettings.logger.debug('Unzipping finished.')

    # clean up the downloaded zip file
    if os.path.isfile(repo_zip_file):
        os.remove(repo_zip_file)
#end of download_repo function


def remember_job(rj_job_dict, rj_redis_connection):
    """
    Save this outstanding job in a REDIS dict
        so that we can match it when we get a callback
    """
    print(f"\nremember_job({rj_job_dict})")
    outstanding_jobs_dict = rj_redis_connection.hgetall(REDIS_JOB_LIST)
    if outstanding_jobs_dict is None:
        print("Created new outstanding_jobs_dict")
        outstanding_jobs_dict = {}
    else: print("Got outstanding_jobs_dict:", outstanding_jobs_dict)
    print(f"Already had {len(outstanding_jobs_dict)} outstanding job(s) in {REDIS_JOB_LIST!r}")
    assert rj_job_dict['job_id'] not in outstanding_jobs_dict
    outstanding_jobs_dict[rj_job_dict['job_id']] = rj_job_dict
    GlobalSettings.logger.info(f"Now have {len(outstanding_jobs_dict)} outstanding job(s) in {REDIS_JOB_LIST!r}")
    rj_redis_connection.hmset(REDIS_JOB_LIST, outstanding_jobs_dict)
# end of remember_job



def process_job(pj_prefix, queued_json_payload, redis_connection):
    """
    Parameters:
        pj_prefix in '' or 'dev-'
        queued_json_payload is a dict
        redis_connection is a StrictRedis instance

    Sets up a temp folder in the AWS S3 bucket.

    It gathers details from the JSON payload.

    It downloads a zip file from the DCS repo to the temp folder and unzips the files,
        and then creates a ResourceContainer (RC) object.

    It creates a manifest_data dictionary,
        gets a TxManifest from the DB and updates it with the above,
        or creates a new one if none existed.

    It then gets and runs a preprocessor on the files in the temp folder.
        A preprocessor has a ResourceContainer (RC) and source and output folders.
        It copies the file(s) from the RC in the source folder, over to the output folder,
            assembling chunks/chapters if necessary.

    The preprocessed files are zipped up in the temp folder
        and then uploaded to the pre-convert bucket in S3.

    A TxJob is now setup and passed on to TxModule in order to
            query the AWS Dynamo DB to
            select a converter module, and
            a linter module.
        The converter and linter settings are then added to the job info
            and the job is inserted into the DB table.

    An S3 CDN folder is now named and emptied
        and a build log dictionary is created and uploaded to it.

    The project.json (in the folder above the CDN one) is also updated, e.g., with new commits.

    Conversion and linting are now initiated by sending a request to each,
        or by creating book_jobs and sending multiple requests to each.
    (These requests are currently initiated by invoking AWS Lambda functions
        which in turn call tX-manager functions.)

    This code is "successful" once the conversion/linting jobs are submitted --
        it currently has no way to determine if they actually get completed.

    The given payload will be appended to the 'failed' queue
        if an exception is thrown in this module.
    """
    #print(f"Processing {pj_prefix+' ' if pj_prefix else ''}job: {queued_json_payload}")

    # Setup a temp folder to use
    source_url_base = f'https://s3-{GlobalSettings.aws_region_name}.amazonaws.com/{GlobalSettings.pre_convert_bucket_name}'
    # Move everything down one directory level for simple delete
    intermediate_dir_name = OUR_NAME
    base_temp_dir_name = os.path.join(tempfile.gettempdir(), intermediate_dir_name)
    try:
        os.makedirs(base_temp_dir_name)
    except:
        pass
    #print("source_url_base", repr(source_url_base), "base_temp_dir_name", repr(base_temp_dir_name))

    # Get the commit_id, commit_url
    commit_id = queued_json_payload['after']
    commit = None
    for commit in queued_json_payload['commits']:
        if commit['id'] == commit_id:
            break
    commit_id = commit_id[:10]  # Only use the short form
    commit_url = commit['url']
    #print("commit_id", repr(commit_id), "commit_url", repr(commit_url))

    # Gather other details from the commit that we will note for the job(s)
    user_name = queued_json_payload['repository']['owner']['username']
    repo_name = queued_json_payload['repository']['name']
    #print("user_name", repr(user_name), "repo_name", repr(repo_name))
    compare_url = queued_json_payload['compare_url']
    commit_message = commit['message']
    #print("compare_url", repr(compare_url), "commit_message", repr(commit_message))

    if 'pusher' in queued_json_payload:
        pusher = queued_json_payload['pusher']
    else:
        pusher = {'username': commit['author']['username']}
    pusher_username = pusher['username']
    #print("pusher", repr(pusher), "pusher_username", repr(pusher_username))

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
    print("client_webhook got manifest_data:", manifest_data)


    # First see if manifest already exists in DB and update it if it is
    print(f"client_webhook getting manifest for {repo_name!r} with user {user_name!r}")
    tx_manifest = TxManifest.get(repo_name=repo_name, user_name=user_name)
    if tx_manifest:
        for key, value in manifest_data.items():
            setattr(tx_manifest, key, value)
        GlobalSettings.logger.debug(f'Updating manifest in manifest table: {manifest_data}')
        tx_manifest.update()
    else:
        tx_manifest = TxManifest(**manifest_data)
        GlobalSettings.logger.debug(f'Inserting manifest into manifest table: {tx_manifest}')
        tx_manifest.insert()

    # Preprocess the files
    print("Preprocessing files...")
    preprocess_dir = tempfile.mkdtemp(dir=base_temp_dir_name, prefix='preprocess_')
    do_preprocess(rc, repo_dir, preprocess_dir)

    # Zip up the massaged files
    print("Zipping files...")
    zip_filepath = tempfile.mktemp(dir=base_temp_dir_name, suffix='.zip')
    GlobalSettings.logger.debug(f'Zipping files from {preprocess_dir} to {zip_filepath}...')
    add_contents_to_zip(zip_filepath, preprocess_dir)
    GlobalSettings.logger.debug('Zipping finished.')

    # Upload zipped file to the S3 pre-convert bucket
    print("Uploading zip file...")
    file_key = upload_zip_file(commit_id, zip_filepath)

    print("Webhook.process_job setting up job dict...")
    pj_job_dict = {}
    pj_job_dict['job_id'] = get_unique_job_id()
    pj_job_dict['identifier'] = pj_job_dict['job_id']
    pj_job_dict['user_name'] = user_name
    pj_job_dict['repo_name'] = repo_name
    pj_job_dict['commit_id'] = commit_id
    pj_job_dict['manifests_id'] = tx_manifest.id
    pj_job_dict['created_at'] = datetime.utcnow()
    # Seems never used (RJH)
    #pj_job_dict['user = user.username  # Username of the token, not necessarily the repo's owner
    pj_job_dict['input_format'] = rc.resource.file_ext
    pj_job_dict['resource_type'] = rc.resource.identifier
    pj_job_dict['source'] = source_url_base + "/" + file_key
    pj_job_dict['cdn_bucket'] = GlobalSettings.cdn_bucket_name
    pj_job_dict['cdn_file'] = f"tx/job/{pj_job_dict['job_id']}.zip"
    pj_job_dict['output'] = f"https://{GlobalSettings.cdn_bucket_name}/{pj_job_dict['cdn_file']}"
    pj_job_dict['callback'] = GlobalSettings.api_url + '/client/callback'
    pj_job_dict['output_format'] = 'html'
    pj_job_dict['links'] = {
        'href': f"{GlobalSettings.api_url}/tx/job/{pj_job_dict['job_id']}",
        'rel': 'self',
        'method': 'GET'
    }
    pj_job_dict['status'] = None
    pj_job_dict['success'] = False


    # Save the job info in Redis for the callback to use
    remember_job(pj_job_dict, redis_connection)

    # Get S3 cdn bucket/dir and empty it
    s3_commit_key = f"u/{pj_job_dict['user_name']}/{pj_job_dict['repo_name']}/{pj_job_dict['commit_id']}"
    clear_commit_directory_in_cdn(s3_commit_key)

    # Create a build log
    build_log_json = create_build_log(commit_id, commit_message, commit_url, compare_url, pj_job_dict,
                                      pusher_username, repo_name, user_name)
    # Upload an initial build_log
    upload_build_log_to_s3(base_temp_dir_name, build_log_json, s3_commit_key)

    # Update the project.json file
    update_project_json(base_temp_dir_name, commit_id, pj_job_dict, repo_name, user_name)

    # Pass the work onto the tX system
    print(f"About to POST request to tX system @ {TX_POST_URL}")
    # TODO: What headers do we really need?
    headers = {"Content-type": "application/json",}
    # TODO: What payload do we really need?
    tx_payload = {
        'identifier': pj_job_dict['identifier'],
        'source_url': pj_job_dict['source'],
        'resource_id': pj_job_dict['resource_type'],
        'cdn_bucket': pj_job_dict['cdn_bucket'],
        'cdn_file': pj_job_dict['cdn_file'],
        'callback': OUR_CALLBACK_URL,
    }
    if 'options' in pj_job_dict:
        tx_payload['options'] = pj_job_dict['options']
    req = Request(TX_POST_URL, urlencode(tx_payload).encode(), headers)
    try:
        response = urlopen(req)
        print('response', response)
        response_json = response.read().decode()
        print('response json', response_json)
    except urllib_error.HTTPError as e:
        logging.error(f'tX POST request got {e}')

    remove_tree(base_temp_dir_name)  # cleanup
    print(f"{pj_prefix}Door43-Job-Handler process_job() is finishing with: {build_log_json}")
    #return build_log_json
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
    #print(f"Current job: {current_job}") # Mostly just displays the job number and payload
    #print("dir",dir(current_job))
    #   dir ['__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_args', '_data', '_dependency_id', '_execute', '_func_name', '_get_status', '_id', '_instance', '_kwargs', '_result', '_set_status', '_status', '_unpickle_data', 'args', 'cancel', 'cleanup', 'connection', 'create', 'created_at', 'data', 'delete', 'delete_dependents', 'dependency', 'dependent_ids', 'dependents_key', 'dependents_key_for', 'description', 'ended_at', 'enqueued_at', 'exc_info', 'exists', 'fetch', 'func', 'func_name', 'get_call_string', 'get_id', 'get_result_ttl', 'get_status', 'get_ttl', 'id', 'instance', 'is_failed', 'is_finished', 'is_queued', 'is_started', 'key', 'key_for', 'kwargs', 'meta', 'origin', 'perform', 'redis_job_namespace_prefix', 'refresh', 'register_dependency', 'result', 'result_ttl', 'return_value', 'save', 'save_meta', 'set_id', 'set_status', 'started_at', 'status', 'timeout', 'to_dict', 'ttl']
    #for fieldname in current_job.__dict__:
        #print(f"{fieldname}: {current_job.__dict__[fieldname]}")
    #print("id",current_job.id) # Displays job number
    #print("origin",current_job.origin) # Displays queue name
    #print("meta",current_job.meta) # Empty dict

    #print(f"Got a job from {current_job.origin} queue: {queued_json_payload}")
    #print(f"\nGot job {current_job.id} from {current_job.origin} queue")
    #queue_prefix = 'dev-' if current_job.origin.startswith('dev-') else ''
    #assert queue_prefix == prefix
    process_job(prefix, queued_json_payload, current_job.connection)

    elapsed_seconds = round(time() - start_time)
    stats_client.gauge('JobTimeSeconds', elapsed_seconds)
    stats_client.incr('JobsCompleted')
    print(f"  Ok, {prefix}Door43 job submission to tX completed in {elapsed_seconds} seconds!")
# end of job function

# end of webhook.py
