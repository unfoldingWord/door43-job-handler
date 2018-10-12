# NOTE: This module name and function name are defined by the rq package and our own door43-enqueue-job package
# This code adapted by RJH Sept 2018 from webhook.py

# NOTE: rq_settings.py is executed at program start-up, reads some environment variables, and sets queue name, etc.
#       job() function (at bottom here) is executed by rq package when there is an available entry in the named queue.

# Python imports
import os
#import shutil
import tempfile
#import ssl
#from urllib import error as urllib_error
#from urllib.parse import urlencode
#from urllib.request import Request, urlopen
#import json
#import hashlib
from datetime import datetime #, timedelta
from time import time

# Library (PyPi) imports
#import requests
from rq import get_current_job
from statsd import StatsClient # Graphite front-end

# Local imports
from rq_settings import prefix, debug_mode_flag, REDIS_JOB_LIST
from general_tools.file_utils import unzip, write_file, remove_tree
from general_tools.url_utils import download_file
#from resource_container.ResourceContainer import RC
#from preprocessors.preprocessors import do_preprocess
#from models.manifest import TxManifest
#from models.job import TxJob
#from models.module import TxModule
from global_settings.global_settings import GlobalSettings



#OUR_NAME = 'Door43_callback_handler'

GlobalSettings(prefix=prefix)
if prefix not in ('', 'dev-'):
    GlobalSettings.logger.critical(f"Unexpected prefix: {prefix!r} -- expected '' or 'dev-'")
stats_prefix = f"door43.{'dev' if prefix else 'prod'}.job-handler" # Can't add .callback here coz we also have .total


# Get the Graphite URL from the environment, otherwise use a local test instance
graphite_url = os.getenv('GRAPHITE_HOSTNAME', 'localhost')
stats_client = StatsClient(host=graphite_url, port=8125, prefix=stats_prefix)



def update_project_json(base_temp_dir_name, commit_id, upj_job, repo_name, repo_owner):
    """
    :param string commit_id:
    :param dict upj_job:
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
        'created_at': upj_job.created_at,
        'status': upj_job.status,
        'success': upj_job.success,
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
    :param dict cbl_job:
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
        GlobalSettings.logger.debug('Removing s3 cdn file: ' + obj.key)
        GlobalSettings.cdn_s3_handler().delete_file(obj.key)
# end of clear_commit_directory_in_cdn function


def build_multipart_source(source_url_base, file_key, book_filename):
    params = urlencode({'convert_only': book_filename})
    source_url = f'{source_url_base}/{file_key}?{params}'
    return source_url
# end of build_multipart_source function


def upload_zip_file(commit_id, zip_filepath):
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


def verify_expected_job(vej_job_dict, vej_redis_connection):
    """
    Check that we have this outstanding job in a REDIS dict
        and delete it once we make a match.

    Return True or False
    """
    GlobalSettings.logger.debug(f"verify_expected_job({vej_job_dict['job_id']})")
    outstanding_jobs_dict = vej_redis_connection.hgetall(REDIS_JOB_LIST) # Gets bytes!!!
    if not outstanding_jobs_dict:
        GlobalSettings.logger.error("No expected jobs found")
        return False
    GlobalSettings.logger.debug(f"Got outstanding_jobs_dict: {outstanding_jobs_dict}")
    GlobalSettings.logger.debug(f"Currently have {len(outstanding_jobs_dict)}"
                                f" outstanding job(s) in {REDIS_JOB_LIST!r}")
    job_id_bytes = vej_job_dict['job_id'].encode()
    if job_id_bytes not in outstanding_jobs_dict:
        GlobalSettings.logger.error(f"Not expecting job with id of {vej_job_dict['job_id']}")
        return False
    # We found a match -- delete that job from the outstanding list
    GlobalSettings.logger.debug(f"Found match with {outstanding_jobs_dict[job_id_bytes]}")
    del outstanding_jobs_dict[job_id_bytes]
    GlobalSettings.logger.debug(f"Still have {len(outstanding_jobs_dict)} outstanding job(s)"
                                f" in {REDIS_JOB_LIST!r}")
    vej_redis_connection.hmset(REDIS_JOB_LIST, outstanding_jobs_dict)
    return True
# end of verify_expected_job


def process_callback(pc_prefix, queued_json_payload, redis_connection):
    """
    The job info is retrieved from REDIS and matched/checked
    The converted file(s) are downloaded
    Templating is done
    The results are uploaded to the S3 CDN bucket
    The final log is uploaded to the S3 CDN bucket

    The given payload will be appended to the 'failed' queue
        if an exception is thrown in this module.
    """
    GlobalSettings.logger.debug(f"Processing {pc_prefix+' ' if pc_prefix else ''}callback: {queued_json_payload}")

    ## Setup a temp folder to use
    #source_url_base = f'https://s3-{GlobalSettings.aws_region_name}.amazonaws.com/{GlobalSettings.pre_convert_bucket_name}'
    ## Move everything down one directory level for simple delete
    #intermediate_dir_name = OUR_NAME
    #base_temp_dir_name = os.path.join(tempfile.gettempdir(), intermediate_dir_name)
    #try:
        #os.makedirs(base_temp_dir_name)
    #except:
        #pass
    ##print("source_url_base", repr(source_url_base), "base_temp_dir_name", repr(base_temp_dir_name))

    # Check that this is an expected callback job
    if 'job_id' not in queued_json_payload:
        error = "Callback job has no 'job_id' field"
        GlobalSettings.logger.error(error)
        raise Exception(error)
    if not verify_expected_job(queued_json_payload, redis_connection):
        error = f'No job found for {queued_json_payload}'
        GlobalSettings.logger.critical(error)
        raise Exception(error)

    # TODO: The following code is just pasted from tx-manager/client_converter_callback.py
    #           and not yet adapted
    temp_dir = tempfile.mkdtemp(suffix='', prefix='client_callback_')

    job_id_parts = identifier.split('/')
    job_id = job_id_parts[0]
    this_job = TxJob.get(job_id)

    if not this_job:
        error = f"No job found for job_id = {job_id}, identifier = {identifier}"
        GlobalSettings.logger.error(error)
        raise Exception(error)

    if len(job_id_parts) == 4:
        part_count, part_id, book = job_id_parts[1:]
        GlobalSettings.logger.debug(f"Multiple project, part {part_id} of {part_count}, converting book {book}")
        multiple_project = True
    else:
        GlobalSettings.logger.debug('Single project')
        part_id = None
        multiple_project = False

    this_job.ended_at = datetime.utcnow()
    this_job.success = success
    for message in log:
        this_job.log_message(message)
    for message in warnings:
        this_job.warnings_message(message)
    for message in errors:
        this_job.error_message(message)
    if len(errors):
        this_job.log_message('{0} function returned with errors.'.format(this_job.convert_module))
    elif len(warnings):
        this_job.log_message('{0} function returned with warnings.'.format(this_job.convert_module))
    else:
        this_job.log_message('{0} function returned successfully.'.format(this_job.convert_module))

    if not success or this_job.errors:
        this_job.success = False
        this_job.status = "failed"
        message = "Conversion failed"
        GlobalSettings.logger.debug(f"Conversion failed, success: {success}, errors: {this_job.errors}")
    elif this_job.warnings:
        this_job.success = True
        this_job.status = "warnings"
        message = "Conversion successful with warnings"
    else:
        this_job.success = True
        this_job.status = "success"
        message = "Conversion successful"

    this_job.message = message
    this_job.log_message(message)
    this_job.log_message('Finished job {0} at {1}'.format(this_job.job_id, this_job.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ")))

    s3_commit_key = 'u/{0}/{1}/{2}'.format(this_job.user_name, this_job.repo_name, this_job.commit_id)
    upload_key = s3_commit_key
    if multiple_project:
        upload_key += "/" + part_id

    GlobalSettings.logger.debug(f"Callback for commit {s3_commit_key}...")

    # Download the ZIP file of the converted files
    converted_zip_url = this_job.output
    converted_zip_file = os.path.join(temp_dir, converted_zip_url.rpartition('/')[2])
    remove(converted_zip_file)  # make sure old file not present
    download_success = True
    GlobalSettings.logger.debug(f"Downloading converted zip file from {converted_zip_url}...")
    try:
        download_file(converted_zip_url, converted_zip_file)
    except:
        download_success = False  # if multiple project we note fail and move on
        if not multiple_project:
            remove_tree(temp_dir)  # cleanup
        if this_job.errors is None:
            this_job.errors = []
        this_job.errors.append("Missing converted file: " + converted_zip_url)
    finally:
        GlobalSettings.logger.debug(f"download finished, success={download_success}")

    this_job.update()

    if download_success:
        # Unzip the archive
        unzip_dir = unzip_converted_files(converted_zip_file)

        # Upload all files to the cdn_bucket with the key of <user>/<repo_name>/<commit> of the repo
        upload_converted_files(upload_key, unzip_dir)

    if multiple_project:
        # Now download the existing build_log.json file, update it and upload it back to S3 as convert_log
        build_log_json = update_convert_log(s3_commit_key, part_id + "/")

        # mark current part as finished
        cdn_upload_contents({}, s3_commit_key + '/' + part_id + '/finished')

    else:  # single part conversion
        # Now download the existing build_log.json file, update it and upload it back to S3 as convert_log
        build_log_json = update_convert_log(s3_commit_key)

        cdn_upload_contents({}, s3_commit_key + '/finished')  # flag finished

    results = ClientLinterCallback.deploy_if_conversion_finished(s3_commit_key, identifier)
    if results:
        all_parts_completed = True
        build_log_json = results

    remove_tree(temp_dir)  # cleanup
    GlobalSettings.logger.info(f"Door43-Job-Handler process_callback() is finishing with: {build_log_json}")
    #return build_log_json
#end of process_callback function


def job(queued_json_payload):
    """
    This function is called by the rq package to process a job in the queue(s).

    The job is removed from the queue before the job is started,
        but if the job throws an exception or times out (timeout specified in enqueue process)
            then the job gets added to the 'failed' queue.
    """
    GlobalSettings.logger.info("Door43-Job-Handler received a callback" + (" (in debug mode)" if debug_mode_flag else ""))
    start_time = time()
    stats_client.incr('callback.jobs.attempted')

    current_job = get_current_job()
    #print(f"Current job: {current_job}") # Mostly just displays the job number and payload
    #print("id",current_job.id) # Displays job number
    #print("origin",current_job.origin) # Displays queue name
    #print("meta",current_job.meta) # Empty dict

    #print(f"Got a job from {current_job.origin} queue: {queued_json_payload}")
    #print(f"\nGot job {current_job.id} from {current_job.origin} queue")
    #queue_prefix = 'dev-' if current_job.origin.startswith('dev-') else ''
    #assert queue_prefix == prefix
    process_callback(prefix, queued_json_payload, current_job.connection)

    elapsed_milliseconds = round((time() - start_time) * 1000)
    stats_client.timing('callback.job.duration', elapsed_milliseconds)
    GlobalSettings.logger.info(f"Door43 callback handling completed in {elapsed_milliseconds:,} milliseconds")

    # Calculate total elapsed time for the job
    total_elapsed_time = datetime.utcnow() - \
                         datetime.strptime(queued_json_payload['door43_webhook_received_at'],
                                           ("%Y-%m-%dT%H:%M:%SZ"))
    total_elapsed_milliseconds = round(total_elapsed_time.total_seconds() * 1000)
    GlobalSettings.logger.info(f"Door43 total job completed in {total_elapsed_milliseconds:,} milliseconds")
    stats_client.timing('total.job.duration', total_elapsed_milliseconds)

    stats_client.incr('callback.jobs.succeeded')
# end of job function

# end of callback.py for door43_enqueue_job
