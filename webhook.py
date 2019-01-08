# NOTE: This module name and function name are defined by the rq package and our own door43-enqueue-job package
# This code adapted by RJH June 2018 from tx-manager/client_webhook/ClientWebhook/process_webhook

# NOTE: rq_settings.py is executed at program start-up, reads some environment variables, and sets queue name, etc.
#       job() function (at bottom here) is executed by rq package when there is an available entry in the named queue.

# Python imports
import os
import tempfile
import json
import hashlib
from datetime import datetime, timedelta
from time import time
from ast import literal_eval
import traceback

# Library (PyPi) imports
import requests
from rq import get_current_job
from statsd import StatsClient # Graphite front-end

# Local imports
from rq_settings import prefix, debug_mode_flag, gogs_user_token, tx_post_url, REDIS_JOB_LIST
from general_tools.file_utils import unzip, add_contents_to_zip, write_file, remove_tree
from general_tools.url_utils import download_file
from resource_container.ResourceContainer import RC
from preprocessors.preprocessors import do_preprocess
from models.manifest import TxManifest
from global_settings.global_settings import GlobalSettings



OUR_NAME = 'Door43_job_handler'

GlobalSettings(prefix=prefix)
if prefix not in ('', 'dev-'):
    GlobalSettings.logger.critical(f"Unexpected prefix: '{prefix}' -- expected '' or 'dev-'")
general_stats_prefix = f"door43.{'dev' if prefix else 'prod'}.job-handler"
stats_prefix = f'{general_stats_prefix}.webhook'
prefixed_our_name = prefix + OUR_NAME


# TX_POST_URL = f'https://git.door43.org/{prefix}tx/'
DOOR43_CALLBACK_URL = f'https://git.door43.org/{prefix}client/webhook/tx-callback/'
ADJUSTED_DOOR43_CALLBACK_URL = 'http://127.0.0.1:8080/tx-callback/' \
                                    if prefix and debug_mode_flag and ':8090' in tx_post_url \
                                 else DOOR43_CALLBACK_URL


# Get the Graphite URL from the environment, otherwise use a local test instance
graphite_url = os.getenv('GRAPHITE_HOSTNAME', 'localhost')
stats_client = StatsClient(host=graphite_url, port=8125)



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
    GlobalSettings.logger.debug(f"Clearing objects from commit directory '{s3_commit_key}' …")
    for obj in GlobalSettings.cdn_s3_handler().get_objects(prefix=s3_commit_key):
        # GlobalSettings.logger.debug(f"Removing s3 cdn file: {obj.key} …")
        GlobalSettings.cdn_s3_handler().delete_file(obj.key)
# end of clear_commit_directory_in_cdn function


def get_unique_job_id():
    """
    :return string:
    """
    job_id = hashlib.sha256(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f').encode('utf-8')).hexdigest()
    #while TxJob.get(job_id):
        #job_id = hashlib.sha256(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f').encode('utf-8')).hexdigest()
    return job_id
# end of get_unique_job_id()


def upload_zip_file(job_id, zip_filepath):
    """
    """
    file_key = f'preconvert/{job_id}.zip'
    GlobalSettings.logger.debug(f'Uploading {zip_filepath} to {GlobalSettings.pre_convert_bucket_name}/{file_key} …')
    try:
        GlobalSettings.pre_convert_s3_handler().upload_file(zip_filepath, file_key, cache_time=0)
    except Exception as e:
        GlobalSettings.logger.error('Failed to upload zipped repo up to server')
        GlobalSettings.logger.exception(e)
    finally:
        GlobalSettings.logger.debug('Upload finished.')
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
    :param str commit_url: The URL of the repository to download
    :param str repo_dir:   The directory where the downloaded file should be unzipped
    :return: None
    """
    repo_zip_url = commit_url.replace('commit', 'archive') + '.zip'
    repo_zip_file = os.path.join(base_temp_dir_name, repo_zip_url.rpartition(os.path.sep)[2])

    try:
        GlobalSettings.logger.debug(f'Downloading {repo_zip_url} …')

        # If the file already exists, remove it, we want a fresh copy
        if os.path.isfile(repo_zip_file):
            os.remove(repo_zip_file)

        download_file(repo_zip_url, repo_zip_file)
    finally:
        GlobalSettings.logger.debug('Downloading finished.')

    try:
        GlobalSettings.logger.debug(f'Unzipping {repo_zip_file} …')
        # NOTE: This is unsafe if the zipfile comes from an untrusted source
        unzip(repo_zip_file, repo_dir)
    finally:
        GlobalSettings.logger.debug('Unzipping finished.')

    # Remove the downloaded zip file (now unzipped)
    if not prefix: # For dev- save this file longer
        if os.path.isfile(repo_zip_file):
            os.remove(repo_zip_file)
#end of download_repo function


def remember_job(rj_job_dict, rj_redis_connection):
    """
    Save this outstanding job in a REDIS dict
        so that we can match it when we get a callback
    """
    # GlobalSettings.logger.debug(f"remember_job( {rj_job_dict['job_id']} )")

    outstanding_jobs_dict = rj_redis_connection.hgetall(REDIS_JOB_LIST) # Gets bytes!!!
    if outstanding_jobs_dict is None:
        GlobalSettings.logger.info("Created new outstanding_jobs_dict")
        outstanding_jobs_dict = {}
    # else:
    #    GlobalSettings.logger.debug(f"Got outstanding_jobs_dict: "
    #                                f" ({len(outstanding_jobs_dict)}) {outstanding_jobs_dict.keys()}")

    if outstanding_jobs_dict:
        GlobalSettings.logger.info(f"Already had {len(outstanding_jobs_dict)}"
                                   f" outstanding job(s) in '{REDIS_JOB_LIST}' redis store.")
        # Remove any outstanding jobs more than two weeks old
        for outstanding_job_id_bytes in outstanding_jobs_dict.copy():
            # print(f"\nLooking at outstanding job {outstanding_job_id_bytes}")
            outstanding_job_dict_bytes = rj_redis_connection.hget(REDIS_JOB_LIST, outstanding_job_id_bytes)
            outstanding_job_dict = literal_eval(outstanding_job_dict_bytes.decode()) # bytes -> str -> dict
            # print(f"Got outstanding_job_dict: {outstanding_job_dict!r}")
            outstanding_duration = datetime.utcnow() \
                                - datetime.strptime(outstanding_job_dict['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            if outstanding_duration >= timedelta(weeks=2):
                GlobalSettings.logger.info(f"Deleting expired saved job from {outstanding_job_dict['created_at']}")
                del_result = rj_redis_connection.hdel(REDIS_JOB_LIST, outstanding_job_id_bytes) # Delete from Redis
                # print("  Got delete result:", del_result)
                assert del_result == 1
                del outstanding_jobs_dict[outstanding_job_id_bytes] # Delete from our local copy also
        # This new job shouldn't already be in the outstanding jobs dict
        assert rj_job_dict['job_id'].encode() not in outstanding_jobs_dict # bytes comparison

    # Add this job to Redis
    outstanding_jobs_dict[rj_job_dict['job_id']] = rj_job_dict
    GlobalSettings.logger.info(f"Now have {len(outstanding_jobs_dict)}"
                               f" outstanding job(s) in '{REDIS_JOB_LIST}' redis store.")
    rj_redis_connection.hmset(REDIS_JOB_LIST, outstanding_jobs_dict)
# end of remember_job


def upload_to_BDB(job_name, BDB_zip_filepath):
    """
    Upload a Bible job (usfm) to the Bible Drop Box.

    Included here temporarily as a way to compare handling of USFM files
        and for a comparison of warnings/errors that are detected/displayed.
        (Would have to be manually compared -- nothing is done here with the BDB results.)
    """
    GlobalSettings.logger.debug(f"upload_to_BDB({job_name, BDB_zip_filepath})…")
    BDB_url = 'http://Freely-Given.org/Software/BibleDropBox/SubmitAction.phtml'
    files_data = {
        'nameLine': (None, prefixed_our_name),
        'emailLine': (None, 'noone@nowhere.org'),
        'projectLine': (None, job_name),
            'doChecks': (None, 'Yes'),
                'NTfinished': (None, 'No'),
                'OTfinished': (None, 'No'),
                'DCfinished': (None, 'No'),
                'ALLfinished': (None, 'No'),
            'doExports': (None, 'Yes'),
                'photoBible': (None, 'No'),
                'odfs': (None, 'No'),
                'pdfs': (None, 'No'),
        'goalLine': (None, 'test'),
            'permission': (None, 'Yes'),
        'uploadedZipFile': (os.path.basename(BDB_zip_filepath), open(BDB_zip_filepath, 'rb'), 'application/zip'),
        'uploadedMetadataFile': ('', b''),
        'submit': (None, 'Submit'),
        }
    GlobalSettings.logger.debug(f"Posting data to {BDB_url} …")
    try:
        response = requests.post(BDB_url, files=files_data)
    except requests.exceptions.ConnectionError as e:
        GlobalSettings.logger.critical(f"BDB connection error: {e}")
        response = None

    if response:
        GlobalSettings.logger.info(f"BDB response.status_code = {response.status_code}, response.reason = {response.reason}")
        GlobalSettings.logger.debug(f"BDB response.headers = {response.headers}")
        # GlobalSettings.logger.debug(f"BDB response.text = {response.text}")
        if response.status_code == 200:
            if "Your project has been submitted" in response.text:
                ix = response.text.find('eventually be available <a href="')
                if ix != -1:
                    ixStart = ix + 33
                    ixEnd = response.text.find('">here</a>')
                    job_url = response.text[ixStart:ixEnd]
                    GlobalSettings.logger.info(f"BDB results will be available at http://Freely-Given.org/Software/BibleDropBox/{job_url}")
            else:
                GlobalSettings.logger.error(f"BDB didn't accept job: {response.text}")
        else:
            GlobalSettings.logger.error(f"Failed to submit job to BDB:"
                                           f" {response.status_code}={response.reason}")
    else: # no response
        error_msg = "Submission of job to BDB got no response"
        GlobalSettings.logger.error(error_msg)
        #raise Exception(error_msg) # Is this the best thing to do here?
# end of upload_to_BDB


user_projects_invoked_string = 'user-projects.invoked.unknown--unknown'
def process_job(queued_json_payload, redis_connection):
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

    A job dict is now setup and remembered in REDIS
        so that we can match it when we get a future callback.

    An S3 CDN folder is now named and emptied
        and a build log dictionary is created and uploaded to it.

    The project.json (in the folder above the CDN one) is also updated, e.g., with new commits.

    The job is now passed to the tX system by means of a
        POST to the tX webhook (which should hopefully respond with a callback).

    This code is "successful" once the job is submitted --
        it has no way to determine if it actually gets completed
        other than if a callback is made.

    The given payload will be automatically appended to the 'failed' queue
        by rq if an exception is thrown in this module.
    """
    global user_projects_invoked_string
    GlobalSettings.logger.debug(f"Processing {prefix+' ' if prefix else ''}job: {queued_json_payload}")


    #  Update repo/owner/pusher stats
    #   (all the following fields are expected from the Gitea webhook from push)
    try:
        stats_client.set(f'{stats_prefix}.repo_ids', queued_json_payload['repository']['id'])
    except (KeyError, AttributeError, IndexError, TypeError):
        stats_client.set(f'{stats_prefix}.repo_ids', 'No id')
    try:
        stats_client.set(f'{stats_prefix}.owner_ids', queued_json_payload['repository']['owner']['id'])
    except (KeyError, AttributeError, IndexError, TypeError):
        stats_client.set(f'{stats_prefix}.owner_ids', 'No id')
    try:
        stats_client.set(f'{stats_prefix}.pusher_ids', queued_json_payload['pusher']['id'])
    except (KeyError, AttributeError, IndexError, TypeError):
        stats_client.set(f'{stats_prefix}.pusher_ids', 'No id')


    # Setup a temp folder to use
    source_url_base = f'https://s3-{GlobalSettings.aws_region_name}.amazonaws.com/{GlobalSettings.pre_convert_bucket_name}'
    # Move everything down one directory level for simple delete
    # NOTE: The base_temp_dir_name needs to be unique if we ever want multiple workers
    # TODO: This might not be enough 6-digit fractions of a second could collide???
    intermediate_dir_name = OUR_NAME + datetime.utcnow().strftime("_%Y-%m-%d_%H:%M:%S.%f")
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
    repo_owner_username = queued_json_payload['repository']['owner']['username']
    repo_name = queued_json_payload['repository']['name']
    compare_url = queued_json_payload['compare_url']
    commit_message = commit['message'].strip() # Seems to always end with a newline

    if 'pusher' in queued_json_payload:
        pusher = queued_json_payload['pusher']
    else:
        pusher = {'username': commit['author']['username']}
    pusher_username = pusher['username']

    our_identifier = f"'{pusher_username}' pushing '{repo_owner_username}/{repo_name}'"
    GlobalSettings.logger.info(f"Processing job for {our_identifier} for \"{commit_message}\"")
    # Seems that statsd 3.3.0 can only handle ASCII chars (not full Unicode)
    ascii_repo_owner_username_bytes = repo_owner_username.encode('ascii', 'replace') # Replaces non-ASCII chars with '?'
    adjusted_repo_owner_username = ascii_repo_owner_username_bytes.decode('utf-8') # Recode as a str
    ascii_repo_name_bytes = repo_name.encode('ascii', 'replace') # Replaces non-ASCII chars with '?'
    adjusted_repo_name = ascii_repo_name_bytes.decode('utf-8') # Recode as a str
    stats_client.incr(f'{stats_prefix}.users.invoked.{adjusted_repo_owner_username}')
    # Using a hyphen as separator as forward slash gets changed to hyphen anyway
    user_projects_invoked_string = f'{general_stats_prefix}.user-projects.invoked.{adjusted_repo_owner_username}--{adjusted_repo_name }'
    # stats_client.gauge(user_projects_invoked_string, -1) # Mark as 'in-process'


    # Here's our programmed failure (for remotely testing failures)
    if pusher_username=='Failure' and 'full_name' in pusher and pusher['full_name']=='Push Test':
        deliberateFailureForTesting


    # Download and unzip the repo files
    repo_dir = get_repo_files(base_temp_dir_name, commit_url, repo_name)


    # Get the resource container
    # GlobalSettings.logger.debug(f'Getting Resource Container…')
    rc = RC(repo_dir, repo_name)
    job_descriptive_name = f'{our_identifier} {rc.resource.type}({rc.resource.format}, {rc.resource.file_ext})'


    # Use the subject to set the resource type more intelligently
    GlobalSettings.logger.debug(f"rc.resource.identifier={rc.resource.identifier}")
    GlobalSettings.logger.debug(f"rc.resource.file_ext={rc.resource.file_ext}")
    GlobalSettings.logger.debug(f"rc.resource.type={rc.resource.type}")
    GlobalSettings.logger.debug(f"rc.resource.subject={rc.resource.subject}")
    adjusted_subject = rc.resource.subject.replace(' ', '_') # NOTE: RC returns 'title' if 'subject' is missing
    resource_type = None
    if adjusted_subject in ('Bible', 'Aligned_Bible', 'Greek_New_Testament', 'Hebrew_Old_Testament',
                'Translation_Academy', 'Translation_Notes', 'Translation_Questions', 'Translation_Words',
                'Open_Bible_Stories', 'OBS_Translation_Notes', 'OBS_Translation_Questions',
                ): # from https://api.door43.org/v3/subjects
        resource_type = adjusted_subject
    if not resource_type: resource_type = rc.resource.identifier # e.g., tq, tn, ta
    if not resource_type: resource_type = rc.resource.type # e.g., help, man
    input_format = rc.resource.file_ext
    GlobalSettings.logger.info(f"Got resource_type={resource_type}, input_format={input_format}.")


    # Save manifest to manifest table
    # GlobalSettings.logger.debug(f'Creating manifest dictionary…')
    manifest_data = {
        'repo_name': repo_name,
        'user_name': repo_owner_username,
        'lang_code': rc.resource.language.identifier,
        'resource_id': rc.resource.identifier,
        'resource_type': resource_type, # This used to be rc.resource.type
        'title': rc.resource.title,
        'manifest': json.dumps(rc.as_dict()),
        'last_updated': datetime.utcnow()
    }
    # First see if manifest already exists in DB (can be slowish) and update it if it is
    GlobalSettings.logger.debug(f"Getting manifest from DB for '{repo_name}' with user '{repo_owner_username}' …")
    tx_manifest = TxManifest.get(repo_name=repo_name, user_name=repo_owner_username)
    if tx_manifest:
        for key, value in manifest_data.items():
            setattr(tx_manifest, key, value)
        GlobalSettings.logger.debug(f"Updating manifest in manifest table: {manifest_data}")
        tx_manifest.update()
    else:
        tx_manifest = TxManifest(**manifest_data)
        GlobalSettings.logger.debug(f"Inserting manifest into manifest table: {tx_manifest}")
        tx_manifest.insert()


    # Preprocess the files
    GlobalSettings.logger.info("Preprocessing files…")
    preprocess_dir = tempfile.mkdtemp(dir=base_temp_dir_name, prefix='preprocess_')
    preprocessor_result = do_preprocess(rc, repo_dir, preprocess_dir)
    # preprocess_result is normally True, but can be a warning dict for the Bible preprocessor
    preprocessor_warning_list = preprocessor_result if isinstance(preprocessor_result, list) else None
    if preprocessor_warning_list:
        GlobalSettings.logger.debug(f"Preprocessor warning list is {preprocessor_warning_list}")


    # Zip up the massaged files
    GlobalSettings.logger.info("Zipping preprocessed files…")
    preprocessed_zip_file = tempfile.NamedTemporaryFile(dir=base_temp_dir_name, prefix='preprocessed_', suffix='.zip', delete=False)
    GlobalSettings.logger.debug(f'Zipping files from {preprocess_dir} to {preprocessed_zip_file.name} …')
    add_contents_to_zip(preprocessed_zip_file.name, preprocess_dir)
    GlobalSettings.logger.debug('Zipping finished.')


    # Upload zipped file to the S3 pre-convert bucket
    GlobalSettings.logger.info("Uploading zip file to S3 pre-convert bucket…")
    our_job_id = get_unique_job_id()
    file_key = upload_zip_file(our_job_id, preprocessed_zip_file.name)


    # We no longer use txJob class but just create our own Python dict
    GlobalSettings.logger.debug("Webhook.process_job setting up job dict…")
    pj_job_dict = {}
    pj_job_dict['job_id'] = our_job_id
    pj_job_dict['identifier'] = our_identifier # So we can recognise this job inside tX Job Handler
    pj_job_dict['user_name'] = repo_owner_username
    pj_job_dict['repo_name'] = repo_name
    pj_job_dict['commit_id'] = commit_id
    pj_job_dict['manifests_id'] = tx_manifest.id
    pj_job_dict['created_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    # Seems never used (RJH)
    #pj_job_dict['user = user.username  # Username of the token, not necessarily the repo's owner
    pj_job_dict['resource_type'] = resource_type # This used to be rc.resource.identifier
    pj_job_dict['input_format'] = input_format
    pj_job_dict['source'] = source_url_base + '/' + file_key
    pj_job_dict['cdn_bucket'] = GlobalSettings.cdn_bucket_name
    pj_job_dict['cdn_file'] = f"tx/job/{our_job_id}.zip"
    pj_job_dict['output'] = f"https://{GlobalSettings.cdn_bucket_name}/{pj_job_dict['cdn_file']}"
    pj_job_dict['callback'] = GlobalSettings.api_url + '/client/callback'
    pj_job_dict['output_format'] = 'html'
    pj_job_dict['user_projects_invoked_string'] = user_projects_invoked_string # Need to save this for reuse
    pj_job_dict['links'] = {
        'href': f"{GlobalSettings.api_url}/tx/job/{our_job_id}",
        'rel': 'self',
        'method': 'GET'
    }
    if preprocessor_warning_list:
        pj_job_dict['preprocessor_warnings'] = preprocessor_warning_list
    pj_job_dict['status'] = None
    pj_job_dict['success'] = False


    # Save the job info in Redis for the callback to use
    remember_job(pj_job_dict, redis_connection)

    # Get S3 cdn bucket/dir and empty it
    s3_commit_key = f"u/{pj_job_dict['user_name']}/{pj_job_dict['repo_name']}/{pj_job_dict['commit_id']}"
    clear_commit_directory_in_cdn(s3_commit_key)

    # Create a build log
    build_log_json = create_build_log(commit_id, commit_message, commit_url, compare_url, pj_job_dict,
                                      pusher_username, repo_name, repo_owner_username)
    # Upload an initial build_log
    upload_build_log_to_s3(base_temp_dir_name, build_log_json, s3_commit_key)

    # Update the project.json file
    update_project_json(base_temp_dir_name, commit_id, pj_job_dict, repo_name, repo_owner_username)



    # Pass the work request onto the tX system
    GlobalSettings.logger.info(f"POST request to tX system @ {tx_post_url} …")
    tx_payload = {
        'job_id': our_job_id,
        'identifier': our_identifier, # So we can recognise this job inside tX Job Handler
        'resource_type': resource_type, # This used to be rc.resource.identifier
        'input_format': input_format,
        'output_format': 'html',
        'source': source_url_base + '/' + file_key,
        'callback': 'http://127.0.0.1:8080/tx-callback/' \
                        if prefix and debug_mode_flag and ':8090' in tx_post_url \
                    else DOOR43_CALLBACK_URL,
        'user_token': gogs_user_token, # Checked by tX enqueue job
        'door43_webhook_received_at': queued_json_payload['door43_webhook_received_at'],
        }
    if 'options' in pj_job_dict and pj_job_dict['options']:
        GlobalSettings.logger.info(f"Have convert job options: {pj_job_dict['options']}!")
        tx_payload['options'] = pj_job_dict['options']

    GlobalSettings.logger.debug(f"Payload for tX: {tx_payload}")
    try:
        response = requests.post(tx_post_url, json=tx_payload)
    except requests.exceptions.ConnectionError as e:
        GlobalSettings.logger.critical(f"Callback connection error: {e}")
        response = None

    if response:
        #GlobalSettings.logger.info(f"response.status_code = {response.status_code}, response.reason = {response.reason}")
        #GlobalSettings.logger.debug(f"response.headers = {response.headers}")
        try:
            GlobalSettings.logger.info(f"response.json = {response.json()}")
        except json.decoder.JSONDecodeError:
            GlobalSettings.logger.info("No valid response JSON found")
            GlobalSettings.logger.debug(f"response.text = {response.text}")
        if response.status_code != 200:
            GlobalSettings.logger.critical(f"Failed to submit job to tX:"
                                           f" {response.status_code}={response.reason}")
    else: # no response
        error_msg = "Submission of job to tX system got no response"
        GlobalSettings.logger.critical(error_msg)
        raise Exception(error_msg) # So we go into the FAILED queue and monitoring system


    # if rc.resource.file_ext in ('usfm', 'usfm3'): # Upload source files to BDB
    #     if prefix and not debug_mode_flag: # Only for dev- chain
    #         GlobalSettings.logger.info(f"Submitting {job_descriptive_name} originals to BDB…")
    #         original_zip_filepath = os.path.join(base_temp_dir_name, commit_url.rpartition(os.path.sep)[2] + '.zip')
    #         upload_to_BDB(f"{repo_owner_username}__{repo_name}__({pusher_username})", original_zip_filepath)
    #         # Not using the preprocessed files (only the originals above)
    #         # GlobalSettings.logger.info(f"Submitting {job_descriptive_name} preprocessed to BDB…")
    #         # upload_to_BDB(f"{repo_owner_username}__{repo_name}__({pusher_username})", preprocessed_zip_file.name)


    if prefix and debug_mode_flag:
        GlobalSettings.logger.debug(f"Temp folder '{base_temp_dir_name}' has been left on disk for debugging!")
    else:
        remove_tree(base_temp_dir_name)  # cleanup
    GlobalSettings.logger.info(f"{prefixed_our_name} process_job() for {job_descriptive_name} is finishing with {build_log_json}")
    return job_descriptive_name
#end of process_job function


def job(queued_json_payload):
    """
    This function is called by the rq package to process a job in the queue(s).

    The job is removed from the queue before the job is started,
        but if the job throws an exception or times out (timeout specified in enqueue process)
            then the job gets added to the 'failed' queue.
    """
    GlobalSettings.logger.info(f"{OUR_NAME} received a job" + (" (in debug mode)" if debug_mode_flag else ""))
    start_time = time()
    stats_client.incr(f'{stats_prefix}.jobs.attempted')

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
    try:
        job_descriptive_name = process_job(queued_json_payload, current_job.connection)
    except Exception as e:
        # Catch most exceptions here so we can log them to CloudWatch
        GlobalSettings.logger.critical(f"{prefixed_our_name} threw an exception while processing: {queued_json_payload}")
        GlobalSettings.logger.critical(f"{e}: {traceback.format_exc()}")
        GlobalSettings.close_logger() # Ensure queued logs are uploaded to AWS CloudWatch
        # Now attempt to log it to an additional, separate FAILED log
        import logging
        from boto3 import Session
        from watchtower import CloudWatchLogHandler
        logger2 = logging.getLogger(prefixed_our_name)
        test_mode_flag = os.getenv('TEST_MODE', '')
        travis_flag = os.getenv('TRAVIS_BRANCH', '')
        log_group_name = f"FAILED_{'' if test_mode_flag or travis_flag else prefix}tX" \
                         f"{'_DEBUG' if debug_mode_flag else ''}" \
                         f"{'_TEST' if test_mode_flag else ''}" \
                         f"{'_TravisCI' if travis_flag else ''}"
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        boto3_session = Session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                            region_name='us-west-2')
        watchtower_log_handler = CloudWatchLogHandler(boto3_session=boto3_session,
                                                    use_queues=False,
                                                    log_group=log_group_name,
                                                    stream_name=prefixed_our_name)
        logger2.addHandler(watchtower_log_handler)
        logger2.setLevel(logging.DEBUG)
        logger2.info(f"Logging to AWS CloudWatch group '{log_group_name}' using key '…{aws_access_key_id[-2:]}'.")
        logger2.critical(f"{prefixed_our_name} threw an exception while processing: {queued_json_payload}")
        logger2.critical(f"{e}: {traceback.format_exc()}")
        watchtower_log_handler.close()
        stats_client.gauge(user_projects_invoked_string, 1) # Mark as 'failed'
        raise e # We raise the exception again so it goes into the failed queue

    elapsed_milliseconds = round((time() - start_time) * 1000)
    stats_client.timing(f'{stats_prefix}.job.duration', elapsed_milliseconds)
    if elapsed_milliseconds < 2000:
        GlobalSettings.logger.info(f"{prefixed_our_name} webhook job handling for {job_descriptive_name} completed in {elapsed_milliseconds:,} milliseconds.")
    else:
        GlobalSettings.logger.info(f"{prefixed_our_name} webhook job handling for {job_descriptive_name} completed in {round(time() - start_time)} seconds.")

    stats_client.incr(f'{stats_prefix}.jobs.completed')
    GlobalSettings.close_logger() # Ensure queued logs are uploaded to AWS CloudWatch
# end of job function

# end of webhook.py for door43_enqueue_job
