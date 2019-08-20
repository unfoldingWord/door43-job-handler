# NOTE: This module name and function name are defined by the rq package and our own door43-enqueue-job package
# This code adapted by RJH Sept 2018 from webhook.py

# NOTE: rq_settings.py is executed at program start-up, reads some environment variables, and sets queue name, etc.
#       job() function (at bottom here) is executed by rq package when there is an available entry in the named queue.

# Python imports
import os
from datetime import datetime
import time
import json
from ast import literal_eval
import tempfile
import traceback
from typing import Union, List, Optional

# Library (PyPi) imports
from rq import get_current_job
from statsd import StatsClient # Graphite front-end

# Local imports
from rq_settings import prefix, debug_mode_flag, REDIS_JOB_LIST
from app_settings.app_settings import AppSettings
from client_converter_callback import ClientConverterCallback
from client_linter_callback import ClientLinterCallback
from door43_tools.project_deployer import ProjectDeployer
from general_tools.file_utils import write_file, remove_tree



AppSettings(prefix=prefix)
if prefix not in ('', 'dev-'):
    AppSettings.logger.critical(f"Unexpected prefix: '{prefix}' -- expected '' or 'dev-'")
general_stats_prefix = f"door43.{'dev' if prefix else 'prod'}.job-handler" # Can't add .callback here coz we also have .total


# Get the Graphite URL from the environment, otherwise use a local test instance
graphite_url = os.getenv('GRAPHITE_HOSTNAME', 'localhost')
stats_client = StatsClient(host=graphite_url, port=8125)



def verify_expected_job(vej_job_id:str, vej_redis_connection) -> Union[dict, bool]:
    """
    Check that we have this outstanding job in a REDIS dict
        and delete it once we make a match.

    Return the job dict or False
    """
    # vej_job_id = vej_job_dict['job_id']
    # AppSettings.logger.debug(f"verify_expected_job({vej_job_id})")

    outstanding_jobs_dict_bytes = vej_redis_connection.get(REDIS_JOB_LIST) # Gets bytes!!!
    if not outstanding_jobs_dict_bytes:
        AppSettings.logger.error("No expected jobs found in redis store")
        return False
    # AppSettings.logger.debug(f"Got outstanding_jobs_dict_bytes:"
    #                             f" ({len(outstanding_jobs_dict_bytes)}) {outstanding_jobs_dict_bytes}")
    assert isinstance(outstanding_jobs_dict_bytes,bytes)
    outstanding_jobs_dict_json_string = outstanding_jobs_dict_bytes.decode() # bytes -> str
    assert isinstance(outstanding_jobs_dict_json_string,str)
    outstanding_jobs_dict = json.loads(outstanding_jobs_dict_json_string)
    assert isinstance(outstanding_jobs_dict,dict)
    AppSettings.logger.info(f"Currently have {len(outstanding_jobs_dict)}"
                               f" outstanding job(s) in '{REDIS_JOB_LIST}' redis store")
    if vej_job_id not in outstanding_jobs_dict:
        AppSettings.logger.error(f"Not expecting job with id of {vej_job_id}")
        AppSettings.logger.debug(f"Only had job ids: {outstanding_jobs_dict.keys()}")
        return False
    this_job_dict = outstanding_jobs_dict[vej_job_id]

    # We found a match -- delete that job from the outstanding list
    AppSettings.logger.debug(f"Found job match for {vej_job_id}")
    del outstanding_jobs_dict[vej_job_id]
    if outstanding_jobs_dict:
        AppSettings.logger.debug(f"Still have {len(outstanding_jobs_dict)}"
                                    f" outstanding job(s) in '{REDIS_JOB_LIST}'")
        # Update the job dict in redis now that this job has been deleted from it
        outstanding_jobs_json_string = json.dumps(outstanding_jobs_dict)
        vej_redis_connection.set(REDIS_JOB_LIST, outstanding_jobs_json_string)
    else: # no outstanding jobs left
        AppSettings.logger.info("Deleting the final outstanding job"
                                  f" in '{REDIS_JOB_LIST}' redis store")
        del_result = vej_redis_connection.delete(REDIS_JOB_LIST)
        # print("  Got redis delete result:", del_result)
        assert del_result == 1 # Should only have deleted one key

    #AppSettings.logger.debug(f"Returning {this_job_dict}")
    return this_job_dict
# end of verify_expected_job


def merge_results_logs(build_log:dict, file_results:dict, converter_flag:bool) -> dict:
    """
    Given a second partial build log file_results,
        combine the log/warnings/errors lists into the first build_log.
    """
    AppSettings.logger.debug(f"Callback.merge_results_logs(…, {file_results}, {converter_flag})…")
    if not build_log:
        return file_results
    if file_results:
        merge_dicts_lists(build_log, file_results, 'message')
        merge_dicts_lists(build_log, file_results, 'log')
        merge_dicts_lists(build_log, file_results, 'warnings')
        merge_dicts_lists(build_log, file_results, 'errors')
        if converter_flag \
        and ('success' in file_results) \
        and (file_results['success'] is False):
            build_log['success'] = file_results['success']
    return build_log
# end of merge_results_logs function


def merge_dicts_lists(build_log:dict, file_results:dict, key:str) -> None:
    """
    Used for merging log dicts from various sub-processes.

    build_log is a dict
    file_results is a dict
    value is a key (string) for the lists that will be merged if in both dicts

    Alters first parameter build_log in place.
    """
    # AppSettings.logger.debug(f"Callback.merge_dicts({build_log}, {file_results}, '{key}')…")
    if key in file_results:
        value = file_results[key]
        if value:
            if (key in build_log) and (build_log[key]):
                build_log[key] += value
            else:
                build_log[key] = value
# end of merge_dicts_lists function


def get_jobID_from_commit_buildLog(project_folder_key:str, commit_id:str) -> Optional[str]:
    """
    Look for build_log.json in the Door43 bucket
        and extract the job_id from it.

    NOTE: It seems like old builds also put build_log.json in the CDN bucket
            but the new ones don't seem to have that.

    Return None if anything fails.
    """
    file_key = f'{project_folder_key}{commit_id}/build_log.json'
    try:
        file_content = AppSettings.door43_s3_handler() \
                    .resource.Object(bucket_name=AppSettings.door43_bucket_name, key=file_key) \
                    .get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        return json_content['job_id']
    except Exception as e:
        AppSettings.logger.critical(f"get_jobID_from_commit_buildLog threw an exception while getting {prefix}D43 '{file_key}': {e}")
# end of get_jobID_from_commit_buildLog function


def clear_commit_directory_from_bucket(s3_bucket_handler, s3_commit_key:str) -> None:
    """
    Clear out and remove the commit directory from the requested bucket for this project revision.
    """
    AppSettings.logger.debug(f"Clearing objects from commit directory '{s3_commit_key}' in {s3_bucket_handler.bucket_name} bucket…")
    s3_bucket_handler.bucket.objects.filter(Prefix=s3_commit_key).delete()
# end of clear_commit_directory_from_bucket function


def remove_excess_commits(commits_list:list, project_folder_key:str) -> List[dict]:
    """
    Given a list of commits (oldest first),
        remove the unnecessary ones from the list
        and DELETE THE files from S3!

    Written: Aug 2019
        This was especially important as we moved from hash numbers
            to tag and branch names.
    """
    MAX_WANTED_COMMITS = 3
    AppSettings.logger.debug(f"remove_excess_commits({len(commits_list)}={commits_list}, {project_folder_key})…")
    new_commits = []
    # Process it backwards in case we want to count how many we have as we go
    for commit in reversed(commits_list):
        AppSettings.logger.debug(f"  Investigating {commit['type']} '{commit['id']}' commit (already have {len(new_commits)} -- want max of {MAX_WANTED_COMMITS})")
        if len(new_commits) >= MAX_WANTED_COMMITS \
        and commit['type'] in ('hash', 'unknown'):
            if 0: # really do it DISABLED DISABLED DISABLED DISABLED DISABLED
                # Delete the commit hash folders from both CDN and D43 buckets
                commit_key = f"{project_folder_key}{commit['id']}"
                AppSettings.logger.info(f"    Removing {prefix}CDN '{commit['type']}' '{commit['id']}' commit! …")
                clear_commit_directory_from_bucket(AppSettings.cdn_s3_handler(), commit_key)
                AppSettings.logger.info(f"    Removing {prefix}D43 '{commit['type']}' '{commit['id']}' commit! …")
                clear_commit_directory_from_bucket(AppSettings.door43_s3_handler(), commit_key)
                # Delete the pre-convert .zip file (available on Download button) from its bucket
                if commit['job_id']:
                    zipFile_key = f"preconvert/{commit['job_id']}.zip"
                    AppSettings.logger.info(f"    Removing {prefix}PreConvert '{commit['type']}' '{zipFile_key}' file! …")
                    clear_commit_directory_from_bucket(AppSettings.pre_convert_s3_handler(), zipFile_key)
                else: # don't know the job_id (or the zip file was already deleted)
                    AppSettings.logger.warning("  No job_id so pre-convert zip file not deleted.")
                # Setup redirects (so users don't get 404 errors from old saved links)
                old_repo_key = f"{project_folder_key}{commit['id']}"
                latest_repo_key = f"/{project_folder_key}{new_commits[-1]['id']}" # Must start with /
                AppSettings.logger.info(f"    Redirecting {old_repo_key} and {old_repo_key}/index.html to {latest_repo_key} …")
                AppSettings.door43_s3_handler().redirect(key=old_repo_key, location=latest_repo_key)
                AppSettings.door43_s3_handler().redirect(key=f'{old_repo_key}/index.html', location=latest_repo_key)
            else:
                AppSettings.logger.warning(f"    CURRENTLY DISABLED Need to remove '{commit['type']}' '{commit['id']}' commit (and files) but CURRENTLY DISABLED…")
                new_commits.insert(0, commit) # Insert at beginning to get the order correct again
        else:
            AppSettings.logger.debug("    Keeping this one.")
            new_commits.insert(0, commit) # Insert at beginning to get the order correct again
    return new_commits
# end of remove_excess_commits


def update_project_file(build_log:dict, output_dirpath:str) -> None:
    """
    project.json is read by the Javascript in door43.org/js/project-page-functions.js
        The commits are used to update the Revision list in the left side-bar.
    """
    AppSettings.logger.debug(f"Callback.update_project_file({build_log}, output_dir={output_dirpath})…")

    commit_id = build_log['commit_id']
    repo_owner_username = build_log['repo_owner_username'] # was 'repo_owner'
    repo_name = build_log['repo_name']
    project_folder_key = f'u/{repo_owner_username}/{repo_name}/'
    project_json_key = f'{project_folder_key}project.json'
    project_json = AppSettings.cdn_s3_handler().get_json(project_json_key)
    project_json['user'] = repo_owner_username
    project_json['repo'] = repo_name
    project_json['repo_url'] = f'https://{AppSettings.gogs_url}/{repo_owner_username}/{repo_name}'
    current_commit = {
        'id': commit_id,
        'job_id': build_log['job_id'],
        'type': build_log['commit_type'],
        'created_at': build_log['created_at'],
        'status': build_log['status'],
        'success': build_log['success'],
        # 'started_at': None,
        # 'ended_at': None
    }
    # if 'started_at' in build_log:
    #     current_commit['started_at'] = build_log['started_at']
    # if 'ended_at' in build_log:
    #     current_commit['ended_at'] = build_log['ended_at']

    def is_hash(commit_str:str) -> bool:
        """
        Checks to see if this looks like a hexadecimal (abbreviated) hash
        """
        if len(commit_str) != 10: return False
        for char in commit_str:
            if char not in 'abcdef1234567890': return False
        return True

    AppSettings.logger.debug("Rebuilding commits list for project.json…")
    if 'commits' not in project_json:
        project_json['commits'] = []
    commits = []
    for c in project_json['commits']:
        AppSettings.logger.debug(f"  Looking at {len(commits)}/ '{c['id']}' {c['id'] == commit_id}…")
        if c['id'] == commit_id: # the old entry for the current commit id
            zip_file_key = f"preconvert/{current_commit['job_id']}.zip"
            AppSettings.logger.info(f"  Removing obsolete {prefix}pre-convert '{current_commit['type']}' '{commit_id}' {zip_file_key} …")
            try:
                clear_commit_directory_from_bucket(AppSettings.pre_convert_s3_handler(), zip_file_key)
            except Exception as e:
                AppSettings.logger.critical(f"  Remove obsolete pre-convert zipfile threw an exception while attempted to delete '{zip_file_key}': {e}")
            # Not appended to commits here coz it happens below instead
        else: # a different commit from the current one
            if 'job_id' not in c: # Might be able to remove this eventually
                c['job_id'] = get_jobID_from_commit_buildLog(project_folder_key, c['id'])
                # Returned job id might have been None
            if 'type' not in c: # Might be able to remove this eventually
                c['type'] = 'hash' if is_hash(c['id']) else 'unknown'
            commits.append(c)
    commits.append(current_commit)
    cleaned_commits = remove_excess_commits(commits, project_folder_key)
    if len(cleaned_commits) < len(commits): # Then we removed some
        # Save a dated (coz this could happen more than once) backup of the project.json file
        save_project_filename = f"project.save.{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.json"
        save_project_filepath = os.path.join(output_dirpath, save_project_filename)
        write_file(save_project_filepath, project_json)
        save_project_json_key = f'{project_folder_key}{save_project_filename}'
        AppSettings.cdn_s3_handler().upload_file(save_project_filepath, save_project_json_key, cache_time=0)
        AppSettings.door43_s3_handler().upload_file(save_project_filepath, save_project_json_key, cache_time=0)
    # Now save the updated project.json file
    project_json['commits'] = cleaned_commits
    project_filepath = os.path.join(output_dirpath, 'project.json')
    write_file(project_filepath, project_json)
    AppSettings.cdn_s3_handler().upload_file(project_filepath, project_json_key, cache_time=0)
# end of update_project_file function


# user_projects_invoked_string = 'user-projects.invoked.unknown--unknown'
project_types_invoked_string = f'{general_stats_prefix}.types.invoked.unknown'
def process_callback_job(pc_prefix, queued_json_payload, redis_connection):
    """
    The job info is retrieved from REDIS and matched/checked
    The converted file(s) are downloaded
    Templating is done
    The results are uploaded to the S3 CDN bucket
    The final log is uploaded to the S3 CDN bucket
    The pages are then deployed.

    The given payload will be appended to the 'failed' queue
        if an exception is thrown in this module.
    """
    # global user_projects_invoked_string
    global project_types_invoked_string
    str_payload = str(queued_json_payload)
    str_payload_adjusted = str_payload if len(str_payload)<1500 \
                            else f'{str_payload[:1000]} …… {str_payload[-500:]}'
    AppSettings.logger.debug(f"Processing {pc_prefix+' ' if pc_prefix else ''}callback: {str_payload_adjusted}")

    # Check that this is an expected callback job
    if 'job_id' not in queued_json_payload:
        error = "Callback job has no 'job_id' field"
        AppSettings.logger.critical(error)
        raise Exception(error)
    # job_id = queued_json_payload['job_id']
    matched_job_dict = verify_expected_job(queued_json_payload['job_id'], redis_connection)
    # NOTE: The above deletes the matched job entry,
    #   so this means callback cannot be successfully retried if it fails below
    if not matched_job_dict:
        error = f"No waiting job found for {queued_json_payload}"
        AppSettings.logger.critical(error)
        raise Exception(error)
    AppSettings.logger.debug(f"Got matched_job_dict: {matched_job_dict}")
    job_descriptive_name = f"{matched_job_dict['resource_type']}({matched_job_dict['input_format']})"

    this_job_dict = queued_json_payload.copy()
    # Get needed fields that we saved but didn't submit to or receive back from tX
    for fieldname in ('repo_owner_username', 'repo_name', 'commit_id', 'input_format', 'door43_webhook_received_at'):
        if prefix and debug_mode_flag: assert fieldname not in this_job_dict
        this_job_dict[fieldname] = matched_job_dict[fieldname]
    # Remove unneeded fields that we saved or received back from tX
    for fieldname in ('callback',):
        if fieldname in this_job_dict:
            del this_job_dict[fieldname]

    if 'preprocessor_warnings' in matched_job_dict:
        # AppSettings.logger.debug(f"Got {len(matched_job_dict['preprocessor_warnings'])}"
        #                            f" remembered preprocessor_warnings: {matched_job_dict['preprocessor_warnings']}")
        # Prepend preprocessor results to linter warnings
        # total_warnings = len(matched_job_dict['preprocessor_warnings']) + len(queued_json_payload['linter_warnings'])
        queued_json_payload['linter_warnings'] = matched_job_dict['preprocessor_warnings'] \
                                               + queued_json_payload['linter_warnings']
        # AppSettings.logger.debug(f"Now have {len(queued_json_payload['linter_warnings'])}"
        #                             f" linter_warnings: {queued_json_payload['linter_warnings']}")
        # assert len(queued_json_payload['linter_warnings']) == total_warnings
        del matched_job_dict['preprocessor_warnings'] # No longer required

    if 'identifier' in queued_json_payload:
        # NOTE: The identifier we send to the tX Job Handler system is a complex string, not a job_id
        identifier = queued_json_payload['identifier']
        AppSettings.logger.debug(f"Got identifier from queued_json_payload: {identifier}.")
        job_descriptive_name = f'{identifier} {job_descriptive_name}'
    if 'identifier' in matched_job_dict: # overrides
        identifier = matched_job_dict['identifier']
        AppSettings.logger.debug(f"Got identifier from matched_job_dict: {identifier}.")
    else:
        identifier = queued_json_payload['job_id']
        AppSettings.logger.debug(f"Got identifier from job_id: {identifier}.")
    # NOTE: following line removed as stats recording used too much disk space
    # user_projects_invoked_string = matched_job_dict['user_projects_invoked_string'] \
    #                     if 'user_projects_invoked_string' in matched_job_dict \
    #                     else f'??{identifier}??'
    project_types_invoked_string = f"{general_stats_prefix}.types.invoked.{matched_job_dict['resource_type']}"

    # NOTE: matched_job_dict gets merged into build_log below
    matched_job_dict['log'] = []
    matched_job_dict['warnings'] = []
    matched_job_dict['errors'] = []

    our_temp_dir = tempfile.mkdtemp(suffix='',
                     prefix='Door43_callback_' + datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S_'))

    # We get the adapted tx-manager calls to do our work for us
    # It doesn't actually matter which one we do first I think
    AppSettings.logger.info("Running linter post-processing…")
    url_part2 = f"u/{this_job_dict['repo_owner_username']}/{this_job_dict['repo_name']}/{this_job_dict['commit_id']}"
    clc = ClientLinterCallback(this_job_dict, identifier,
                               queued_json_payload['linter_success'],
                               queued_json_payload['linter_info'] if 'linter_info' in queued_json_payload else None,
                               queued_json_payload['linter_warnings'],
                               queued_json_payload['linter_errors'] if 'linter_errors' in queued_json_payload else None,
                                )
                            #    s3_results_key=url_part2)
    linter_log = clc.do_post_processing()
    build_log = merge_results_logs(matched_job_dict, linter_log, converter_flag=False)
    AppSettings.logger.info("Running converter post-processing…")
    ccc = ClientConverterCallback(this_job_dict, identifier,
                                  queued_json_payload['converter_success'],
                                  queued_json_payload['converter_info'],
                                  queued_json_payload['converter_warnings'],
                                  queued_json_payload['converter_errors'],
                                  our_temp_dir)
    unzip_dir, converter_log = ccc.do_post_processing()
    final_build_log = merge_results_logs(build_log, converter_log, converter_flag=True)

    if final_build_log['errors']:
        final_build_log['status'] = 'errors'
    elif final_build_log['warnings']:
        final_build_log['status'] = 'warnings'
    else:
        final_build_log['status'] = 'success'
    final_build_log['success'] = queued_json_payload['converter_success']
    final_build_log['ended_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    update_project_file(final_build_log, our_temp_dir)
    # NOTE: The following is disabled coz it's done (again) later by the deployer
    # upload_build_log(final_build_log, 'build_log.json', output_dir, url_part2, cache_time=600)

    if unzip_dir is None:
        AppSettings.logger.critical("Unable to deploy because file download failed previously")
        deployed = False
    else:
        # Now deploy the new pages (was previously a separate AWS Lambda call)
        AppSettings.logger.info(f"Deploying to the website (convert status='{final_build_log['status']}')…")
        deployer = ProjectDeployer(unzip_dir, our_temp_dir)
        deployer.deploy_revision_to_door43(final_build_log) # Does templating and uploading
        deployed = True

    if prefix and debug_mode_flag:
        AppSettings.logger.debug(f"Temp folder '{our_temp_dir}' has been left on disk for debugging!")
    else:
        remove_tree(our_temp_dir)

    # Finishing off
    str_final_build_log = str(final_build_log)
    str_final_build_log_adjusted = str_final_build_log if len(str_final_build_log)<1500 \
                            else f'{str_final_build_log[:1000]} …… {str_final_build_log[-500:]}'
    AppSettings.logger.info(f"Door43-Job-Handler process_callback_job() for {job_descriptive_name} is finishing with {str_final_build_log_adjusted}")
    if 'echoed_from_production' in matched_job_dict and matched_job_dict['echoed_from_production']:
        AppSettings.logger.info("This job was ECHOED FROM PRODUCTION (for dev- chain testing)!")
        AppSettings.logger.info("  (Use https://git.door43.org/tx-manager-test-data/echo_prodn_to_dev_off/settings/hooks/44079 to turn it off.)")
    if deployed:
        AppSettings.logger.info(f"{'Should become available' if final_build_log['success'] is True or final_build_log['success']=='True' or final_build_log['status'] in ('success', 'warnings') else 'Would be'}"
                               f" at https://{AppSettings.door43_bucket_name.replace('dev-door43','dev.door43')}/{url_part2}/")
    return job_descriptive_name, matched_job_dict['door43_webhook_received_at']
#end of process_callback_job function



def job(queued_json_payload) -> None:
    """
    This function is called by the rq package to process a job in the queue(s).

    The job is removed from the queue before the job is started,
        but if the job throws an exception or times out (timeout specified in enqueue process)
            then the job gets added to the 'failed' queue.
    """
    AppSettings.logger.info("Door43-Job-Handler received a callback" + (" (in debug mode)" if debug_mode_flag else ""))
    start_time = time.time()
    stats_client.incr(f'{general_stats_prefix}.callback.jobs.attempted')

    current_job = get_current_job()
    #print(f"Current job: {current_job}") # Mostly just displays the job number and payload
    #print("id",current_job.id) # Displays job number
    #print("origin",current_job.origin) # Displays queue name
    #print("meta",current_job.meta) # Empty dict

    #print(f"Got a job from {current_job.origin} queue: {queued_json_payload}")
    #print(f"\nGot job {current_job.id} from {current_job.origin} queue")
    #queue_prefix = 'dev-' if current_job.origin.startswith('dev-') else ''
    #assert queue_prefix == prefix
    try:
        job_descriptive_name, door43_webhook_received_at = \
                process_callback_job(prefix, queued_json_payload, current_job.connection)
    except Exception as e:
        # Catch most exceptions here so we can log them to CloudWatch
        prefixed_name = f"{prefix}Door43_Callback"
        AppSettings.logger.critical(f"{prefixed_name} threw an exception while processing: {queued_json_payload}")
        AppSettings.logger.critical(f"{e}: {traceback.format_exc()}")
        AppSettings.close_logger() # Ensure queued logs are uploaded to AWS CloudWatch
        # Now attempt to log it to an additional, separate FAILED log
        import logging
        from boto3 import Session
        from watchtower import CloudWatchLogHandler
        logger2 = logging.getLogger(prefixed_name)
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
                                                    stream_name=prefixed_name)
        logger2.addHandler(watchtower_log_handler)
        logger2.setLevel(logging.DEBUG)
        logger2.info(f"Logging to AWS CloudWatch group '{log_group_name}' using key '…{aws_access_key_id[-2:]}'.")
        logger2.critical(f"{prefixed_name} threw an exception while processing: {queued_json_payload}")
        logger2.critical(f"{e}: {traceback.format_exc()}")
        watchtower_log_handler.close()
        # NOTE: following line removed as stats recording used too much disk space
        # stats_client.gauge(user_projects_invoked_string, 1) # Mark as 'failed'
        stats_client.gauge(project_types_invoked_string, 1) # Mark as 'failed'
        raise e # We raise the exception again so it goes into the failed queue

    elapsed_milliseconds = round((time.time() - start_time) * 1000)
    stats_client.timing(f'{general_stats_prefix}.callback.job.duration', elapsed_milliseconds)
    if elapsed_milliseconds < 2000:
        AppSettings.logger.info(f"{prefix}Door43 callback handling for {job_descriptive_name} completed in {elapsed_milliseconds:,} milliseconds.")
    else:
        AppSettings.logger.info(f"{prefix}Door43 callback handling for {job_descriptive_name} completed in {round(time.time() - start_time)} seconds.")

    # Calculate total elapsed time for the job
    total_elapsed_time = datetime.utcnow() - \
                         datetime.strptime(door43_webhook_received_at,
                                           '%Y-%m-%dT%H:%M:%SZ')
    AppSettings.logger.info(f"{prefix}Door43 total job for {job_descriptive_name} completed in {round(total_elapsed_time.total_seconds())} seconds.")
    stats_client.timing(f'{general_stats_prefix}.total.job.duration', round(total_elapsed_time.total_seconds() * 1000))

    # NOTE: following line removed as stats recording used too much disk space
    # stats_client.gauge(user_projects_invoked_string, 0) # Mark as 'succeeded'
    stats_client.gauge(project_types_invoked_string, 0) # Mark as 'succeeded'
    stats_client.incr(f'{general_stats_prefix}.callback.jobs.succeeded')
    AppSettings.close_logger() # Ensure queued logs are uploaded to AWS CloudWatch
# end of job function

# end of callback.py for door43_enqueue_job
