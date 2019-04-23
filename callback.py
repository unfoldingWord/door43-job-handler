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

# Library (PyPi) imports
from rq import get_current_job
from statsd import StatsClient # Graphite front-end

# Local imports
from rq_settings import prefix, debug_mode_flag, REDIS_JOB_LIST
from global_settings.global_settings import GlobalSettings
from client_converter_callback import ClientConverterCallback
from client_linter_callback import ClientLinterCallback
from door43_tools.project_deployer import ProjectDeployer
from general_tools.file_utils import write_file, remove_tree



GlobalSettings(prefix=prefix)
if prefix not in ('', 'dev-'):
    GlobalSettings.logger.critical(f"Unexpected prefix: '{prefix}' -- expected '' or 'dev-'")
general_stats_prefix = f"door43.{'dev' if prefix else 'prod'}.job-handler" # Can't add .callback here coz we also have .total


# Get the Graphite URL from the environment, otherwise use a local test instance
graphite_url = os.getenv('GRAPHITE_HOSTNAME', 'localhost')
stats_client = StatsClient(host=graphite_url, port=8125)



def verify_expected_job(vej_job_dict, vej_redis_connection):
    """
    Check that we have this outstanding job in a REDIS dict
        and delete it once we make a match.

    Return the job dict or False
    """
    job_id = vej_job_dict['job_id']
    # GlobalSettings.logger.debug(f"verify_expected_job({job_id})")

    outstanding_jobs_dict_bytes = vej_redis_connection.get(REDIS_JOB_LIST) # Gets bytes!!!
    if not outstanding_jobs_dict_bytes:
        GlobalSettings.logger.error("No expected jobs found in redis store")
        return False
    # GlobalSettings.logger.debug(f"Got outstanding_jobs_dict_bytes:"
    #                             f" ({len(outstanding_jobs_dict_bytes)}) {outstanding_jobs_dict_bytes}")
    assert isinstance(outstanding_jobs_dict_bytes,bytes)
    outstanding_jobs_dict_json_string = outstanding_jobs_dict_bytes.decode() # bytes -> str
    assert isinstance(outstanding_jobs_dict_json_string,str)
    outstanding_jobs_dict = json.loads(outstanding_jobs_dict_json_string)
    assert isinstance(outstanding_jobs_dict,dict)
    GlobalSettings.logger.info(f"Currently have {len(outstanding_jobs_dict)}"
                               f" outstanding job(s) in '{REDIS_JOB_LIST}' redis store")
    if job_id not in outstanding_jobs_dict:
        GlobalSettings.logger.error(f"Not expecting job with id of {job_id}")
        GlobalSettings.logger.debug(f"Only had job ids: {outstanding_jobs_dict.keys()}")
        return False
    this_job_dict = outstanding_jobs_dict[job_id]

    # We found a match -- delete that job from the outstanding list
    GlobalSettings.logger.debug(f"Found job match for {job_id}")
    del outstanding_jobs_dict[job_id]
    if outstanding_jobs_dict:
        GlobalSettings.logger.debug(f"Still have {len(outstanding_jobs_dict)}"
                                    f" outstanding job(s) in '{REDIS_JOB_LIST}'")
        # Update the job dict in redis now that this job has been deleted from it
        outstanding_jobs_json_string = json.dumps(outstanding_jobs_dict)
        vej_redis_connection.set(REDIS_JOB_LIST, outstanding_jobs_json_string)
    else: # no outstanding jobs left
        GlobalSettings.logger.info("Deleting the final outstanding job"
                                  f" in '{REDIS_JOB_LIST}' redis store")
        del_result = vej_redis_connection.delete(REDIS_JOB_LIST)
        # print("  Got redis delete result:", del_result)
        assert del_result == 1 # Should only have deleted one key

    #GlobalSettings.logger.debug(f"Returning {this_job_dict}")
    return this_job_dict
# end of verify_expected_job


def merge_results_logs(build_log, file_results, linter_file):
    """
    Given a second partial build log file_results,
        combine the log/warnings/errors lists into the first build_log.
    """
    assert not linter_file
    GlobalSettings.logger.debug(f"Callback.merge_results_logs(…, {file_results}, {linter_file})…")
    if not build_log:
        return file_results
    if file_results:
        merge_dicts_lists(build_log, file_results, 'message')
        merge_dicts_lists(build_log, file_results, 'log')
        merge_dicts_lists(build_log, file_results, 'warnings')
        merge_dicts_lists(build_log, file_results, 'errors')
        if not linter_file and ('success' in file_results) and (file_results['success'] is False):
            build_log['success'] = file_results['success']
    return build_log
# end of merge_results_logs function


def merge_dicts_lists(build_log, file_results, key):
    """
    Used for merging log dicts from various sub-processes.

    build_log is a dict
    file_results is a dict
    value is a key (string) for the lists that will be merged if in both dicts

    Alters first parameter build_log in place.
    """
    GlobalSettings.logger.debug(f"Callback.merge_dicts({build_log}, {file_results}, '{key}')…")
    if key in file_results:
        value = file_results[key]
        if value:
            if (key in build_log) and (build_log[key]):
                build_log[key] += value
            else:
                build_log[key] = value
# end of merge_dicts_lists function


# TODO: Is this really needed? What uses it?
def update_project_file(build_log, output_dir):
    GlobalSettings.logger.debug(f"Callback.update_project_file({build_log}, output_dir={output_dir})…")
    # if not output_dir:
    #     output_dir = tempfile.mkdtemp(suffix='',
    #                  prefix='Door43_callback_update_project_file_' + datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S_'))

    commit_id = build_log['commit_id']
    user_name = build_log['repo_owner']
    repo_name = build_log['repo_name']
    project_json_key = f'u/{user_name}/{repo_name}/project.json'
    project_json = GlobalSettings.cdn_s3_handler().get_json(project_json_key)
    project_json['user'] = user_name
    project_json['repo'] = repo_name
    project_json['repo_url'] = f'https://{GlobalSettings.gogs_url}/{user_name}/{repo_name}'
    commit = {
        'id': commit_id,
        'created_at': build_log['created_at'],
        'status': build_log['status'],
        'success': build_log['success'],
        'started_at': None,
        'ended_at': None
    }
    if 'started_at' in build_log:
        commit['started_at'] = build_log['started_at']
    if 'ended_at' in build_log:
        commit['ended_at'] = build_log['ended_at']
    if 'commits' not in project_json:
        project_json['commits'] = []
    commits = []
    for c in project_json['commits']:
        if c['id'] != commit_id:
            commits.append(c)
    commits.append(commit)
    project_json['commits'] = commits
    project_file = os.path.join(output_dir, 'project.json')
    write_file(project_file, project_json)
    GlobalSettings.cdn_s3_handler().upload_file(project_file, project_json_key, cache_time=0)
    # if prefix and debug_mode_flag:
    #     GlobalSettings.logger.debug(f"Temp folder '{output_dir}' has been left on disk for debugging!")
    # else:
    #     remove_tree(output_dir)
    # return project_json
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
    GlobalSettings.logger.debug(f"Processing {pc_prefix+' ' if pc_prefix else ''}callback: {str_payload_adjusted}")

    # Check that this is an expected callback job
    if 'job_id' not in queued_json_payload:
        error = "Callback job has no 'job_id' field"
        GlobalSettings.logger.critical(error)
        raise Exception(error)
    job_id = queued_json_payload['job_id']
    verify_result = verify_expected_job(queued_json_payload, redis_connection)
    # NOTE: The above deletes the matched job entry,
    #   so this means callback cannot be successfully retried if it fails below
    if not verify_result:
        error = f"No waiting job found for {queued_json_payload}"
        GlobalSettings.logger.critical(error)
        raise Exception(error)
    matched_job_dict = verify_result
    GlobalSettings.logger.debug(f"Got matched_job_dict: {matched_job_dict}")
    job_descriptive_name = f"{queued_json_payload['resource_type']}({queued_json_payload['input_format']})"
    if 'repo_owner' not in matched_job_dict: # Why did we have to add this?
        matched_job_dict['repo_owner'] = matched_job_dict['user_name'] # Where did it used to be done/gotten from?

    this_job_dict = queued_json_payload.copy()
    # Get needed fields that we saved but didn't submit to tX
    for fieldname in ('user_name', 'repo_name', 'commit_id'):
        assert fieldname not in this_job_dict
        this_job_dict[fieldname] = matched_job_dict[fieldname]

    if 'preprocessor_warnings' in matched_job_dict:
        # GlobalSettings.logger.debug(f"Got {len(matched_job_dict['preprocessor_warnings'])}"
        #                            f" remembered preprocessor_warnings: {matched_job_dict['preprocessor_warnings']}")
        # Prepend preprocessor results to linter warnings
        # total_warnings = len(matched_job_dict['preprocessor_warnings']) + len(queued_json_payload['linter_warnings'])
        queued_json_payload['linter_warnings'] = matched_job_dict['preprocessor_warnings'] \
                                               + queued_json_payload['linter_warnings']
        # GlobalSettings.logger.debug(f"Now have {len(queued_json_payload['linter_warnings'])}"
        #                             f" linter_warnings: {queued_json_payload['linter_warnings']}")
        # assert len(queued_json_payload['linter_warnings']) == total_warnings
        del matched_job_dict['preprocessor_warnings'] # No longer required

    if 'identifier' in queued_json_payload:
        # NOTE: The identifier we send to the tX Job Handler system is a complex string, not a job_id
        identifier = queued_json_payload['identifier']
        GlobalSettings.logger.debug(f"Got identifier from queued_json_payload: {identifier}.")
        job_descriptive_name = f'{identifier} {job_descriptive_name}'
    if 'identifier' in matched_job_dict: # overrides
        identifier = matched_job_dict['identifier']
        GlobalSettings.logger.debug(f"Got identifier from matched_job_dict: {identifier}.")
    else:
        identifier = job_id
        GlobalSettings.logger.debug(f"Got identifier from job_id: {identifier}.")
    # NOTE: following line removed as stats recording used too much disk space
    # user_projects_invoked_string = matched_job_dict['user_projects_invoked_string'] \
    #                     if 'user_projects_invoked_string' in matched_job_dict \
    #                     else f'??{identifier}??'
    project_types_invoked_string = f"{general_stats_prefix}.types.invoked.{matched_job_dict['resource_type']}"

    matched_job_dict['log'] = []
    matched_job_dict['warnings'] = []
    matched_job_dict['errors'] = []

    our_temp_dir = tempfile.mkdtemp(suffix='',
                     prefix='Door43_callback_' + datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S_'))

    # We get the adapted tx-manager calls to do our work for us
    # It doesn't actually matter which one we do first I think
    GlobalSettings.logger.info("Running linter post-processing…")
    url_part2 = f"u/{this_job_dict['user_name']}/{this_job_dict['repo_name']}/{this_job_dict['commit_id']}"
    clc = ClientLinterCallback(this_job_dict, identifier,
                               queued_json_payload['linter_success'],
                               queued_json_payload['linter_info'] if 'linter_info' in queued_json_payload else None,
                               queued_json_payload['linter_warnings'],
                               queued_json_payload['linter_errors'] if 'linter_errors' in queued_json_payload else None,
                                )
                            #    s3_results_key=url_part2)
    linter_log = clc.do_post_processing()
    build_log = merge_results_logs(matched_job_dict, linter_log, linter_file=False) # What is the last parameter for?
    GlobalSettings.logger.info("Running converter post-processing…")
    ccc = ClientConverterCallback(this_job_dict, identifier,
                                  queued_json_payload['converter_success'],
                                  queued_json_payload['converter_info'],
                                  queued_json_payload['converter_warnings'],
                                  queued_json_payload['converter_errors'],
                                  our_temp_dir)
    unzip_dir, converter_log = ccc.do_post_processing()
    # deploy_if_conversion_finished(url_part2, identifier)
    final_build_log = merge_results_logs(build_log, converter_log, linter_file=False) # What is the last parameter for?

    if final_build_log['errors']:
        final_build_log['status'] = 'errors'
    elif final_build_log['warnings']:
        final_build_log['status'] = 'warnings'
    else:
        final_build_log['status'] = 'success'
        final_build_log['success'] = True
    final_build_log['ended_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    update_project_file(final_build_log, our_temp_dir)
    # NOTE: The following is disabled coz it's done (again) later by the deployer
    # upload_build_log(final_build_log, 'build_log.json', output_dir, url_part2, cache_time=600)

    if unzip_dir is None:
        GlobalSettings.logger.critical("Unable to deploy because file download failed previously")
        deployed = False
    else:
        # Now deploy the new pages (was previously a separate AWS Lambda call)
        GlobalSettings.logger.info(f"Deploying to the website (convert status='{final_build_log['status']}')…")
        deployer = ProjectDeployer(unzip_dir, our_temp_dir)
        # build_log_key = f'{url_part2}/build_log.json'
        # GlobalSettings.logger.debug(f"Got {GlobalSettings.cdn_bucket_name} build_log_key={build_log_key}")
        # deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)
        # No need to download the build log since we have it here
        deployer.deploy_revision_to_door43(final_build_log) # Does templating and uploading
        deployed = True

    if prefix and debug_mode_flag:
        GlobalSettings.logger.debug(f"Temp folder '{our_temp_dir}' has been left on disk for debugging!")
    else:
        remove_tree(our_temp_dir)

    # Finishing off
    str_final_build_log = str(final_build_log)
    str_final_build_log_adjusted = str_final_build_log if len(str_final_build_log)<1500 \
                            else f'{str_final_build_log[:1000]} …… {str_final_build_log[-500:]}'
    GlobalSettings.logger.info(f"Door43-Job-Handler process_callback_job() for {job_descriptive_name} is finishing with {str_final_build_log_adjusted}")
    if deployed:
        GlobalSettings.logger.info(f"{'Should become available' if final_build_log['success']=='True' or final_build_log['status'] in ('success', 'warnings') else 'Would be'}"
                               f" at https://{GlobalSettings.door43_bucket_name.replace('dev-door43','dev.door43')}/{url_part2}/")
    return job_descriptive_name
#end of process_callback_job function



def job(queued_json_payload):
    """
    This function is called by the rq package to process a job in the queue(s).

    The job is removed from the queue before the job is started,
        but if the job throws an exception or times out (timeout specified in enqueue process)
            then the job gets added to the 'failed' queue.
    """
    GlobalSettings.logger.info("Door43-Job-Handler received a callback" + (" (in debug mode)" if debug_mode_flag else ""))
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
        job_descriptive_name = process_callback_job(prefix, queued_json_payload, current_job.connection)
    except Exception as e:
        # Catch most exceptions here so we can log them to CloudWatch
        prefixed_name = f"{prefix}Door43_Callback"
        GlobalSettings.logger.critical(f"{prefixed_name} threw an exception while processing: {queued_json_payload}")
        GlobalSettings.logger.critical(f"{e}: {traceback.format_exc()}")
        GlobalSettings.close_logger() # Ensure queued logs are uploaded to AWS CloudWatch
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
        GlobalSettings.logger.info(f"{prefix}Door43 callback handling for {job_descriptive_name} completed in {elapsed_milliseconds:,} milliseconds.")
    else:
        GlobalSettings.logger.info(f"{prefix}Door43 callback handling for {job_descriptive_name} completed in {round(time.time() - start_time)} seconds.")

    # Calculate total elapsed time for the job
    total_elapsed_time = datetime.utcnow() - \
                         datetime.strptime(queued_json_payload['door43_webhook_received_at'],
                                           '%Y-%m-%dT%H:%M:%SZ')
    GlobalSettings.logger.info(f"{prefix}Door43 total job for {job_descriptive_name} completed in {round(total_elapsed_time.total_seconds())} seconds.")
    stats_client.timing(f'{general_stats_prefix}.total.job.duration', round(total_elapsed_time.total_seconds() * 1000))

    # NOTE: following line removed as stats recording used too much disk space
    # stats_client.gauge(user_projects_invoked_string, 0) # Mark as 'succeeded'
    stats_client.gauge(project_types_invoked_string, 0) # Mark as 'succeeded'
    stats_client.incr(f'{general_stats_prefix}.callback.jobs.succeeded')
    GlobalSettings.close_logger() # Ensure queued logs are uploaded to AWS CloudWatch
# end of job function

# end of callback.py for door43_enqueue_job
