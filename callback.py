# NOTE: This module name and function name are defined by the rq package and our own door43-enqueue-job package
# This code adapted by RJH Sept 2018 from webhook.py

# NOTE: rq_settings.py is executed at program start-up, reads some environment variables, and sets queue name, etc.
#       job() function (at bottom here) is executed by rq package when there is an available entry in the named queue.

# Python imports
import os
from datetime import datetime
from time import time
from ast import literal_eval
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



GlobalSettings(prefix=prefix)
if prefix not in ('', 'dev-'):
    GlobalSettings.logger.critical(f"Unexpected prefix: {prefix!r} -- expected '' or 'dev-'")
stats_prefix = f"door43.{'dev' if prefix else 'prod'}.job-handler" # Can't add .callback here coz we also have .total


# Get the Graphite URL from the environment, otherwise use a local test instance
graphite_url = os.getenv('GRAPHITE_HOSTNAME', 'localhost')
stats_client = StatsClient(host=graphite_url, port=8125)



def verify_expected_job(vej_job_dict, vej_redis_connection):
    """
    Check that we have this outstanding job in a REDIS dict
        and delete it once we make a match.

    Return the job dict or False
    """
    GlobalSettings.logger.debug(f"verify_expected_job({vej_job_dict['job_id']})")

    outstanding_jobs_list = vej_redis_connection.hkeys(REDIS_JOB_LIST) # Gets bytes!!!
    if not outstanding_jobs_list:
        GlobalSettings.logger.error("No expected jobs found")
        return False
    # GlobalSettings.logger.debug(f"Got outstanding_jobs_list:"
    #                             f" ({len(outstanding_jobs_list)}) {outstanding_jobs_list}")
    # GlobalSettings.logger.debug(f"Currently have {len(outstanding_jobs_list)}"
    #                             f" outstanding job(s) in {REDIS_JOB_LIST!r}")
    job_id_bytes = vej_job_dict['job_id'].encode()
    if job_id_bytes not in outstanding_jobs_list:
        GlobalSettings.logger.error(f"Not expecting job with id of {vej_job_dict['job_id']}")
        return False
    this_job_dict_bytes = vej_redis_connection.hget(REDIS_JOB_LIST, job_id_bytes)

    # We found a match -- delete that job from the outstanding list
    GlobalSettings.logger.debug(f"Found job match for {job_id_bytes}")
    if len(outstanding_jobs_list) > 1:
        GlobalSettings.logger.debug(f"Still have {len(outstanding_jobs_list)-1}"
                                    f" outstanding job(s) in {REDIS_JOB_LIST!r}")
    #vej_redis_connection.hmset(REDIS_JOB_LIST, outstanding_jobs_dict) # Doesn't delete!!!
    del_result = vej_redis_connection.hdel(REDIS_JOB_LIST, job_id_bytes)
    #print("  Got delete result:", del_result)
    assert del_result == 1

    this_job_dict = literal_eval(this_job_dict_bytes.decode()) # bytes -> str -> dict
    #GlobalSettings.logger.debug(f"Returning {this_job_dict}")
    return this_job_dict
# end of verify_expected_job


# user_projects_invoked_string = 'user-projects.invoked.unknown--unknown'
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
        error = f"No job found for {queued_json_payload}"
        GlobalSettings.logger.critical(error)
        raise Exception(error)
    matched_job_dict = verify_result
    GlobalSettings.logger.debug(f"Got matched_job_dict: {matched_job_dict}")
    job_descriptive_name = f"{queued_json_payload['resource_type']}({queued_json_payload['input_format']})"


    this_job_dict = queued_json_payload.copy()
    # Get needed fields that we saved but didn't submit to tX
    for fieldname in ('user_name', 'repo_name', 'commit_id',):
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

    # We get the tx-manager existing calls to do our work for us
    # It doesn't actually matter which one we do first I think
    GlobalSettings.logger.info("Running linter post-processing…")
    url_part2 = f"u/{this_job_dict['user_name']}/{this_job_dict['repo_name']}/{this_job_dict['commit_id']}"
    clc = ClientLinterCallback(this_job_dict, identifier,
                               queued_json_payload['linter_success'],
                               queued_json_payload['linter_info'] if 'linter_info' in queued_json_payload else None,
                               queued_json_payload['linter_warnings'],
                               queued_json_payload['linter_errors'] if 'linter_errors' in queued_json_payload else None,
                               s3_results_key=url_part2)
    # clc_build_log = clc.do_post_processing() # We don't use the result
    clc.do_post_processing()
    GlobalSettings.logger.info("Running converter post-processing…")
    ccc = ClientConverterCallback(this_job_dict, identifier,
                                  queued_json_payload['converter_success'],
                                  queued_json_payload['converter_info'],
                                  queued_json_payload['converter_warnings'],
                                  queued_json_payload['converter_errors'])
    ccc_build_log = ccc.do_post_processing()
    final_build_log = ccc_build_log

    # Now deploy the new pages (was previously a separate AWS Lambda call)
    GlobalSettings.logger.info(f"Deploying to the website (convert status='{final_build_log['status']}')…")
    deployer = ProjectDeployer()
    # build_log_key = f'{url_part2}/build_log.json'
    # GlobalSettings.logger.debug(f"Got {GlobalSettings.cdn_bucket_name} build_log_key={build_log_key}")
    # deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)
    deployer.deploy_revision_to_door43(final_build_log) # No need to download the build log since we have it here

    # Finishing off
    str_final_build_log = str(final_build_log)
    str_final_build_log_adjusted = str_final_build_log if len(str_final_build_log)<1500 \
                            else f'{str_final_build_log[:1000]} …… {str_final_build_log[-500:]}'
    GlobalSettings.logger.info(f"Door43-Job-Handler process_callback_job() for {job_descriptive_name} is finishing with {str_final_build_log_adjusted}")
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
    start_time = time()
    stats_client.incr(f'{stats_prefix}.callback.jobs.attempted')

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
        raise e # We raise the exception again so it goes into the failed queue

    elapsed_milliseconds = round((time() - start_time) * 1000)
    stats_client.timing(f'{stats_prefix}.callback.job.duration', elapsed_milliseconds)
    if elapsed_milliseconds < 2000:
        GlobalSettings.logger.info(f"{prefix}Door43 callback handling for {job_descriptive_name} completed in {elapsed_milliseconds:,} milliseconds.")
    else:
        GlobalSettings.logger.info(f"{prefix}Door43 callback handling for {job_descriptive_name} completed in {round(time() - start_time)} seconds.")

    # Calculate total elapsed time for the job
    total_elapsed_time = datetime.utcnow() - \
                         datetime.strptime(queued_json_payload['door43_webhook_received_at'],
                                           '%Y-%m-%dT%H:%M:%SZ')
    GlobalSettings.logger.info(f"{prefix}Door43 total job for {job_descriptive_name} completed in {round(total_elapsed_time.total_seconds())} seconds.")
    stats_client.timing(f'{stats_prefix}.total.job.duration', round(total_elapsed_time.total_seconds() * 1000))

    # NOTE: following line removed as stats recording used too much disk space
    # stats_client.gauge(user_projects_invoked_string, 0) # Mark as 'succeeded'
    stats_client.incr(f'{stats_prefix}.callback.jobs.succeeded')
    GlobalSettings.close_logger() # Ensure queued logs are uploaded to AWS CloudWatch
# end of job function

# end of callback.py for door43_enqueue_job
