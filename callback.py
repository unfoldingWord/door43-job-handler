# NOTE: This module name and function name are defined by the rq package and our own door43-enqueue-job package
# This code adapted by RJH Sept 2018 from webhook.py

# NOTE: rq_settings.py is executed at program start-up, reads some environment variables, and sets queue name, etc.
#       job() function (at bottom here) is executed by rq package when there is an available entry in the named queue.

# Python imports
import os
import tempfile
#import json
from datetime import datetime
from time import time
from ast import literal_eval

# Library (PyPi) imports
from rq import get_current_job
from statsd import StatsClient # Graphite front-end

# Local imports
from rq_settings import prefix, debug_mode_flag, REDIS_JOB_LIST
from general_tools.file_utils import unzip, write_file, remove_tree, remove
from general_tools.url_utils import download_file
from global_settings.global_settings import GlobalSettings
from client_converter_callback import ClientConverterCallback
from client_linter_callback import ClientLinterCallback



#OUR_NAME = 'Door43_callback_handler'

GlobalSettings(prefix=prefix)
if prefix not in ('', 'dev-'):
    GlobalSettings.logger.critical(f"Unexpected prefix: {prefix!r} -- expected '' or 'dev-'")
stats_prefix = f"door43.{'dev' if prefix else 'prod'}.job-handler" # Can't add .callback here coz we also have .total


# Get the Graphite URL from the environment, otherwise use a local test instance
graphite_url = os.getenv('GRAPHITE_HOSTNAME', 'localhost')
stats_client = StatsClient(host=graphite_url, port=8125, prefix=stats_prefix)



#def update_project_json(base_temp_dir_name, commit_id, upj_job, repo_name, repo_owner):
    #"""
    #:param string commit_id:
    #:param dict upj_job:
    #:param string repo_name:
    #:param string repo_owner:
    #:return:
    #"""
    #project_json_key = f'u/{repo_owner}/{repo_name}/project.json'
    #project_json = GlobalSettings.cdn_s3_handler().get_json(project_json_key)
    #project_json['user'] = repo_owner
    #project_json['repo'] = repo_name
    #project_json['repo_url'] = f'https://git.door43.org/{repo_owner}/{repo_name}'
    #commit = {
        #'id': commit_id,
        #'created_at': upj_job.created_at,
        #'status': upj_job.status,
        #'success': upj_job.success,
        #'started_at': None,
        #'ended_at': None
    #}
    ## Get all other previous commits, and then add this one
    #if 'commits' in project_json:
        #commits = [c for c in project_json['commits'] if c['id'] != commit_id]
        #commits.append(commit)
    #else:
        #commits = [commit]
    #project_json['commits'] = commits
    #project_file = os.path.join(base_temp_dir_name, 'project.json')
    #write_file(project_file, project_json)
    #GlobalSettings.cdn_s3_handler().upload_file(project_file, project_json_key)
## end of update_project_json function


#def upload_build_log_to_s3(base_temp_dir_name, build_log, s3_commit_key, part=''):
    #"""
    #:param dict build_log:
    #:param string s3_commit_key:
    #:param string part:
    #:return:
    #"""
    #build_log_file = os.path.join(base_temp_dir_name, 'build_log.json')
    #write_file(build_log_file, build_log)
    #upload_key = f'{s3_commit_key}/{part}build_log.json'
    #GlobalSettings.logger.debug(f'Saving build log to {GlobalSettings.cdn_bucket_name}/{upload_key}')
    #GlobalSettings.cdn_s3_handler().upload_file(build_log_file, upload_key, cache_time=0)
    ## GlobalSettings.logger.debug('build log contains: ' + json.dumps(build_log_json))
##end of upload_build_log_to_s3


#def create_build_log(commit_id, commit_message, commit_url, compare_url, cbl_job, pusher_username, repo_name, repo_owner):
    #"""
    #:param string commit_id:
    #:param string commit_message:
    #:param string commit_url:
    #:param string compare_url:
    #:param dict cbl_job:
    #:param string pusher_username:
    #:param string repo_name:
    #:param string repo_owner:
    #:return dict:
    #"""
    #build_log_json = dict(cbl_job)
    #build_log_json['repo_name'] = repo_name
    #build_log_json['repo_owner'] = repo_owner
    #build_log_json['commit_id'] = commit_id
    #build_log_json['committed_by'] = pusher_username
    #build_log_json['commit_url'] = commit_url
    #build_log_json['compare_url'] = compare_url
    #build_log_json['commit_message'] = commit_message

    #return build_log_json
## end of create_build_log function


#def clear_commit_directory_in_cdn(s3_commit_key):
    #"""
    #Clear out the commit directory in the cdn bucket for this project revision.
    #"""
    #for obj in GlobalSettings.cdn_s3_handler().get_objects(prefix=s3_commit_key):
        #GlobalSettings.logger.debug('Removing s3 cdn file: ' + obj.key)
        #GlobalSettings.cdn_s3_handler().delete_file(obj.key)
## end of clear_commit_directory_in_cdn function


#def build_multipart_source(source_url_base, file_key, book_filename):
    #params = urlencode({'convert_only': book_filename})
    #source_url = f'{source_url_base}/{file_key}?{params}'
    #return source_url
## end of build_multipart_source function


#def upload_zip_file(commit_id, zip_filepath):
    #file_key = f'preconvert/{commit_id}.zip'
    #GlobalSettings.logger.debug(f'Uploading {zip_filepath} to {GlobalSettings.pre_convert_bucket_name}/{file_key}...')
    #try:
        #GlobalSettings.pre_convert_s3_handler().upload_file(zip_filepath, file_key, cache_time=0)
    #except Exception as e:
        #GlobalSettings.logger.error('Failed to upload zipped repo up to server')
        #GlobalSettings.logger.exception(e)
    #finally:
        #GlobalSettings.logger.debug('finished.')
    #return file_key
## end of upload_zip_file function


#def get_repo_files(base_temp_dir_name, commit_url, repo_name):
    #temp_dir = tempfile.mkdtemp(dir=base_temp_dir_name, prefix=f'{repo_name}_')
    #download_repo(base_temp_dir_name, commit_url, temp_dir)
    #repo_dir = os.path.join(temp_dir, repo_name.lower())
    #if not os.path.isdir(repo_dir):
        #repo_dir = temp_dir
    #return repo_dir
## end of get_repo_files function


#def download_repo(base_temp_dir_name, commit_url, repo_dir):
    #"""
    #Downloads and unzips a git repository from Github or git.door43.org
    #:param str|unicode commit_url: The URL of the repository to download
    #:param str|unicode repo_dir:   The directory where the downloaded file should be unzipped
    #:return: None
    #"""
    #repo_zip_url = commit_url.replace('commit', 'archive') + '.zip'
    #repo_zip_file = os.path.join(base_temp_dir_name, repo_zip_url.rpartition(os.path.sep)[2])

    #try:
        #GlobalSettings.logger.debug(f'Downloading {repo_zip_url}...')

        ## if the file already exists, remove it, we want a fresh copy
        #if os.path.isfile(repo_zip_file):
            #os.remove(repo_zip_file)

        #download_file(repo_zip_url, repo_zip_file)
    #finally:
        #GlobalSettings.logger.debug('Downloading finished.')

    #try:
        #GlobalSettings.logger.debug(f'Unzipping {repo_zip_file}...')
        ## NOTE: This is unsafe if the zipfile comes from an untrusted source
        #unzip(repo_zip_file, repo_dir)
    #finally:
        #GlobalSettings.logger.debug('Unzipping finished.')

    ## clean up the downloaded zip file
    #if os.path.isfile(repo_zip_file):
        #os.remove(repo_zip_file)
##end of download_repo function


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
    GlobalSettings.logger.debug(f"Got outstanding_jobs_list:"
                                f" ({len(outstanding_jobs_list)}) {outstanding_jobs_list}")
    GlobalSettings.logger.debug(f"Currently have {len(outstanding_jobs_list)}"
                                f" outstanding job(s) in {REDIS_JOB_LIST!r}")
    job_id_bytes = vej_job_dict['job_id'].encode()
    if job_id_bytes not in outstanding_jobs_list:
        GlobalSettings.logger.error(f"Not expecting job with id of {vej_job_dict['job_id']}")
        return False
    this_job_dict_bytes = vej_redis_connection.hget(REDIS_JOB_LIST, job_id_bytes)

    # We found a match -- delete that job from the outstanding list
    GlobalSettings.logger.debug(f"Found match for {job_id_bytes}")
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


#def deploy_if_conversion_finished(s3_results_key, identifier):
    #"""
    #Adapted from GitHub/tx-manager/libraries/client/client_linter_callback.py

    #check if all parts are finished, and if so then save merged build_log as well as update jobs table
    #:param s3_results_key: format - u/user/repo/commid_id
    #:param identifier: either
                #job_id/part_count/part_id/book if multi-part job
                    #or
                #job_id if single job
    #:return:
    #"""
    #GlobalSettings.logger.debug(f"deploy_if_conversion_finished({s3_results_key}, {identifier})")

    #output_dir = tempfile.mkdtemp(suffix="", prefix="client_callback_deploy_")
    #build_log = None
    #id_parts = identifier.split('/')
    #multiple_project = len(id_parts) > 3
    #all_parts_completed = True

    #if not multiple_project:
        #GlobalSettings.logger.debug("Single job: checking if convert and lint have completed.")
        #build_log = ClientLinterCallback.merge_build_status_for_part(build_log, s3_results_key, output_dir)
    #else:
        #GlobalSettings.logger.debug("Multiple parts: Checking if all parts completed.")
        #job_id, part_count, part_id, book = id_parts[:4]
        #for i in range(0, int(part_count)):
            #part_key = f'{s3_results_key}/{i}'
            #build_log = ClientLinterCallback.merge_build_status_for_part(build_log, part_key, output_dir)
            #if build_log is None:
                #GlobalSettings.logger.debug(f"Part {part_key} not complete")
                #all_parts_completed = False

    #if all_parts_completed and build_log is not None:  # if all parts found, save build log and kick off deploy
        ## set overall status
        #if len(build_log['errors']):
            #build_log['status'] = 'errors'
        #elif len(build_log['warnings']):
            #build_log['status'] = 'warnings'
        #build_log['ended_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        #if multiple_project:
            #build_log['multiple'] = True

        #ClientLinterCallback.upload_build_log(build_log, "final_build_log.json", output_dir, s3_results_key)
        #if not multiple_project:
            #ClientLinterCallback.upload_build_log(build_log, "build_log.json", output_dir, s3_results_key)
        #ClientLinterCallback.update_project_file(build_log, output_dir)
        #GlobalSettings.logger.debug('All parts completed')
    #else:
        #GlobalSettings.logger.debug('Not all parts completed')
        #build_log = None

    #file_utils.remove_tree(output_dir)
    #return build_log
## end of deploy_if_conversion_finished function


#def update_convert_log(temp_dir, s3_base_key, part=''):
    #GlobalSettings.logger.debug(f"update_convert_log({temp_dir}, {s3_base_key}, {part!r})")
    #build_log_json = get_build_log(s3_base_key, part)
    #upload_convert_log(temp_dir, build_log_json, s3_base_key, part)
    #return build_log_json
## end of update_convert_log function


#def upload_convert_log(temp_dir, build_log_json, s3_base_key, part=''):
    #GlobalSettings.logger.debug(f"upload_convert_log({temp_dir}, {build_log_json}, {s3_base_key}, {part!r})")

    #if 0: # not fixed yet
        #print("SELF NOT FIXED YET")
        #if self.job.started_at:
            #build_log_json['started_at'] = self.job.started_at.strftime('%Y-%m-%dT%H:%M:%SZ')
        #else:
            #build_log_json['started_at'] = None
        #if self.job.ended_at:
            #build_log_json['ended_at'] = self.job.ended_at.strftime('%Y-%m-%dT%H:%M:%SZ')
        #else:
            #build_log_json['ended_at'] = None
        #build_log_json['success'] = self.job.success
        #build_log_json['status'] = self.job.status
        #build_log_json['message'] = self.job.message
        #if self.job.log:
            #build_log_json['log'] = self.job.log
        #else:
            #build_log_json['log'] = []
        #if self.job.warnings:
            #build_log_json['warnings'] = self.job.warnings
        #else:
            #build_log_json['warnings'] = []
        #if self.job.errors:
            #build_log_json['errors'] = self.job.errors
        #else:
            #build_log_json['errors'] = []

    #build_log_key = get_build_log_key(s3_base_key, part, name='convert_log.json')
    #GlobalSettings.logger.debug('Writing build log to ' + build_log_key)
    ## GlobalSettings.logger.debug('build_log contents: ' + json.dumps(build_log_json))
    #cdn_upload_contents(temp_dir, build_log_json, build_log_key)
    #return build_log_json
## end of upload_convert_log function


#def cdn_upload_contents(temp_dir, contents, key):
    #GlobalSettings.logger.debug(f"cdn_upload_contents({temp_dir}, {contents}, {key})")
    #file_name = os.path.join(temp_dir, 'contents.json')
    #write_file(file_name, contents)
    #GlobalSettings.logger.debug('Writing file to ' + key)
    #GlobalSettings.cdn_s3_handler().upload_file(file_name, key, cache_time=0)
## end of cdn_upload_contents function


#def get_build_log(s3_base_key, part=''):
    #GlobalSettings.logger.debug(f"get_build_log({s3_base_key}, {part!r})")
    #build_log_key = get_build_log_key(s3_base_key, part)
    ## GlobalSettings.logger.debug('Reading build log from ' + build_log_key)
    #build_log_json = GlobalSettings.cdn_s3_handler().get_json(build_log_key)
    ## GlobalSettings.logger.debug('build_log contents: ' + json.dumps(build_log_json))
    #return build_log_json
## end of get_build_log function


#def get_build_log_key(s3_base_key, part='', name='build_log.json'):
    #GlobalSettings.logger.debug(f"get_build_log_key({s3_base_key}, {part!r}, {name})")
    #upload_key = '{0}/{1}{2}'.format(s3_base_key, part, name)
    #return upload_key
## end of get_build_log_key function


#def unzip_converted_files(temp_dir, converted_zip_file):
    #unzip_dir = tempfile.mkdtemp(prefix='unzip_', dir=temp_dir)
    #try:
        #GlobalSettings.logger.debug(f"Unzipping {converted_zip_file}...")
        #unzip(converted_zip_file, unzip_dir)
    #finally:
        #GlobalSettings.logger.debug("finished.")

    #return unzip_dir
## end of unzip_converted_files function


#def upload_converted_files(s3_commit_key, unzip_dir):
    #for root, dirs, files in os.walk(unzip_dir):
        #for f in sorted(files):
            #path = os.path.join(root, f)
            #key = s3_commit_key + path.replace(unzip_dir, '')
            #GlobalSettings.logger.debug(f"Uploading {f} to {key}")
            #GlobalSettings.cdn_s3_handler().upload_file(path, key, cache_time=0)
## end of upload_converted_files function


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
    matched_job_dict = verify_result # Do we need any of this info?
    print("Got matched_job_dict:", matched_job_dict )

    if 'identifier' in queued_json_payload:
        identifier = queued_json_payload['identifier']
        GlobalSettings.logger.debug(f"Got identifier={identifier!r} from queued_json_payload")
    elif 'identifier' in matched_job_dict:
        identifier = matched_job_dict['identifier']
        GlobalSettings.logger.debug(f"Got identifier={identifier!r} from matched_job_dict")
    else:
        identifier = job_id
        GlobalSettings.logger.debug(f"Got identifier={identifier!r} from job_id")

    this_job_dict = queued_json_payload.copy()
    # Get needed fields that we saved but didn't submit to tX
    for fieldname in ('user_name', 'repo_name', 'commit_id',):
        assert fieldname not in this_job_dict
        this_job_dict[fieldname] = matched_job_dict[fieldname]

    # It doesn't actually matter which one we do first I think
    GlobalSettings.logger.info("Running linter callback...")
    clc = ClientLinterCallback(this_job_dict, identifier,
                               queued_json_payload['linter_success'],
                               queued_json_payload['linter_info'] if 'linter_info' in queued_json_payload else None,
                               queued_json_payload['linter_warnings'],
                               queued_json_payload['linter_errors'] if 'linter_errors' in queued_json_payload else None,
                               s3_results_key= f"u/{this_job_dict['user_name']}/{this_job_dict['repo_name']}/{this_job_dict['commit_id']}")
    clc_build_log = clc.process_callback()
    GlobalSettings.logger.info("Running converter callback...")
    ccc = ClientConverterCallback(this_job_dict, identifier,
                                  queued_json_payload['converter_success'],
                                  queued_json_payload['converter_info'],
                                  queued_json_payload['converter_warnings'],
                                  queued_json_payload['converter_errors'])
    ccc_build_log = ccc.process_callback()
    final_build_log = ccc_build_log

    #if 0:
        ## The following code is adapted from tx-manager/client_converter_callback.py
        #temp_dir = tempfile.mkdtemp(suffix='', prefix='client_callback_')

        ###job_id_parts = identifier.split('/') # Might contain job_id/book_count/i/book
        ###job_id = job_id_parts[0]
        ##this_job = TxJob.get(job_id)

        ##if not this_job:
            ##identifier = queued_json_payload['identifier'] \
                                ##if 'identifier' in queued_json_payload else None
            ##error = f"No job found for job_id = {job_id}, identifier = {identifier}"
            ##GlobalSettings.logger.critical(error)
            ##raise Exception(error)
        ##print("Got this job:", this_job)

        ##if len(job_id_parts) == 4:
            ##part_count, part_id, book = job_id_parts[1:]
            ##GlobalSettings.logger.debug(f"Multiple project, part {part_id} of {part_count}, converting book {book}")
            ##multiple_project = True
        ##else:
            ##GlobalSettings.logger.debug('Single project')
            ##part_id = None
            ##multiple_project = False

        #this_job_dict = queued_json_payload.copy()
        ## Get needed fields that we saved but didn't submit to tX
        #for fieldname in ('user_name', 'repo_name', 'commit_id',):
            #assert fieldname not in this_job_dict
            #this_job_dict[fieldname] = matched_job_dict[fieldname]

        #assert 'log' not in this_job_dict and 'warnings' not in this_job_dict and 'errors' not in this_job_dict
        #this_job_dict['log'], this_job_dict['warnings'], this_job_dict['errors'] = [], [], []
        #for message in queued_json_payload['converter_info']:
            #this_job_dict['log'].append(message)
            #GlobalSettings.logger.info(message)
        #for message in queued_json_payload['converter_warnings']:
            #this_job_dict['warnings'].append(message)
            #GlobalSettings.logger.warning(message)
        #for message in queued_json_payload['converter_errors']:
            #this_job_dict['errors'].append(message)
            #GlobalSettings.logger.error(message)
        #if queued_json_payload['converter_errors']:
            #result_message = f"{queued_json_payload['convert_module']} function returned with errors."
        #elif queued_json_payload['converter_warnings']:
            #result_message = f"{queued_json_payload['convert_module']} function returned with warnings."
        #else:
            #result_message = f"{queued_json_payload['convert_module']} function returned successfully."
        #this_job_dict['result_message'] = result_message
        #GlobalSettings.logger.info(result_message)

        #this_job_dict['success'] = queued_json_payload['converter_success'] # We'll ignore the linter here
        #if not this_job_dict['success'] or this_job_dict['errors']:
            #this_job_dict['success'] = False
            #this_job_dict['status'] = 'failed'
            #message = "Conversion failed"
            #GlobalSettings.logger.debug(f"Conversion failed, success: {success}, errors: {this_job_dict['errors']}")
        #elif this_job_dict['warnings']:
            #this_job_dict['success'] = True
            #this_job_dict['status'] = 'warnings'
            #message = "Conversion successful with warnings"
        #else:
            #this_job_dict['success'] = True
            #this_job_dict['status'] = 'success'
            #message = "Conversion successful"
        #this_job_dict['message'] = message
        #this_job_dict['log'].append(message)
        #GlobalSettings.logger.info(message)

        #this_job_dict['ended_at'] = datetime.utcnow()
        #finish_message = f"Finished job {job_id} at {this_job_dict['ended_at'].strftime('%Y-%m-%dT%H:%M:%SZ')}"
        #this_job_dict['log'].append(finish_message)
        #GlobalSettings.logger.info(finish_message)

        #multiple_project = False # TODO: Not sure where/how to handle this yet
        #s3_commit_key = f"u/{this_job_dict['user_name']}/{this_job_dict['repo_name']}/{this_job_dict['commit_id']}"
        #upload_key = s3_commit_key
        #if multiple_project:
            #upload_key += '/' + part_id

        #GlobalSettings.logger.debug(f"Callback for commit {s3_commit_key}...")

        ## Download the ZIP file of the converted files
        #converted_zip_url = this_job_dict['output']
        #converted_zip_file = os.path.join(temp_dir, converted_zip_url.rpartition('/')[2])
        #remove(converted_zip_file)  # make sure old file not present
        #GlobalSettings.logger.debug(f"Downloading converted zip file from {converted_zip_url}...")
        #try:
            #download_file(converted_zip_url, converted_zip_file)
            #download_success = True
        #except:
            #download_success = False  # if multiple project we note fail and move on
            #if not multiple_project:
                #remove_tree(temp_dir)  # cleanup
            #this_job_dict['errors'].append("Missing converted file: " + converted_zip_url)
        #finally:
            #GlobalSettings.logger.debug(f"Download finished, success={download_success}")

        #if download_success:
            ## Unzip the archive
            #unzip_dir = unzip_converted_files(temp_dir, converted_zip_file)

            ## Upload all files to the cdn_bucket with the key of <user>/<repo_name>/<commit> of the repo
            #upload_converted_files(upload_key, unzip_dir)

        #if multiple_project:
            ## Now download the existing build_log.json file, update it and upload it back to S3 as convert_log
            #build_log_json = update_convert_log(temp_dir, s3_commit_key, part_id + '/')

            ## mark current part as finished
            #cdn_upload_contents(temp_dir, {}, s3_commit_key + '/' + part_id + '/finished')

        #else:  # single part conversion
            ## Now download the existing build_log.json file, update it and upload it back to S3 as convert_log
            #build_log_json = update_convert_log(temp_dir, s3_commit_key)

            #cdn_upload_contents(temp_dir, {}, s3_commit_key + '/finished')  # flag finished

        #results = deploy_if_conversion_finished(s3_commit_key, identifier)
        #if results:
            #all_parts_completed = True
            #build_log_json = results

        #remove_tree(temp_dir)  # cleanup

        #GlobalSettings.logger.info(f"Door43-Job-Handler process_callback() is finishing with {build_log_json}")

    GlobalSettings.logger.info(f"Door43-Job-Handler process_callback() is finishing with {final_build_log}")
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
                                           '%Y-%m-%dT%H:%M:%SZ')
    total_elapsed_milliseconds = round(total_elapsed_time.total_seconds() * 1000)
    GlobalSettings.logger.info(f"Door43 total job completed in {total_elapsed_milliseconds:,} milliseconds")
    stats_client.timing('total.job.duration', total_elapsed_milliseconds)

    stats_client.incr('callback.jobs.succeeded')
# end of job function

# end of callback.py for door43_enqueue_job
