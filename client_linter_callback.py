import os
import tempfile
import time
from datetime import datetime

from rq_settings import prefix, debug_mode_flag
from global_settings.global_settings import GlobalSettings
from general_tools.file_utils import write_file, remove_tree



class ClientLinterCallback:

    def __init__(self, job_dict, identifier, success, info, warnings, errors, s3_results_key):
        """
        :param identifier: either
                    job_id/part_count/part_id/book if multi-part job
                        or
                    job_id if single job
        :param bool success:
        :param list info:
        :param list warnings:
        :param list errors:
        :param s3_results_key: format
                    u/user/repo/commid_id if single part
                        or
                    u/user/repo/commid_id/part_id if multi-part job
        """
        self.job_dict = job_dict
        self.identifier = identifier
        self.success = success
        self.log = info
        self.warnings = warnings
        self.errors = errors
        self.all_parts_completed = False
        self.multipart = False

        if not self.log:
            self.log = []
        if not self.warnings:
            self.warnings = []
        if not self.errors:
            self.errors = []
        self.temp_dir = tempfile.mkdtemp(suffix='',
                            prefix='Door43_linter_callback_' + datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f_'))
        self.s3_results_key = s3_results_key
        self.job = None

    def process_callback(self):
        if not self.identifier:
            error = 'No identifier found'
            GlobalSettings.logger.error(error)
            raise Exception(error)

        if not self.s3_results_key:
            error = f"No s3_results_key found for identifier = {self.identifier}"
            GlobalSettings.logger.error(error)
            raise Exception(error)

        id_parts = self.identifier.split('/')
        self.multipart = len(id_parts) > 3
        if self.multipart:
            part_count, part_id, book = id_parts[1:4]
            GlobalSettings.logger.debug('Multiple project, part {0} of {1}, linted book {2}'.
                             format(part_id, part_count, book))
            s3__master_results_key = '/'.join(self.s3_results_key.split('/')[:-1])
        else:
            GlobalSettings.logger.debug('Single project')
            s3__master_results_key = self.s3_results_key

        build_log = {
            'identifier': self.identifier,
            'success': self.success,
            'multipart_project': self.multipart,
            'log': self.log,
            'warnings': self.warnings,
            'errors': self.errors,
            's3_commit_key': self.s3_results_key
        }

        if not self.success:
            msg = "Linter failed for identifier: " + self.identifier
            build_log['warnings'].append(msg)
            GlobalSettings.logger.error(msg)
        else:
            GlobalSettings.logger.debug(f"Linter {self.identifier} had success with"
                                        f" {len(self.warnings)} warnings: {', '.join(self.warnings[:5])} …")

        has_warnings = len(build_log['warnings']) > 0
        if has_warnings:
            msg = f"Linter {self.identifier} has Warnings!"
            build_log['log'].append(msg)
        else:
            msg = f"Linter {self.identifier} completed with no warnings"
            build_log['log'].append(msg)

        ClientLinterCallback.upload_build_log(build_log, 'lint_log.json', self.temp_dir, self.s3_results_key)

        results = ClientLinterCallback.deploy_if_conversion_finished(s3__master_results_key, self.identifier)
        if results:
            self.all_parts_completed = True
            build_log = results

        if prefix and debug_mode_flag:
            GlobalSettings.logger.debug(f"Temp folder '{self.temp_dir}' has been left on disk for debugging!")
        else:
            remove_tree(self.temp_dir)  # cleanup
        GlobalSettings.db_close()
        return build_log


    @staticmethod
    def upload_build_log(build_log, file_name, output_dir, s3_results_key, cache_time=0):
        build_log_file = os.path.join(output_dir, file_name)
        write_file(build_log_file, build_log)
        upload_key = f'{s3_results_key}/{file_name}'
        GlobalSettings.logger.debug(f"Uploading build log to {GlobalSettings.cdn_bucket_name}/{upload_key} …")
        GlobalSettings.cdn_s3_handler().upload_file(build_log_file, upload_key, cache_time=cache_time)


    @staticmethod
    def deploy_if_conversion_finished(s3_results_key, identifier):
        """
        check if all parts are finished, and if so then save merged build_log as well as update jobs table
        :param s3_results_key: format - u/user/repo/commid_id
        :param identifier: either
                    job_id/part_count/part_id/book if multi-part job
                        or
                    job_id if single job
        :return:
        """
        output_dir = tempfile.mkdtemp(suffix='',
                        prefix='Door43_callback_deploy_' + datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f_'))
        build_log = None
        id_parts = identifier.split('/')
        multiple_project = len(id_parts) > 3
        all_parts_completed = True

        if not multiple_project:
            GlobalSettings.logger.debug('Single job: checking if both convert and lint have completed_')
            build_log = ClientLinterCallback.merge_build_status_for_part(build_log, s3_results_key, output_dir)
        else:
            GlobalSettings.logger.debug('Multiple parts: Checking if all parts completed_')
            # job_id, part_count, part_id, book = id_parts[:4]
            part_count = id_parts[1]
            for i in range(0, int(part_count)):
                part_key = f'{s3_results_key}/{i}'
                build_log = ClientLinterCallback.merge_build_status_for_part(build_log, part_key, output_dir)
                if build_log is None:
                    GlobalSettings.logger.debug(f"Part {part_key} not complete.")
                    all_parts_completed = False

        if all_parts_completed and build_log is not None:  # if all parts found, save build log and kick off deploy
            # set overall status
            if len(build_log['errors']):
                build_log['status'] = 'errors'
            elif len(build_log['warnings']):
                build_log['status'] = 'warnings'
            build_log['ended_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            if multiple_project:
                build_log['multiple'] = True

            ClientLinterCallback.upload_build_log(build_log, 'final_build_log.json', output_dir, s3_results_key)
            if not multiple_project:
                ClientLinterCallback.upload_build_log(build_log, 'build_log.json', output_dir, s3_results_key)
            ClientLinterCallback.update_project_file(build_log, output_dir)
            GlobalSettings.logger.debug("All parts completed.")
        else:
            GlobalSettings.logger.debug("Not all parts completed.")
            remove_tree(output_dir)
            build_log = None

        if prefix and debug_mode_flag:
            GlobalSettings.logger.debug(f"Temp folder '{output_dir}' has been left on disk for debugging!")
        else:
            remove_tree(output_dir)
        return build_log


    @staticmethod
    def upload_logs(s3_results_key, build_log, output_dir):
        """
        Was update_jobs_table
        """
        # GlobalSettings.logger.debug(f"upload_logs({s3_results_key}, {build_log}, {output_dir})")
        GlobalSettings.logger.info(f"Uploading final logs to S3:…/{s3_results_key} _")

        job_id = build_log['job_id']
        GlobalSettings.logger.debug('merging build_logs for job : ' + job_id)
        build_log['ended_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        # Flag this part as done
        ClientLinterCallback.upload_build_log(build_log, 'merged.json', output_dir, s3_results_key)
        # Update build_log to start deploy of this part
        ClientLinterCallback.upload_build_log(build_log, 'build_log.json', output_dir, s3_results_key, cache_time=600)
        GlobalSettings.logger.info("Deployment on AWS should automatically begin now_")
        return


    @staticmethod
    def merge_build_status_for_part(build_log, s3_results_key, output_dir):
        """
        merges convert and linter status for this part of conversion into build_log.  Returns None if part not finished.
        :param output_dir:
        :param build_log:
        :param s3_results_key:
        :return:
        """
        GlobalSettings.logger.debug(f"merge_build_status_for_part({build_log}, {s3_results_key}, {output_dir})")

        part_build_log = ClientLinterCallback.get_results(s3_results_key, "merged.json")  # see if already merged
        if not part_build_log:
            convert_finished = ClientLinterCallback.is_convert_finished(s3_results_key)
            if not convert_finished:
                GlobalSettings.logger.debug(f"Convert not yet finished for {s3_results_key}")
                return None

            part_build_log = ClientLinterCallback.get_results(s3_results_key, "convert_log.json")
            if part_build_log:
                part_build_log_combined = ClientLinterCallback.merge_build_status_for_file(part_build_log,
                                                                                           s3_results_key,
                                                                                           "lint_log.json",
                                                                                           linter_file=True)
                if not part_build_log_combined:
                    GlobalSettings.logger.debug(f"Lint_log.json not found yet for {s3_results_key}, wait and retry")
                    time.sleep(2)
                    part_build_log_combined = ClientLinterCallback.merge_build_status_for_file(part_build_log,
                                                                                               s3_results_key,
                                                                                               "lint_log.json",
                                                                                               linter_file=True)

                if part_build_log_combined:
                    build_log = ClientLinterCallback.merge_results_logs(build_log, part_build_log_combined,
                                                                        linter_file=False)
                    ClientLinterCallback.upload_logs(s3_results_key, part_build_log_combined, output_dir)
                    return build_log
                else:
                    GlobalSettings.logger.debug(f"Lint_log.json not found for {s3_results_key}")
            else:
                GlobalSettings.logger.debug(f"convert_log.json not found for {s3_results_key}")

            return None

        else:
            build_log = ClientLinterCallback.merge_results_logs(build_log, part_build_log, linter_file=False)
            return build_log


    @staticmethod
    def is_convert_finished(s3_results_key):
        key = f'{s3_results_key}/finished'
        try:
            convert_finished = GlobalSettings.cdn_s3_handler().key_exists(key)
        except Exception:
            convert_finished = False
        return convert_finished


    @staticmethod
    def get_results(s3_results_key, file_name):
        key = f'{s3_results_key}/{file_name}'
        file_results = GlobalSettings.cdn_s3_handler().get_json(key)
        return file_results


    @staticmethod
    def merge_build_status_for_file(build_log, s3_results_key, file_name, linter_file=False):
        key = f'{s3_results_key}/{file_name}'
        file_results = GlobalSettings.cdn_s3_handler().get_json(key)
        if file_results:
            build_log = ClientLinterCallback.merge_results_logs(build_log, file_results, linter_file)
            return build_log
        return None

    @staticmethod
    def merge_results_logs(build_log, file_results, linter_file):
        if not build_log:
            return file_results
        if file_results:
            ClientLinterCallback.merge_lists(build_log, file_results, 'log')
            ClientLinterCallback.merge_lists(build_log, file_results, 'warnings')
            ClientLinterCallback.merge_lists(build_log, file_results, 'errors')
            if not linter_file and ('success' in file_results) and (file_results['success'] is False):
                build_log['success'] = file_results['success']
        return build_log

    @staticmethod
    def merge_lists(build_log, file_results, key):
        if key in file_results:
            value = file_results[key]
            if value:
                if (key in build_log) and (build_log[key]):
                    build_log[key] += value
                else:
                    build_log[key] = value

    @staticmethod
    def update_project_file(build_log, output_dir):
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
        return project_json
