import json
import os
import tempfile
from datetime import datetime

from global_settings.global_settings import GlobalSettings
from general_tools.file_utils import unzip, write_file, remove_tree, remove
from general_tools.url_utils import download_file
from client_linter_callback import ClientLinterCallback


class DummyJob:
    """
    This is a temporary class to replace the basic functionality of TxJob
    """
    def __init__(self, job_dict):
        self.job_dict = job_dict
        self.job_id = self.job_dict['job_id']
        self.convert_module = self.job_dict['convert_module']
        self.user_name = self.job_dict['user_name']
        self.repo_name = self.job_dict['repo_name']
        self.commit_id = self.job_dict['commit_id']
        self.output = self.job_dict['output']
        self.started_at = datetime.strptime(self.job_dict['started_at'], '%Y-%m-%dT%H:%M:%SZ')
        self.log, self.warnings, self.errors = [], [], []

    def log_message(self, msg):
        self.log.append(msg)
        GlobalSettings.logger.debug(msg) # DEBUG coz we don't need all these displayed in production mode

    def warnings_message(self, msg):
        self.warnings.append(msg)
        GlobalSettings.logger.warning(msg)

    def error_message(self, msg):
        self.errors.append(msg)
        GlobalSettings.logger.error(msg)

    def update(self):
        pass # Nowhere to save anything


class ClientConverterCallback:

    def __init__(self, job_dict, identifier, success, info, warnings, errors):
        """
        :param string identifier:
        :param bool success:
        :param list info:
        :param list warnings:
        :param list errors:
        """
        self.job = DummyJob(job_dict)
        self.identifier = identifier
        self.success = success
        self.log = info
        self.warnings = warnings
        self.errors = errors
        self.all_parts_completed = False

        if not self.log:
            self.log = []
        if not self.warnings:
            self.warnings = []
        if not self.errors:
            self.errors = []
        self.temp_dir = tempfile.mkdtemp(suffix="", prefix="client_callback_")

    def process_callback(self):
        job_id_parts = self.identifier.split('/')
        job_id = job_id_parts[0]
        #self.job = TxJob.get(job_id)
        assert job_id == self.job.job_id

        # This is now checked elsewhere
        #if not self.job:
            #error = 'No job found for job_id = {0}, identifier = {0}'.format(job_id, self.identifier)
            #GlobalSettings.logger.error(error)
            #raise Exception(error)

        if len(job_id_parts) == 4:
            part_count, part_id, book = job_id_parts[1:]
            GlobalSettings.logger.debug(f"Multiple project, part {part_id}"
                                        f" of {part_count}, converting book {book}")
            multiple_project = True
        else:
            GlobalSettings.logger.debug("Single project")
            part_id = None
            multiple_project = False

        self.job.ended_at = datetime.utcnow()
        self.job.success = self.success
        for message in self.log:
            self.job.log_message(message)
        for message in self.warnings:
            self.job.warnings_message(message)
        for message in self.errors:
            self.job.error_message(message)
        if len(self.errors):
            self.job.log_message(f"{self.job.convert_module} function returned with errors.")
        elif len(self.warnings):
            self.job.log_message(f"{self.job.convert_module} function returned with warnings.")
        else:
            self.job.log_message(f"{self.job.convert_module} function returned successfully.")

        if not self.success or len(self.job.errors):
            self.job.success = False
            self.job.status = 'failed'
            message = "Conversion failed"
            GlobalSettings.logger.debug(f"Conversion failed, success: {self.success}, errors: {self.job.errors}")
        elif len(self.job.warnings) > 0:
            self.job.success = True
            self.job.status = 'warnings'
            message = "Conversion successful with warnings"
        else:
            self.job.success = True
            self.job.status = 'success'
            message = "Conversion successful"

        self.job.message = message
        self.job.log_message(message)
        self.job.log_message(f"Finished job {self.job.job_id} at {self.job.ended_at.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        s3_commit_key = f'u/{self.job.user_name}/{self.job.repo_name}/{self.job.commit_id}'
        upload_key = s3_commit_key
        if multiple_project:
            upload_key += '/' + part_id

        GlobalSettings.logger.debug(f"Callback for commit {s3_commit_key} …")

        # Download the ZIP file of the converted files
        converted_zip_url = self.job.output
        converted_zip_file = os.path.join(self.temp_dir, converted_zip_url.rpartition('/')[2])
        remove(converted_zip_file)  # make sure old file not present
        download_success = True
        GlobalSettings.logger.debug(f"Downloading converted zip file from {converted_zip_url} …")
        try:
            download_file(converted_zip_url, converted_zip_file)
        except:
            download_success = False  # if multiple project we note fail and move on
            if not multiple_project:
                remove_tree(self.temp_dir)  # cleanup
            if self.job.errors is None:
                self.job.errors = []
            message = f"Missing converted file: {converted_zip_url}"
            GlobalSettings.logger.debug(message)
            if not self.job.errors or not self.job.errors[0].startswith("No converter "):
                # Missing file error is irrelevant if no conversion was attempted
                self.job.errors.append(message)
        finally:
            GlobalSettings.logger.debug(f"download finished, success={download_success}")

        self.job.update()

        if download_success:
            # Unzip the archive
            unzip_dir = self.unzip_converted_files(converted_zip_file)

            # Upload all files to the cdn_bucket with the key of <user>/<repo_name>/<commit> of the repo
            self.upload_converted_files(upload_key, unzip_dir)

        if multiple_project:
            # Now download the existing build_log.json file, update it and upload it back to S3 as convert_log
            build_log_json = self.update_convert_log(s3_commit_key, part_id + "/")

            # mark current part as finished
            self.cdn_upload_contents({}, s3_commit_key + '/' + part_id + '/finished')

        else:  # single part conversion
            # Now download the existing build_log.json file, update it and upload it back to S3 as convert_log
            build_log_json = self.update_convert_log(s3_commit_key)

            self.cdn_upload_contents({}, s3_commit_key + '/finished')  # flag finished

        results = ClientLinterCallback.deploy_if_conversion_finished(s3_commit_key, self.identifier)
        if results:
            self.all_parts_completed = True
            build_log_json = results

        remove_tree(self.temp_dir)  # cleanup
        return build_log_json

    def unzip_converted_files(self, converted_zip_file):
        unzip_dir = tempfile.mkdtemp(prefix='unzip_', dir=self.temp_dir)
        try:
            GlobalSettings.logger.debug(f"Unzipping {converted_zip_file} …")
            unzip(converted_zip_file, unzip_dir)
        finally:
            GlobalSettings.logger.debug("finished.")

        return unzip_dir

    @staticmethod
    def upload_converted_files(s3_commit_key, unzip_dir):
        for root, dirs, files in os.walk(unzip_dir):
            for f in sorted(files):
                path = os.path.join(root, f)
                key = s3_commit_key + path.replace(unzip_dir, '')
                GlobalSettings.logger.debug(f"Uploading {f} to {key}")
                GlobalSettings.cdn_s3_handler().upload_file(path, key, cache_time=0)

    def update_convert_log(self, s3_base_key, part=''):
        build_log_json = self.get_build_log(s3_base_key, part)
        self.upload_convert_log(build_log_json, s3_base_key, part)
        return build_log_json

    def upload_convert_log(self, build_log_json, s3_base_key, part=''):
        if self.job.started_at:
            build_log_json['started_at'] = self.job.started_at.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            build_log_json['started_at'] = None
        if self.job.ended_at:
            build_log_json['ended_at'] = self.job.ended_at.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            build_log_json['ended_at'] = None
        build_log_json['success'] = self.job.success
        build_log_json['status'] = self.job.status
        build_log_json['message'] = self.job.message
        if self.job.log:
            build_log_json['log'] = self.job.log
        else:
            build_log_json['log'] = []
        if self.job.warnings:
            build_log_json['warnings'] = self.job.warnings
        else:
            build_log_json['warnings'] = []
        if self.job.errors:
            build_log_json['errors'] = self.job.errors
        else:
            build_log_json['errors'] = []
        build_log_key = self.get_build_log_key(s3_base_key, part, name='convert_log.json')
        GlobalSettings.logger.debug(f"Writing build log to {build_log_key}")
        # GlobalSettings.logger.debug('build_log contents: ' + json.dumps(build_log_json))
        self.cdn_upload_contents(build_log_json, build_log_key)
        return build_log_json

    def cdn_upload_contents(self, contents, key):
        file_name = os.path.join(self.temp_dir, 'contents.json')
        write_file(file_name, contents)
        GlobalSettings.logger.debug(f"Writing file to {key}")
        GlobalSettings.cdn_s3_handler().upload_file(file_name, key, cache_time=0)

    def get_build_log(self, s3_base_key, part=''):
        build_log_key = self.get_build_log_key(s3_base_key, part)
        # GlobalSettings.logger.debug('Reading build log from ' + build_log_key)
        build_log_json = GlobalSettings.cdn_s3_handler().get_json(build_log_key)
        # GlobalSettings.logger.debug('build_log contents: ' + json.dumps(build_log_json))
        return build_log_json

    @staticmethod
    def get_build_log_key(s3_base_key, part='', name='build_log.json'):
        upload_key = f'{s3_base_key}/{part}{name}'
        return upload_key
