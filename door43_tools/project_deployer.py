import os
import tempfile
import json
import time
import traceback
from glob import glob
from shutil import copyfile
from datetime import datetime, timedelta

from rq_settings import prefix, debug_mode_flag
from global_settings.global_settings import GlobalSettings
from general_tools import file_utils
from general_tools.file_utils import write_file, remove_tree
from door43_tools.templaters import init_template



class ProjectDeployer:
    """
    Deploys a project's revision to the door43.org bucket

    Read from the project's user dir in the cdn.door43.org bucket
    by applying the door43.org template to the raw html files
    """

    def __init__(self, unzip_dir):
        # GlobalSettings.logger.debug(f"ProjectDeployer.__init__({unzip_dir})…")
        self.unzip_dir = unzip_dir
        self.temp_dir = tempfile.mkdtemp(suffix='',
                            prefix='Door43_deployer_' + datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S_'))


    def close(self):
        """
        Delete temp files (except in debug mode)
        """
        if prefix and debug_mode_flag:
            GlobalSettings.logger.debug(f"Temp deployer folder '{self.temp_dir}' has been left on disk for debugging!")
        else:
            remove_tree(self.temp_dir)


    # def __del__(self):
    #     self.close()


    # def download_buildlog_and_deploy_revision_to_door43(self, build_log_key):
    #     """
    #     Deploys a single revision of a project to door43.org
    #     :param string build_log_key:
    #     :return bool:

    #     Was used by Lambda function
    #         but now only called by test routines.
    #     """
    #     build_log = None
    #     try:
    #         build_log = GlobalSettings.cdn_s3_handler().get_json(build_log_key, catch_exception=False)
    #     except Exception as e:
    #         GlobalSettings.logger.debug(f"Deploying error could not access {build_log_key}: {e}")
    #         pass

    #     if not build_log or 'commit_id' not in build_log or 'repo_owner' not in build_log \
    #             or 'repo_name' not in build_log:
    #         GlobalSettings.logger.debug(f"Exiting, Invalid build log at {build_log_key}: {build_log}")
    #         return False

    #     return self.deploy_revision_to_door43(build_log)


    def deploy_revision_to_door43(self, build_log):
        """
        Deploys a single revision of a project to door43.org
        :param dict build_log:
        :return bool:
        """
        start = time.time()
        GlobalSettings.logger.debug(f"Deploying, build log: {json.dumps(build_log)[:256]} …")

        user = build_log['repo_owner']
        repo_name = build_log['repo_name']
        commit_id = build_log['commit_id'][:10]

        s3_commit_key = f'u/{user}/{repo_name}/{commit_id}'
        s3_repo_key = f'u/{user}/{repo_name}'
        # download_key = s3_commit_key

        do_part_template_only = False
        do_multipart_merge = False
        assert 'multiple' not in build_log
        assert 'part' not in build_log
        # if 'multiple' in build_log:
        #     do_multipart_merge = build_log['multiple']
        #     GlobalSettings.logger.debug(f"Found multi-part merge: {download_key}")

        #     prefix = download_key + '/'
        #     undeployed = self.get_undeployed_parts(prefix)
        #     if len(undeployed) > 0:
        #         GlobalSettings.logger.debug(f"Exiting, Parts not yet deployed: {undeployed}")
        #         return False

        #     key_deployed_ = download_key + '/final_deployed'
        #     if GlobalSettings.cdn_s3_handler().key_exists(key_deployed_):
        #         GlobalSettings.logger.debug(f"Exiting, Already merged parts: {download_key}")
        #         return False
        #     self.write_data_to_file(self.temp_dir, key_deployed_, 'final_deployed', ' ')  # flag that deploy has begun
        #     GlobalSettings.logger.debug(f"Continuing with merge: {download_key}")

        # elif 'part' in build_log:
        #     part = build_log['part']
        #     download_key += '/' + part
        #     do_part_template_only = True
        #     GlobalSettings.logger.debug(f"Found partial: {download_key}")

        #     if not GlobalSettings.cdn_s3_handler().key_exists(download_key + '/finished'):
        #         GlobalSettings.logger.debug("Exiting, Not ready to process partial")
        #         return False

        source_dir = tempfile.mkdtemp(prefix='source_', dir=self.temp_dir)
        output_dir = tempfile.mkdtemp(prefix='output_', dir=self.temp_dir)
        template_dir = tempfile.mkdtemp(prefix='template_', dir=self.temp_dir)

        resource_type = build_log['resource_type']
        template_key = 'templates/project-page.html'
        template_file = os.path.join(template_dir, 'project-page.html')
        GlobalSettings.logger.debug(f"Downloading {template_key} to {template_file} …")
        GlobalSettings.door43_s3_handler().download_file(template_key, template_file)

        if not do_multipart_merge:
            source_dir, success = self.template_converted_files(build_log, output_dir, repo_name,
                                                                resource_type, s3_commit_key, source_dir, start,
                                                                template_file)
            if not success:
                return False
        # else:
        #     source_dir, success = self.multipart_master_merge(s3_commit_key, resource_type, download_key, output_dir,
        #                                                       source_dir, start, template_file)
        #     if not success:
        #         return False

        #######################
        #
        #  Now do the deploy
        #
        #######################

        if not do_part_template_only or do_multipart_merge:
            # Copy first HTML file to index.html if index.html doesn't exist
            html_files = sorted(glob(os.path.join(output_dir, '*.html')))
            index_file = os.path.join(output_dir, 'index.html')
            if len(html_files) > 0 and not os.path.isfile(index_file):
                copyfile(os.path.join(output_dir, html_files[0]), index_file)

        # Copy all other files over that don't already exist in output_dir, like css files
        for filename in sorted(glob(os.path.join(source_dir, '*'))):
            output_file = os.path.join(output_dir, os.path.basename(filename))
            if not os.path.exists(output_file) and not os.path.isdir(filename):
                copyfile(filename, output_file)

            # if do_part_template_only:  # move files to common area
            #     basename = os.path.basename(filename)
            #     if basename not in ['finished', 'build_log.json', 'index.html', 'merged.json', 'lint_log.json']:
            #         GlobalSettings.logger.debug(f"Moving {basename} to common area…")
            #         GlobalSettings.cdn_s3_handler().upload_file(filename, s3_commit_key + '/' + basename, cache_time=0)
            #         GlobalSettings.cdn_s3_handler().delete_file(download_key + '/' + basename)

        # Save master build_log.json
        build_log['ended_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        file_utils.write_file(os.path.join(output_dir, 'build_log.json'), build_log)
        GlobalSettings.logger.debug(f"Final build_log.json: {json.dumps(build_log)[:256]} …")

        # Upload all files to the door43.org bucket
        GlobalSettings.logger.info(f"Uploading all files to the website bucket: {GlobalSettings.door43_bucket_name} …")
        for root, dirs, files in os.walk(output_dir):
            for filename in sorted(files):
                filepath = os.path.join(root, filename)
                if os.path.isdir(filepath):
                    continue
                key = s3_commit_key + filepath.replace(output_dir, '').replace(os.path.sep, '/')
                GlobalSettings.logger.debug(f"Uploading {filename} to {key} …")
                GlobalSettings.door43_s3_handler().upload_file(filepath, key, cache_time=0)

        # if not do_part_template_only:
        # Now we place json files and redirect index.html for the whole repo to this index.html file
        try:
            GlobalSettings.door43_s3_handler().copy(from_key=f'{s3_repo_key}/project.json', from_bucket=GlobalSettings.cdn_bucket_name)
            GlobalSettings.door43_s3_handler().copy(from_key=f'{s3_commit_key}/manifest.json',
                                            to_key=f'{s3_repo_key}/manifest.json')
            GlobalSettings.door43_s3_handler().redirect(s3_repo_key, '/' + s3_commit_key)
            GlobalSettings.door43_s3_handler().redirect(s3_repo_key + '/index.html', '/' + s3_commit_key)
            self.write_data_to_file(output_dir, s3_commit_key, 'deployed', ' ')  # flag that deploy has finished
        except:
            pass

        # else:  # if processing part of multi-part merge
        #     self.write_data_to_file(output_dir, download_key, 'deployed', ' ')  # flag that deploy has finished
        #     if GlobalSettings.cdn_s3_handler().key_exists(s3_commit_key + '/final_build_log.json'):
        #         GlobalSettings.logger.debug("final build detected")
        #         GlobalSettings.logger.debug("conversions all finished, trigger final merge")
        #         GlobalSettings.cdn_s3_handler().copy(from_key=s3_commit_key + '/final_build_log.json',
        #                                   to_key=s3_commit_key + '/build_log.json')

        elapsed_seconds = int(time.time() - start)
        GlobalSettings.logger.debug(f"Deploy type partial={do_part_template_only}, multi_merge={do_multipart_merge}")
        GlobalSettings.logger.debug(f"Deploy completed in {elapsed_seconds} seconds.")
        self.close()
        return True


    # def multipart_master_merge(self, s3_commit_key, resource_type, download_key, output_dir, source_dir, start,
    #                            template_file):
    #     prefix = download_key + '/'
    #     GlobalSettings.door43_s3_handler().download_dir(prefix, source_dir)  # get previous templated files
    #     source_dir = os.path.join(source_dir, download_key)
    #     files = sorted(glob(os.path.join(source_dir, '*.*')))
    #     for f in files:
    #         GlobalSettings.logger.debug("Downloaded: " + f)
    #     fname = os.path.join(source_dir, 'index.html')
    #     if os.path.isfile(fname):
    #         os.remove(fname)  # remove index if already exists
    #     elapsed_seconds = int(time.time() - start)
    #     GlobalSettings.logger.debug("deploy download completed in " + str(elapsed_seconds) + " seconds")
    #     templater = init_template(resource_type, source_dir, output_dir, template_file)
    #     # restore index from previous passes
    #     index_json = self.get_templater_index(s3_commit_key, 'index.json')
    #     templater.titles = index_json['titles']
    #     templater.chapters = index_json['chapters']
    #     templater.book_codes = index_json['book_codes']
    #     templater.already_converted = templater.files  # do not reconvert files
    #     # merge the source files with the template
    #     try:
    #         self.run_templater(templater)
    #         success = True
    #     # except Exception as e:
    #     except:
    #         GlobalSettings.logger.error(f"Error multi-part applying template {template_file} to resource type {resource_type}")
    #         self.close()
    #         success = False
    #     return source_dir, success


    # def get_undeployed_parts(self, prefix):
    #     unfinished = []
    #     for o in GlobalSettings.cdn_s3_handler().get_objects(prefix=prefix, suffix='/build_log.json'):
    #         parts = o.key.split(prefix)
    #         if len(parts) == 2:
    #             parts = parts[1].split('/')
    #             if len(parts) > 1:
    #                 part_num = parts[0]
    #                 deployed_key = prefix + part_num + '/deployed'
    #                 if not GlobalSettings.cdn_s3_handler().key_exists(deployed_key):
    #                     GlobalSettings.logger.debug(f"Part {part_num} unfinished")
    #                     unfinished.append(part_num)
    #     return unfinished


    def template_converted_files(self, build_log, output_dir, repo_name, resource_type, s3_commit_key,
                                 source_dir, start_time, template_filepath):
        GlobalSettings.logger.info(f"template_converted_files({build_log}, od={output_dir}, '{repo_name}'," \
                                   f" '{resource_type}', k={s3_commit_key}, sd={source_dir}," \
                                   f" {start_time}, tf={template_filepath}) with {self.unzip_dir}…")
        assert 'errors' in build_log
        assert 'message' in build_log
        assert repo_name
        # GlobalSettings.cdn_s3_handler().download_dir(download_key + '/', source_dir)
        # source_dir = os.path.join(source_dir, download_key.replace('/', os.path.sep))
        # elapsed_seconds = int(time.time() - start_time)
        # GlobalSettings.logger.debug(f"Deploy download completed in {elapsed_seconds} seconds")
        source_dir = self.unzip_dir
        html_files = sorted(glob(os.path.join(source_dir, '*.html')))
        if len(html_files) < 1:
            GlobalSettings.logger.warning("No html files found by ProjectDeployer.template_converted_files!")
            content = ""
            if build_log['errors']:
                content += """
                        <div style="text-align:center;margin-bottom:20px">
                            <i class="fa fa-times-circle-o" style="font-size: 250px;font-weight: 300;color: red"></i>
                            <br/>
                            <h2>Critical!</h2>
                            <h3>Here is what went wrong with this build:</h3>
                        </div>
                    """
                content += '<div><ul><li>' + '</li><li>'.join(build_log['errors']) + '</li></ul></div>'
            else:
                content += f'<h1 class="conversion-requested">{build_log["message"]}</h1>'
                content += f'<p><i>No content is available to show for {repo_name} yet.</i></p>'
            html = f"""
                    <html lang="en">
                        <head>
                            <title>{repo_name}</title>
                        </head>
                        <body>
                            <div id="content">{content}</div>
                        </body>
                    </html>"""
            repo_index_file = os.path.join(source_dir, 'index.html')
            write_file(repo_index_file, html)

        # Merge the source files with the template
        templater = init_template(resource_type, source_dir, output_dir, template_filepath)
        try:
            self.run_templater(templater)
            success = True
        except Exception as e:
            GlobalSettings.logger.error(f"Error applying template {template_filepath} to resource type {resource_type}:")
            GlobalSettings.logger.error(f'{e}: {traceback.format_exc()}')
            self.close()
            success = False

        if success:
            # Update index of templated files
            index_json_fname = 'index.json'
            index_json = self.get_templater_index(s3_commit_key, index_json_fname)
            GlobalSettings.logger.debug("initial 'index.json': " + json.dumps(index_json)[:256])
            self.update_index_key(index_json, templater, 'titles')
            self.update_index_key(index_json, templater, 'chapters')
            self.update_index_key(index_json, templater, 'book_codes')
            GlobalSettings.logger.debug("final 'index.json': " + json.dumps(index_json)[:256])
            self.write_data_to_file(output_dir, s3_commit_key, index_json_fname, index_json)
        return source_dir, success


    def write_data_to_file(self, output_dir, s3_commit_key, fname, data):
        out_file = os.path.join(output_dir, fname)
        write_file(out_file, data)
        key = s3_commit_key + '/' + fname
        GlobalSettings.logger.debug(f"Writing {fname} to {key} …")
        GlobalSettings.cdn_s3_handler().upload_file(out_file, key, cache_time=0)


    def run_templater(self, templater):  # for test purposes
        templater.run()


    @staticmethod
    def update_index_key(index_json, templater, key):
        data = index_json[key]
        data.update(getattr(templater, key))
        index_json[key] = data


    @staticmethod
    def get_templater_index(s3_commit_key, index_json_fname):
        index_json = GlobalSettings.cdn_s3_handler().get_json(s3_commit_key + '/' + index_json_fname)
        if not index_json:
            index_json['titles'] = {}
            index_json['chapters'] = {}
            index_json['book_codes'] = {}
        return index_json
