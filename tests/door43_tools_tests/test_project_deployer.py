import unittest
import os
import tempfile
from shutil import rmtree

from bs4 import BeautifulSoup
from moto import mock_s3

from app_settings.app_settings import dcs_url
from preprocessors.preprocessors import TqPreprocessor
from preprocessors.preprocessors import TnPreprocessor
from door43_tools.project_deployer import ProjectDeployer
from door43_tools.td_language import TdLanguage
from general_tools import file_utils
from general_tools.file_utils import unzip
from door43_tools.bible_books import BOOK_NUMBERS


@mock_s3
class ProjectDeployerTests(unittest.TestCase):
    resources_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources')

    def setUp(self):
        """Runs before each test."""
        dcs_url(prefix=f'{self._testMethodName}-')
        dcs_url.cdn_s3_handler().create_bucket()
        dcs_url.door43_s3_handler().create_bucket()
        self.temp_dir = tempfile.mkdtemp(prefix='Door43_test_project_deployer')
        self.deployer = ProjectDeployer(self.temp_dir)
        TdLanguage.language_list = {
            'aa': TdLanguage({'gw': False, 'ld': 'ltr', 'ang': 'Afar', 'lc': 'aa', 'ln': 'Afaraf', 'lr': 'Africa',
                              'pk': 6}),
            'en': TdLanguage({'gw': True, 'ld': 'ltr', 'ang': 'English', 'lc': 'en', 'ln': 'English',
                              'lr': 'Europe', 'pk': 1747}),
            'es': TdLanguage({'gw': True, 'ld': 'ltr', 'ang': 'Spanish', 'lc': 'es', 'ln': 'espa\xf1ol',
                              'lr': 'Europe', 'pk': 1776}),
            'fr': TdLanguage({'gw': True, 'ld': 'ltr', 'ang': 'French', 'lc': 'fr',
                              'ln': 'fran\xe7ais, langue fran\xe7aise', 'lr': 'Europe', 'pk': 1868})
        }

    def tearDown(self):
        rmtree(self.temp_dir, ignore_errors=True)

    # def test_obs_download_buildlog_and_deploy_revision_to_door43(self):
    #     self.mock_s3_obs_project()
    #     build_log_key = '{0}/build_log.json'.format(self.project_key)
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)
    #     self.assertTrue(ret)
    #     self.assertTrue(AppSettings.door43_s3_handler().key_exists(build_log_key))
    #     self.assertTrue(AppSettings.door43_s3_handler().key_exists('{0}/50.html'.format(self.project_key)))

    # def test_obs_download_buildlog_and_deploy_revision_to_door43_exception(self):
    #     self.mock_s3_obs_project()
    #     build_log_key = '{0}/build_log.json'.format(self.project_key)
    #     self.deployer.run_templater = self.mock_run_templater_exception
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)
    #     self.assertFalse(ret)

    # def test_bad_download_buildlog_and_deploy_revision_to_door43(self):
    #     self.mock_s3_obs_project()
    #     bad_key = 'u/test_user/test_repo/12345678/bad_build_log.json'
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(bad_key)
    #     self.assertFalse(ret)

    # def test_tq_download_buildlog_and_deploy_revision_to_door43(self):
    #     # given
    #     self.mock_s3_tq_project()
    #     build_log_key = '{0}/build_log.json'.format(self.project_key)

    #     # when
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)

    #     # then
    #     self.assertTrue(ret)
    #     self.assertTrue(AppSettings.door43_s3_handler().key_exists(build_log_key))
    #     files_to_verify = ['manifest.yaml']
    #     for book in BOOK_NUMBERS:
    #         html_file = '{0}-{1}.html'.format(BOOK_NUMBERS[book], book.upper())
    #         files_to_verify.append(html_file)

    #     for file_name in files_to_verify:
    #         key = '{0}/{1}'.format(self.project_key, file_name)
    #         self.assertTrue(AppSettings.door43_s3_handler().key_exists(key), "Key not found: {0}".format(key))
    #     parent_key = '/'.join(self.project_key.split('/')[:-1])
    #     for file_name in ['project.json']:
    #         key = '{0}/{1}'.format(parent_key, file_name)
    #         self.assertTrue(AppSettings.door43_s3_handler().key_exists(key), "Key not found: {0}".format(key))

    # def test_tw_download_buildlog_and_deploy_revision_to_door43(self):
    #     self.mock_s3_tw_project()
    #     build_log_key = '{0}/build_log.json'.format(self.project_key)
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)
    #     self.assertTrue(ret)
    #     self.assertTrue(AppSettings.door43_s3_handler().key_exists(build_log_key))
    #     for file_name in ['index.html', 'kt.html', 'names.html', 'other.html', 'build_log.json', 'manifest.yaml']:
    #         key = '{0}/{1}'.format(self.project_key, file_name)
    #         self.assertTrue(AppSettings.door43_s3_handler().key_exists(key), "Key not found: {0}".format(key))
    #     parent_key = '/'.join(self.project_key.split('/')[:-1])
    #     for file_name in ['project.json']:
    #         key = '{0}/{1}'.format(parent_key, file_name)
    #         self.assertTrue(AppSettings.door43_s3_handler().key_exists(key), "Key not found: {0}".format(key))

    # def test_tn_download_buildlog_and_deploy_revision_to_door43(self):
    #     # given
    #     part = '1'
    #     self.mock_s3_tn_project(part)
    #     build_log_key = '{0}/{1}/build_log.json'.format(self.project_key, part)

    #     # when
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)

    #     # then
    #     self.assertTrue(ret)
    #     self.assertTrue(AppSettings.door43_s3_handler().key_exists('{0}/build_log.json'.format(self.project_key)))
    #     files_to_verify = ['01-GEN.html', 'index.json']

    #     for file_name in files_to_verify:
    #         key = '{0}/{1}'.format(self.project_key, file_name)
    #         self.assertTrue(AppSettings.door43_s3_handler().key_exists(key), "Key not found: {0}".format(key))

    # def test_bible_deploy_part_revision_to_door43(self):
    #     # given
    #     test_repo_name = 'en-ulb-4-books-multipart.zip'
    #     project_key = 'u/tx-manager-test-data/en-ulb/22f3d09f7a'
    #     self.mock_s3_bible_project(test_repo_name, project_key)
    #     part = 1
    #     build_log_key = '{0}/{1}/build_log.json'.format(self.project_key, part)
    #     output_file = '02-EXO.html'
    #     output_key = '{0}/{1}'.format(self.project_key, output_file)
    #     expect_success = True

    #     # when
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)

    #     # then
    #     self.validate_bible_results(ret, build_log_key, expect_success, output_key)

    # def test_bible_deploy_part_revision_to_door43_exception(self):
    #     # given
    #     test_repo_name = 'en-ulb-4-books-multipart.zip'
    #     project_key = 'u/tx-manager-test-data/en-ulb/22f3d09f7a'
    #     self.mock_s3_bible_project(test_repo_name, project_key)
    #     part = 1
    #     build_log_key = '{0}/{1}/build_log.json'.format(self.project_key, part)
    #     self.deployer.run_templater = self.mock_run_templater_exception

    #     # when
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)

    #     # then
    #     self.assertFalse(ret)

    # def test_bible_deploy_part_not_ready_revision_to_door43(self):
    #     # given
    #     test_repo_name = 'en-ulb-4-books-multipart.zip'
    #     project_key = 'u/tx-manager-test-data/en-ulb/22f3d09f7a'
    #     self.mock_s3_bible_project(test_repo_name, project_key)
    #     part = 0
    #     build_log_key = '{0}/{1}/build_log.json'.format(self.project_key, part)
    #     expect_success = False

    #     # when
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)

    #     # then
    #     self.validate_bible_results(ret, build_log_key, expect_success, None)

    # def test_bible_deploy_part_file_missing_revision_to_door43(self):
    #     # given
    #     test_repo_name = 'en-ulb-4-books-multipart.zip'
    #     project_key = 'u/tx-manager-test-data/en-ulb/22f3d09f7a'
    #     self.mock_s3_bible_project(test_repo_name, project_key)
    #     part = 2
    #     build_log_key = '{0}/{1}/build_log.json'.format(self.project_key, part)
    #     expect_success = True

    #     # when
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)

    #     # then
    #     self.validate_bible_results(ret, build_log_key, expect_success, None)

    # def test_bible_deploy_multi_part_merg_revision_to_door43(self):
    #     # given
    #     test_repo_name = 'en-ulb-4-books-multipart.zip'
    #     project_key = 'u/tx-manager-test-data/en-ulb/22f3d09f7a'
    #     self.mock_s3_bible_project(test_repo_name, project_key, True)
    #     self.set_deployed_flags(project_key, 4)
    #     build_log_key = '{0}/build_log.json'.format(self.project_key)
    #     expect_success = True
    #     output_file = '02-EXO.html'
    #     output_key = '{0}/{1}'.format(self.project_key, output_file)

    #     # when
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)

    #     # then
    #     self.validate_bible_results(ret, build_log_key, expect_success, output_key)

    # def test_bible_deploy_multi_part_merg_revision_to_door43_exception(self):
    #     # given
    #     test_repo_name = 'en-ulb-4-books-multipart.zip'
    #     project_key = 'u/tx-manager-test-data/en-ulb/22f3d09f7a'
    #     self.mock_s3_bible_project(test_repo_name, project_key, True)
    #     build_log_key = '{0}/build_log.json'.format(self.project_key)
    #     self.deployer.run_templater = self.mock_run_templater_exception

    #     # when
    #     ret = self.deployer.download_buildlog_and_deploy_revision_to_door43(build_log_key)

    #     # then
    #     self.assertFalse(ret)

    # def test_redeploy_all_projects(self):
    #     self.mock_s3_obs_project()
    #     AppSettings.cdn_s3_handler().put_contents('u/user1/project1/revision1/build_log.json', '{}')
    #     AppSettings.cdn_s3_handler().put_contents('u/user2/project2/revision2/build_log.json', '{}')
    #     self.assertTrue(self.deployer.redeploy_all_projects('test-door43_deployer'))

    #
    # helpers
    #

    def validate_bible_results(self, ret, build_log_key, expect_success, output_key):
        self.assertEqual(ret, expect_success)
        if expect_success:
            if output_key:
                self.assertTrue(dcs_url.door43_s3_handler().key_exists(output_key))

    def mock_run_templater_exception(self):
        raise NotImplementedError("Test Exception")

    def mock_s3_tq_project(self):
        zip_file = os.path.join(self.resources_dir, 'converted_projects', 'en_tq_converted.zip')
        out_dir = os.path.join(self.temp_dir, 'en_tq_converted')
        unzip(zip_file, out_dir)
        src_dir = os.path.join(out_dir, 'en_tq_converted')
        self.project_files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
        self.project_key = 'u/door43/en_tq/12345678'
        for filename in self.project_files:
            dcs_url.cdn_s3_handler().upload_file(os.path.join(src_dir, filename), '{0}/{1}'.format(self.project_key,
                                                                                               filename))
        dcs_url.cdn_s3_handler().upload_file(os.path.join(src_dir, 'project.json'),
                                         'u/door43/en_tq/project.json')
        dcs_url.door43_s3_handler().upload_file(os.path.join(self.resources_dir, 'templates', 'project-page.html'),
                                            'templates/project-page.html')

    def mock_s3_tw_project(self):
        zip_file = os.path.join(self.resources_dir, 'converted_projects', 'en_tw_converted.zip')
        out_dir = os.path.join(self.temp_dir, 'en_tw_converted')
        unzip(zip_file, out_dir)
        self.src_dir = src_dir = os.path.join(out_dir, 'en_tw_converted')
        self.project_files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
        self.project_key = 'u/door43/en_tw/12345678'
        for filename in self.project_files:
            dcs_url.cdn_s3_handler().upload_file(os.path.join(src_dir, filename), '{0}/{1}'.format(self.project_key,
                                                                                                   filename))
        dcs_url.cdn_s3_handler().upload_file(os.path.join(src_dir, 'project.json'),
                                         'u/door43/en_tw/project.json')
        dcs_url.door43_s3_handler().upload_file(os.path.join(self.resources_dir, 'templates', 'project-page.html'),
                                            'templates/project-page.html')

    def mock_s3_tn_project(self, part):
        zip_file = os.path.join(self.resources_dir, 'converted_projects', 'en_tn_converted.zip')
        out_dir = os.path.join(self.temp_dir, 'en_tn_converted')
        unzip(zip_file, out_dir)
        src_dir = os.path.join(out_dir, 'en_tn_converted')
        self.project_files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
        self.project_key = 'u/door43/en_tn/12345678'
        build_log = file_utils.load_json_object(os.path.join(src_dir, 'build_log.json'))
        build_log['part'] = part
        file_utils.write_file(os.path.join(src_dir, 'build_log.json'), build_log)
        dcs_url.cdn_s3_handler().upload_file(os.path.join(src_dir, 'build_log.json'),
                                         '{0}/{1}/build_log.json'.format(self.project_key, part))
        dcs_url.cdn_s3_handler().upload_file(os.path.join(src_dir, 'index.json'),
                                         '{0}/{1}/index.json'.format(self.project_key, part))
        dcs_url.cdn_s3_handler().upload_file(os.path.join(src_dir, 'build_log.json'),
                                         '{0}/{1}/finished'.format(self.project_key, part))
        dcs_url.cdn_s3_handler().upload_file(os.path.join(src_dir, '01-GEN.html'),
                                         '{0}/{1}/01-GEN.html'.format(self.project_key, part))
        dcs_url.cdn_s3_handler().upload_file(os.path.join(src_dir, 'project.json'),
                                         'u/door43/en_tq/project.json')
        dcs_url.door43_s3_handler().upload_file(os.path.join(self.resources_dir, 'templates', 'project-page.html'),
                                            'templates/project-page.html')

    def mock_s3_obs_project(self):
        zip_file = os.path.join(self.resources_dir, 'converted_projects', 'en-obs-complete.zip')
        out_dir = os.path.join(self.temp_dir, 'en-obs-complete')
        unzip(zip_file, out_dir)
        project_dir = os.path.join(out_dir, 'door43', 'en-obs', '12345678')
        self.project_files = [f for f in os.listdir(project_dir) if os.path.isfile(os.path.join(project_dir, f))]
        self.project_key = 'u/door43/en-obs/12345678'
        for filename in self.project_files:
            dcs_url.cdn_s3_handler().upload_file(os.path.join(project_dir, filename), '{0}/{1}'.format(self.project_key,
                                                                                                   filename))
        dcs_url.cdn_s3_handler().upload_file(os.path.join(out_dir, 'door43', 'en-obs', 'project.json'),
                                         'u/door43/en-obs/project.json')
        dcs_url.door43_s3_handler().upload_file(os.path.join(self.resources_dir, 'templates', 'project-page.html'),
                                            'templates/project-page.html')

    def set_deployed_flags(self, project_key, part_count, skip=-1):
        tempf = tempfile.mktemp(prefix="temp", suffix="deployed")
        file_utils.write_file(tempf, ' ')
        for i in range(0, part_count):
            if i != skip:
                key = '{0}/{1}/deployed'.format(project_key, i)
                dcs_url.cdn_s3_handler().upload_file(tempf, key, cache_time=0)
        os.remove(tempf)

    def mock_s3_bible_project(self, test_file_name, project_key, multi_part=False):
        converted_proj_dir = os.path.join(self.resources_dir, 'converted_projects')
        test_file_base = test_file_name.split('.zip')[0]
        zip_file = os.path.join(converted_proj_dir, test_file_name)
        out_dir = os.path.join(self.temp_dir, test_file_base)
        unzip(zip_file, out_dir)
        project_dir = os.path.join(out_dir, test_file_base) + os.path.sep
        self.project_files = file_utils.get_files(out_dir)
        self.project_key = project_key
        for filename in self.project_files:
            sub_path = filename.split(project_dir)[1].replace(os.path.sep, '/')  # Make sure it is a bucket path
            dcs_url.cdn_s3_handler().upload_file(filename, '{0}/{1}'.format(project_key, sub_path))

            if multi_part:  # copy files from cdn to door43
                base_name = os.path.basename(filename)
                if '.html' in base_name:
                    with open(filename, 'r') as f:
                        soup = BeautifulSoup(f, 'html.parser')

                    # add nav tag
                    new_tag = soup.new_tag('div', id='right-sidebar')
                    soup.body.append(new_tag)
                    html = str(soup)
                    file_utils.write_file(filename, html.encode('ascii', 'xmlcharrefreplace'))

                dcs_url.door43_s3_handler().upload_file(filename, '{0}/{1}'.format(project_key, base_name))

        # u, user, repo = project_key
        dcs_url.door43_s3_handler().upload_file(os.path.join(self.resources_dir, 'templates', 'project-page.html'),
                                          'templates/project-page.html')
