import os
import tempfile
import unittest
import shutil

from resource_container.ResourceContainer import RC
from preprocessors.preprocessors import do_preprocess, TnPreprocessor
from general_tools.file_utils import unzip, read_file


class TestTnPreprocessor(unittest.TestCase):

    resources_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources')

    def setUp(self):
        """Runs before each test."""
        self.out_dir = ''
        self.temp_dir = ""

    def tearDown(self):
        """Runs after each test."""
        # delete temp files
        if os.path.isdir(self.out_dir):
            shutil.rmtree(self.out_dir, ignore_errors=True)
        if os.path.isdir(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    @unittest.skip("Skip test for time reasons - takes too long for automated tests - leave for standalone testing")
    def test_tn_preprocessor_long(self):
        # given
        repo_name = 'en_tn'
        file_name = os.path.join('raw_sources', repo_name + '.zip')
        rc, repo_dir, self.temp_dir = self.extractFiles(file_name, repo_name)
        repo_dir = os.path.join(repo_dir)
        self.out_dir = tempfile.mkdtemp(prefix='Door43_test_output_')
        repo_name = 'dummy_repo'

        # when
        do_preprocess('Translation_Notes', 'dummyOwner', 'dummyURL', rc, repo_dir, self.out_dir)

        # then
        # self.assertTrue(preproc.is_multiple_jobs())
        # self.assertEqual(len(preproc.get_book_list()), 66)
        self.assertTrue(os.path.isfile(os.path.join(self.out_dir, 'index.json')))
        self.assertTrue(os.path.isfile(os.path.join(self.out_dir, '01-GEN.md')))
        self.assertTrue(os.path.isfile(os.path.join(self.out_dir, '67-REV.md')))
        gen = read_file(os.path.join(self.out_dir, '01-GEN.md'))
        self.assertGreater(len(gen), 1000)
        rev = read_file(os.path.join(self.out_dir, '67-REV.md'))
        self.assertGreater(len(rev), 1000)

    def test_tn_preprocessor_short(self):
        # given
        repo_name = 'en_tn_2books'
        file_name = os.path.join('raw_sources', repo_name + '.zip')
        rc, repo_dir, self.temp_dir = self.extractFiles(file_name, repo_name)
        repo_dir = os.path.join(repo_dir)
        self.out_dir = tempfile.mkdtemp(prefix='Door43_test_output_')
        # repo_name = 'dummy_repo'

        # when
        do_preprocess('Translation_Notes', 'dummyOwner', 'dummyURL', rc, repo_dir, self.out_dir)

        # then
        # self.assertTrue(preproc.is_multiple_jobs())
        # self.assertEqual(len(preproc.get_book_list()), 2)
        self.assertTrue(os.path.isfile(os.path.join(self.out_dir, 'index.json')))
        self.assertFalse(os.path.isfile(os.path.join(self.out_dir, '01-GEN.md')))
        self.assertFalse(os.path.isfile(os.path.join(self.out_dir, '67-REV.md')))
        self.assertTrue(os.path.isfile(os.path.join(self.out_dir, '02-EXO.md')))
        self.assertTrue(os.path.isfile(os.path.join(self.out_dir, '03-LEV.md')))
        read_file(os.path.join(self.out_dir, 'index.json'))
        exo = read_file(os.path.join(self.out_dir, '02-EXO.md'))
        self.assertGreater(len(exo), 1000)
        lev = read_file(os.path.join(self.out_dir, '03-LEV.md'))
        self.assertGreater(len(lev), 1000)

    def test_tn_preprocessor_short_tsv7(self):
        # given
        repo_name = 'en_tn_2books_tsv7'
        file_name = os.path.join('raw_sources', repo_name + '.zip')
        rc, repo_dir, self.temp_dir = self.extractFiles(file_name, repo_name)
        repo_dir = os.path.join(repo_dir)
        self.out_dir = tempfile.mkdtemp(prefix='Door43_test_output_')
        # repo_name = 'dummy_repo'

        # when
        do_preprocess('Translation_Notes', 'dummyOwner', 'dummyURL', rc, repo_dir, self.out_dir)

        # then
        # self.assertTrue(preproc.is_multiple_jobs())
        # self.assertEqual(len(preproc.get_book_list()), 2)
        self.assertTrue(os.path.isfile(os.path.join(self.out_dir, 'index.json')))
        self.assertFalse(os.path.isfile(os.path.join(self.out_dir, '01-GEN.md')))
        self.assertFalse(os.path.isfile(os.path.join(self.out_dir, '67-REV.md')))
        self.assertTrue(os.path.isfile(os.path.join(self.out_dir, '02-EXO.md')))
        self.assertTrue(os.path.isfile(os.path.join(self.out_dir, '03-LEV.md')))
        read_file(os.path.join(self.out_dir, 'index.json'))
        exo = read_file(os.path.join(self.out_dir, '02-EXO.md'))
        self.assertGreater(len(exo), 1000)
        lev = read_file(os.path.join(self.out_dir, '03-LEV.md'))
        self.assertGreater(len(lev), 1000)

    def test_tn_links(self):
        repo_name = 'en_tn_2books'
        file_name = os.path.join('raw_sources', repo_name + '.zip')
        rc, _repo_dir, self.temp_dir = self.extractFiles(file_name, repo_name)
        # repo_dir = os.path.join(repo_dir)
        self.out_dir = tempfile.mkdtemp(prefix='Door43_test_output_')

        language_code = 'pq-xy'
        repo_owner = 'dummyOwner'
        tn_preprocessor = TnPreprocessor(commit_url=None, rc=rc, repo_owner=repo_owner, source_dir=None, output_dir=self.out_dir)
        for given_input, expected_output in (
            ('Some random string', 'Some random string'),
            ('rc://*/ta/man/translate/figs-euphemism',
                f'https://git.door43.org/{repo_owner}/{language_code}_ta/src/branch/master/translate/figs-euphemism/01.md'),
            ('rc://*/ta/man/translate/figs-abstractnouns',
                f'https://git.door43.org/{repo_owner}/{language_code}_ta/src/branch/master/translate/figs-abstractnouns/01.md'),
            ('rc://en/ta/man/translate/figs-euphemism',
                f'https://git.door43.org/{repo_owner}/en_ta/src/branch/master/translate/figs-euphemism/01.md'),
            ('rc://*/tn/help/1sa/16/02',
                f'https://git.door43.org/{repo_owner}/{language_code}_tn/src/branch/master/1sa/16/02.md'),
            ('rc://en/tn/help/1sa/16/02',
                f'https://git.door43.org/{repo_owner}/en_tn/src/branch/master/1sa/16/02.md'),
            ):
                actual_output = tn_preprocessor.fix_tN_links('Gen 2:3', given_input, repo_owner, language_code)
                self.assertEqual(actual_output, expected_output)

    @classmethod
    def extractFiles(cls, file_name, repo_name):
        file_path = os.path.join(TestTnPreprocessor.resources_dir, file_name)

        # 1) unzip the repo files
        temp_dir = tempfile.mkdtemp(prefix='Door43_test_repo_')
        unzip(file_path, temp_dir)
        repo_dir = os.path.join(temp_dir, repo_name)
        if not os.path.isdir(repo_dir):
            repo_dir = file_path

        # 2) Get the resource container
        rc = RC(repo_dir)

        return rc, repo_dir, temp_dir
