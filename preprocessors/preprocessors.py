# Python imports
from typing import Dict, List, Tuple, Any, Optional
import os
import re
import json
from glob import glob
from shutil import copy, copytree
from urllib.request import urlopen
from urllib.error import HTTPError

# Local imports
from rq_settings import prefix, debug_mode_flag
from app_settings.app_settings import AppSettings
from door43_tools.bible_books import BOOK_NUMBERS, BOOK_NAMES, BOOK_CHAPTER_VERSES
from general_tools.file_utils import write_file, read_file, make_dir
from general_tools.url_utils import get_url
from resource_container.ResourceContainer import RC
from preprocessors.converters import txt2md



def do_preprocess(repo_subject:str, repo_owner:str, commit_url:str, rc:RC,
                                        repo_dir:str, output_dir:str) -> Tuple[int, List[str]]:
    if repo_subject == 'Open_Bible_Stories':
        AppSettings.logger.info(f"do_preprocess: using ObsPreprocessor for '{repo_subject}'…")
        preprocessor = ObsPreprocessor(commit_url, rc, repo_owner, repo_dir, output_dir)
    elif repo_subject in ('OBS_Study_Notes','OBS_Study_Questions',
                            'OBS_Translation_Notes','OBS_Translation_Questions'):
        AppSettings.logger.info(f"do_preprocess: using ObsNotesPreprocessor for '{repo_subject}'…")
        preprocessor = ObsNotesPreprocessor(commit_url, rc, repo_owner, repo_dir, output_dir)
    elif repo_subject in ('Bible','Aligned_Bible', 'Greek_New_Testament','Hebrew_Old_Testament'):
        AppSettings.logger.info(f"do_preprocess: using BiblePreprocessor for '{repo_subject}'…")
        preprocessor = BiblePreprocessor(commit_url, rc, repo_owner, repo_dir, output_dir)
    elif repo_subject == 'Translation_Academy':
        AppSettings.logger.info(f"do_preprocess: using TaPreprocessor for '{repo_subject}'…")
        preprocessor = TaPreprocessor(commit_url, rc, repo_owner, repo_dir, output_dir)
    elif repo_subject == 'Translation_Questions':
        AppSettings.logger.info(f"do_preprocess: using TqPreprocessor for '{repo_subject}'…")
        preprocessor = TqPreprocessor(commit_url, rc, repo_owner, repo_dir, output_dir)
    elif repo_subject == 'Translation_Words':
        AppSettings.logger.info(f"do_preprocess: using TwPreprocessor for '{repo_subject}'…")
        preprocessor = TwPreprocessor(commit_url, rc, repo_owner, repo_dir, output_dir)
    elif repo_subject in ('Translation_Notes', 'TSV_Translation_Notes'):
        AppSettings.logger.info(f"do_preprocess: using TnPreprocessor for '{repo_subject}'…")
        preprocessor = TnPreprocessor(commit_url, rc, repo_owner, repo_dir, output_dir)
    elif repo_subject in ('Greek_Lexicon','Hebrew-Aramaic_Lexicon'):
        AppSettings.logger.info(f"do_preprocess: using LexiconPreprocessor for '{repo_subject}'…")
        preprocessor = LexiconPreprocessor(commit_url, rc, repo_owner, repo_dir, output_dir)
    else:
        AppSettings.logger.warning(f"do_preprocess: using generic Preprocessor for '{repo_subject}' resource: {rc.resource.identifier} …")
        preprocessor = Preprocessor(commit_url, rc, repo_owner, repo_dir, output_dir)
    return preprocessor.run()
# end of do_preprocess()



class Preprocessor:
    # NOTE: Both of these lists are used for CASE-SENSITIVE comparisons
    ignoreDirectories = ['.apps', '.git', '.github', '00']
    ignoreFiles = ['.DS_Store', 'reference.txt', 'title.txt', 'LICENSE.md', 'README.md', 'README.rst']


    def __init__(self, commit_url:str, rc:RC, repo_owner:str, source_dir:str, output_dir:str) -> None:
        """
        :param URLString commit_url:    URL of this commit on DCS -- used for fixing links
        :param RC rc:
        :param string source_dir:
        :param string output_dir:
        """
        self.commit_url = commit_url
        self.rc = rc
        self.repo_owner = repo_owner
        self.source_dir = source_dir  # Local directory
        self.output_dir = output_dir  # Local directory
        self.num_files_written = 0
        self.warnings:List[str] = []

        # Check that we had a manifest (or equivalent) file
        # found_manifest = False
        # for some_filename in ('manifest.yaml','manifest.json','package.json','project.json','meta.json',):
        #     if os.path.isfile(os.path.join(source_dir,some_filename)):
        #         found_manifest = True; break
        # if not found_manifest:
        if not self.rc.loadeded_manifest_file:
            self.warnings.append("Possible missing manifest file in project folder")

        # Write out the new manifest file based on the resource container
        write_file(os.path.join(self.output_dir, 'manifest.yaml'), self.rc.as_dict())


    def run(self) -> Tuple[int, List[str]]:
        """
        Default Preprocessor

        Case #1: Project path is a file, then we copy the file over to the output dir
        Case #2: It's a directory of files, so we copy them over to the output directory
        Case #3: The project path is multiple chapters, so we piece them together
        """
        AppSettings.logger.debug(f"Default preprocessor starting with {self.source_dir} = {os.listdir(self.source_dir)} …")
        for idx, project in enumerate(self.rc.projects):
            project_path = os.path.join(self.source_dir, project.path)

            if os.path.isfile(project_path):
                # Case #1: Project path is a file, then we copy the file over to the output dir
                AppSettings.logger.debug(f"Default preprocessor case #1: Copying single file for '{project.identifier}' …")
                if project.identifier.lower() in BOOK_NUMBERS:
                    filename = f'{BOOK_NUMBERS[project.identifier.lower()]}-{project.identifier.upper()}.{self.rc.resource.file_ext}'
                else:
                    filename = f'{str(idx + 1).zfill(2)}-{project.identifier}.{self.rc.resource.file_ext}'
                copy(project_path, os.path.join(self.output_dir, filename))
                self.num_files_written += 1
            else:
                # Case #2: It's a directory of files, so we copy them over to the output directory
                AppSettings.logger.debug(f"Default preprocessor case #2: Copying files for '{project.identifier}' …")
                files = glob(os.path.join(project_path, f'*.{self.rc.resource.file_ext}'))
                if files:
                    for file_path in files:
                        output_file_path = os.path.join(self.output_dir, os.path.basename(file_path))
                        if os.path.isfile(file_path) and not os.path.exists(output_file_path) \
                                and os.path.basename(file_path) not in self.ignoreFiles:
                            copy(file_path, output_file_path)
                            self.num_files_written += 1
                else:
                    # Case #3: The project path is multiple chapters, so we piece them together
                    AppSettings.logger.debug(f"Default preprocessor case #3: piecing together chapters for '{project.identifier}' …")
                    chapters = self.rc.chapters(project.identifier)
                    AppSettings.logger.debug(f"Merging chapters in '{project.identifier}' …")
                    if chapters:
                        text = ''
                        for chapter in chapters:
                            text = self.mark_chapter(project.identifier, chapter, text)
                            for chunk in self.rc.chunks(project.identifier, chapter):
                                text = self.mark_chunk(project.identifier, chapter, chunk, text)
                                text += read_file(os.path.join(project_path, chapter, chunk))+"\n\n"
                        if project.identifier.lower() in BOOK_NUMBERS:
                            filename = f'{BOOK_NUMBERS[project.identifier.lower()]}-{project.identifier.upper()}.{self.rc.resource.file_ext}'
                        else:
                            filename = f'{str(idx+1).zfill(2)}-{project.identifier}.{self.rc.resource.file_ext}'
                        write_file(os.path.join(self.output_dir, filename), text)
                        self.num_files_written += 1
        if self.num_files_written == 0:
            AppSettings.logger.error(f"Default preprocessor didn't write any files")
            self.warnings.append("No source files discovered")
        else:
            AppSettings.logger.debug(f"Default preprocessor wrote {self.num_files_written} files with {len(self.warnings)} warnings")
        AppSettings.logger.debug(f"Default preprocessor returning with {self.output_dir} = {os.listdir(self.output_dir)}")
        return self.num_files_written, self.warnings
    # end of DefaultPreprocessor run()


    def mark_chapter(self, ident:int, chapter:str, text:str) -> str:
        return text  # default does nothing to text


    def mark_chunk(self, ident:int, chapter:str, chunk:str, text:str) -> str:
        return text  # default does nothing to text


    def is_multiple_jobs(self) -> bool:
        return False


    def get_book_list(self):
        return None
# end of Preprocessor class



class ObsPreprocessor(Preprocessor):
    # def __init__(self, *args, **kwargs) -> None:
    #     super(ObsPreprocessor, self).__init__(*args, **kwargs)

    @staticmethod
    def get_chapters(project_path:str) -> List[Dict[str,Any]]:
        chapters:List[Dict[str,Any]] = []
        for chapter in sorted(os.listdir(project_path)):
            if os.path.isdir(os.path.join(project_path, chapter)) and chapter not in ObsPreprocessor.ignoreDirectories:
                chapters.append({
                    'id': chapter,
                    'title': ObsPreprocessor.get_chapter_title(project_path, chapter),
                    'reference': ObsPreprocessor.get_chapter_reference(project_path, chapter),
                    'frames': ObsPreprocessor.get_chapter_frames(project_path, chapter)
                })
        return chapters


    @staticmethod
    def get_chapter_title(project_path:str, chapter) -> str:
        """
        Get a chapter title.
        if the title file does not exist, it will hand back the number with a period only.
        """
        title_file = os.path.join(project_path, chapter, 'title.txt')
        if os.path.exists(title_file):
            contents = read_file(title_file)
            title = contents.strip()
        else:
            title = chapter.lstrip('0') + '. '
        return title


    @staticmethod
    def get_chapter_reference(project_path:str, chapter:str) -> str:
        """Get the chapters reference text"""
        reference_file = os.path.join(project_path, chapter, 'reference.txt')
        reference = ''
        if os.path.exists(reference_file):
            contents = read_file(reference_file)
            reference = contents.strip()
        return reference


    @staticmethod
    def get_chapter_frames(project_path:str, chapter:str) -> List[Dict[str,Any]]:
        frames:List[Dict[str,Any]] = []
        chapter_dir = os.path.join(project_path, chapter)
        for frame in sorted(os.listdir(chapter_dir)):
            if frame not in ObsPreprocessor.ignoreFiles:
                text = read_file(os.path.join(project_path, chapter, frame))
                frames.append({
                    'id': chapter + '-' + frame.strip('.txt'),
                    'text': text
                })
        return frames


    def is_chunked(self, project) -> bool:
        chapters = self.rc.chapters(project.identifier)
        if chapters and len(chapters):
            chunks = self.rc.chunks(project.identifier, chapters[0])
            for chunk in chunks:
                if os.path.basename(chunk) in ['title.txt', 'reference.txt', '01.txt']:
                    return True
        return False


    def run(self) -> Tuple[int, List[str]]:
        AppSettings.logger.debug(f"Obs preprocessor starting with {self.source_dir} = {os.listdir(self.source_dir)} …")
        for project in self.rc.projects:
            AppSettings.logger.debug(f"OBS preprocessor: Copying markdown files for '{project.identifier}' …")
            project_path = os.path.join(self.source_dir, project.path)
            # Copy all the markdown files in the project root directory to the output directory
            for file_path in glob(os.path.join(project_path, '*.md')):
                output_file_path = os.path.join(self.output_dir, os.path.basename(file_path))
                if os.path.isfile(file_path) and not os.path.exists(output_file_path) \
                        and os.path.basename(file_path) not in self.ignoreFiles:
                    copy(file_path, output_file_path)
                    self.num_files_written += 1
            if self.is_chunked(project):
                for chapter in self.get_chapters(project_path):
                    markdown = f"# {chapter['title']}\n\n"
                    for frame in chapter['frames']:
                        markdown += f"![Frame {frame.get('id')}](https://cdn.door43.org/obs/jpg/360px/obs-en-{frame.get('id')}.jpg)\n\n"
                        markdown += frame['text'] + '\n\n'
                    markdown += f"_{chapter['reference']}_\n"
                    output_file = os.path.join(self.output_dir, f"{chapter.get('id')}.md")
                    write_file(output_file, markdown)
                    self.num_files_written += 1
            else:
                for chapter in self.rc.chapters(project.identifier):
                    f = None
                    if os.path.isfile(os.path.join(project_path, chapter, '01.md')):
                        f = os.path.join(project_path, chapter, '01.md')
                    elif os.path.isfile(os.path.join(project_path, chapter, 'intro.md')):
                        f = os.path.join(project_path, chapter, 'intro.md')
                    if f:
                        copy(f, os.path.join(self.output_dir, f'{chapter}.md'))
                        self.num_files_written += 1
        if self.num_files_written == 0:
            AppSettings.logger.error(f"OBS preprocessor didn't write any markdown files")
            self.warnings.append("No OBS source files discovered")
        else:
            AppSettings.logger.debug(f"OBS preprocessor wrote {self.num_files_written} markdown files with {len(self.warnings)} warnings")
        AppSettings.logger.debug(f"OBS preprocessor returning with {self.output_dir} = {os.listdir(self.output_dir)}")
        return self.num_files_written, self.warnings
    # end of ObsPreprocessor run()
# end of class ObsPreprocessor



class ObsNotesPreprocessor(Preprocessor):
    def __init__(self, *args, **kwargs) -> None:
        super(ObsNotesPreprocessor, self).__init__(*args, **kwargs)
        self.section_container_id = 1


    def run(self) -> Tuple[int, List[str]]:
        AppSettings.logger.debug(f"OBSNotes preprocessor starting with {self.source_dir} = {os.listdir(self.source_dir)} …")
        for project in self.rc.projects:
            AppSettings.logger.debug(f"OBSNotes preprocessor: Copying folders and files for project '{project.identifier}' …")
            content_folder_path = os.path.join(self.source_dir, 'content/')
            if not os.path.isdir(content_folder_path):
                self.warnings.append(f"Unable to find 'contents/' folder for '{project.identifier}'")
                continue
            for story_number in range(0, 50+1): # Includes optional story "0" = introduction
                story_number_string = str(story_number).zfill(2)
                story_folder_path = os.path.join(content_folder_path, f'{story_number_string}/')
                markdown = toc_contents = ""
                if os.path.isdir(story_folder_path):
                    AppSettings.logger.debug(f"Story {story_number}/ found {story_folder_path}")
                    for filename in sorted(os.listdir(story_folder_path)):
                        story_filepath = os.path.join(story_folder_path, filename)
                        if filename.endswith('.md'):
                            story_file_contents = read_file(story_filepath)
                            file_basename = filename[:-3] # Remove the .md
                            markdown += f'\n# <a id="{story_number_string}-{file_basename}"/> {story_number_string}-{file_basename}\n\n{story_file_contents}\n\n'
                            toc_contents += f'  - title: "{story_number_string}-{file_basename}"\n    link: {story_number_string}-{file_basename}\n\n'
                        else:
                            self.warnings.append(f"Unexpected '{filename}' file in 'content/{story_number_string}/'")
                else: # no content/{story_number_string}/ folder
                    story_filepath = os.path.join(content_folder_path, f'{story_number_string}.md')
                    if os.path.isfile(story_filepath):
                        AppSettings.logger.debug(f"Story {story_number}/ found {story_filepath}")
                        markdown = read_file(story_filepath)
                        title = story_number_string # default
                        if markdown:
                            title = markdown.split('\n',1)[0] # Get the first line
                            if title.startswith("# "): title = title[2:] # Get rid of leading hash
                        print(f"{title=}")
                        # toc_contents += f'  - title: "{title}"\n    link: {story_number_string}\n\n'
                    elif story_number != 0:
                        self.warnings.append(f"Unable to find story {story_number_string} text")
                if markdown:
                    # rc_count = markdown.count('rc://')
                    # if rc_count: print(f"Story number {story_number_string} has {rc_count} 'rc://' links")
                    # double_bracket_count = markdown.count('[[')
                    # if double_bracket_count: print(f"Story number {story_number_string} has {double_bracket_count} '[[…]]' links")
                    markdown = self.fix_links(markdown, self.repo_owner)
                    rc_count = markdown.count('rc://')
                    if rc_count:
                        AppSettings.logger.error(f"Story number {story_number_string} still has {rc_count} 'rc://' links!")
                    write_file(os.path.join(self.output_dir,f'{story_number_string}.md'), markdown)
                    self.num_files_written += 1

                # OBSNotes: Create a toc.yaml file and write it to output dir so they can be used to
                #       generate the ToC on door43.org
                if toc_contents:
                    toc_contents = "title: Table of Contents\nsections:\n" + toc_contents
                    toc_filepath = os.path.join(self.output_dir, f'{story_number_string}-toc.yaml')
                    write_file(toc_filepath,toc_contents)

        if self.num_files_written == 0:
            AppSettings.logger.error("OBSNotes preprocessor didn't write any markdown files")
            self.warnings.append("No OBSNotes source files discovered")
        else:
            AppSettings.logger.debug(f"OBSNotes preprocessor wrote {self.num_files_written} markdown files with {len(self.warnings)} warnings")
        AppSettings.logger.debug(f"OBSNotes preprocessor returning with {self.output_dir} = {os.listdir(self.output_dir)}")
        return self.num_files_written, self.warnings
    # end of ObsNotesPreprocessor run()


    def fix_links(self, content:str, repo_owner:str) -> str:
        """
        OBS Translation Notes contain links to translationAcademy

        (OBS Translation Questions don't seem to have any links)
        """
        # convert tA RC links, e.g. rc://en/ta/man/translate/figs-euphemism
        #   => https://git.door43.org/{repo_owner}/en_ta/translate/figs-euphemism/01.md
        content = re.sub(r'rc://([^/]+)/ta/([^/]+)/([^\s)\]\n$]+)',
                         rf'https://git.door43.org/{repo_owner}/\1_ta/src/branch/master/\3/01.md',
                         content,
                         flags=re.IGNORECASE)
        return content
    # end of ObsNotesPreprocessor fix_links(content)
# end of class ObsNotesPreprocessor



class BiblePreprocessor(Preprocessor):
    def __init__(self, *args, **kwargs) -> None:
        super(BiblePreprocessor, self).__init__(*args, **kwargs)
        self.book_filenames:List[str] = []
        self.RC_links:List[tuple] = []


    def is_multiple_jobs(self):
        return len(self.book_filenames) > 1


    def get_book_list(self):
        self.book_filenames.sort()
        return self.book_filenames


    def remove_closed_w_field(self, B:str, C:str, V:str, line:str, marker:str, text:str) -> str:
        """
        Extract words out of either \\w or \\+w fields
        """
        assert marker in ('w', '+w')

        ixW = text.find(f'\\{marker} ')
        while ixW != -1:
            ixEnd = text.find(f'\\{marker}*', ixW)
            if ixEnd != -1:
                field = text[ixW+len(marker)+2:ixEnd]
                # AppSettings.logger.debug(f"Cleaning \\w field: {field!r} from '{line}'")
                bits = field.split('|')
                adjusted_field = bits[0]
                # AppSettings.logger.debug(f"Adjusted field to: {adjusted_field!r}")
                text = text[:ixW] + adjusted_field + text[ixEnd+len(marker)+2:]
                # AppSettings.logger.debug(f"Adjusted line to: '{text}'")
            else:
                AppSettings.logger.error(f"Missing \\{marker}* in {B} {C}:{V} line: '{line}'")
                self.warnings.append(f"{B} {C}:{V} - Missing \\{marker}* closure")
                text = text.replace(f'\\{marker} ', '', 1) # Attempt to limp on
            ixW = text.find(f'\\{marker} ', ixW) # Might be another one
        return text
    # end of BiblePreprocessor.remove_closed_w_field function


    def write_clean_file(self, file_name: str, file_contents: str) -> None:
        """
        Cleans the USFM text as it writes it.

        Also saves a list of RC links for later checking
            (from inside \w fields of original Heb/Grk texts,
                e.g., x-tw="rc://*/tw/dict/bible/names/paul)

        TODO: Check/Remove some of this code once tC export is fixed
        TODO: Remove most of this once tX Job Handler handles full USFM3
        """
        # AppSettings.logger.debug(f"write_clean_file( {file_name}, {file_contents[:500]+('…' if len(file_contents)>500 else '')!r} )")

        # Replacing this code:
        # write_file(file_name, file_contents)
        # return

        # Make sure the directory exists
        make_dir(os.path.dirname(file_name))

        # Clean the USFM
        B = file_name[-8:-5] # Extract book abbreviation from somepath/nn-BBB.usfm
        needs_global_check = False
        has_USFM3_line = '\\usfm 3' in file_contents
        preadjusted_file_contents = file_contents

        # Check USFM2/3 pairs
        for opener,closer in (
                                # Character formatting
                                ('\\add ', '\\add*'),
                                ('\\addpn ', '\\addpn*'),
                                ('\\bd ', '\\bd*'),
                                ('\\bdit ', '\\bdit*'),
                                ('\\bk ', '\\bk*'),
                                ('\\dc ', '\\dc*'),
                                ('\\em ', '\\em*'),
                                ('\\fig ', '\\fig*'),
                                ('\\it ', '\\it*'),
                                ('\\k ', '\\k*'),
                                ('\\nd ', '\\nd*'),
                                ('\\ndx ', '\\ndx*'),
                                ('\\no ', '\\no*'),
                                ('\\ord ', '\\ord*'),
                                ('\\pn ', '\\pn*'),
                                ('\\pro ', '\\pro*'),
                                ('\\qt ', '\\qt*'),
                                ('\\sc ', '\\sc*'),
                                ('\\sig ', '\\sig*'),
                                ('\\sls ', '\\sls*'),
                                ('\\tl ', '\\tl*'),
                                ('\\w ', '\\w*'),
                                ('\\wg ', '\\wg*'),
                                ('\\wh ', '\\wh*'),
                                ('\\wj ', '\\wj*'),

                                ('\\f ', '\\f*'),
                                ('\\x ', '\\x*'),
                             ):
            cnt1, cnt2 = file_contents.count(opener), file_contents.count(closer)
            if cnt1 != cnt2:
                error_msg = f"{B} - Mismatched '{opener}' ({cnt1:,}) and '{closer}' ({cnt2:,}) field counts"
                AppSettings.logger.error(error_msg)
                self.warnings.append(error_msg)

        if has_USFM3_line:
            # Issue any global USFM3 warnings
            if '\\s5' in file_contents:
                warning_msg = f"{B} - \\s5 fields should be coded as \\ts-s\\*…\\ts-e\\* milestones"
                AppSettings.logger.warning(warning_msg)
                self.warnings.append(warning_msg)

            # Check USFM3 pairs
            for opener,closer in ( # NOTE: These are in addition to the USFM2 ones above
                                    # Character formatting
                                    ('\\png ', '\\png*'),
                                    ('\\rb ', '\\rb*'),
                                    ('\\sup ', '\\sup*'),
                                    ('\\wa ', '\\wa*'),
                                    ('\\va ', '\\va*'),
                                    # Milestones
                                    ('\\qt-s\\*', '\\qt-e\\*'), # NOTE: Will this still work if it has attributes?
                                    ('\\qt1-s\\*', '\\qt1-e\\*'), # NOTE: Will this still work if it has attributes?
                                    ('\\qt2-s\\*', '\\qt2-e\\*'), # NOTE: Will this still work if it has attributes?
                                    ('\\ts-s\\*', '\\ts-e\\*'),
                                    ('\\zaln-s ', '\\zaln-e\\*'),
                                 ):
                cnt1, cnt2 = file_contents.count(opener), file_contents.count(closer)
                if cnt1 != cnt2:
                    error_msg = f"{B} - Mismatched '{opener}' ({cnt1:,}) and '{closer}' ({cnt2:,}) field counts"
                    AppSettings.logger.error(error_msg)
                    self.warnings.append(error_msg)

            # Do some global adjustments to make things easier
            preadjusted_file_contents = re.sub(r'\\zaln-s (.+?)\\\*', '', preadjusted_file_contents) # Remove \zaln start milestones
            preadjusted_file_contents = preadjusted_file_contents.replace('\\zaln-e\\*','') # Remove \zaln end milestones
            preadjusted_file_contents = preadjusted_file_contents.replace('\\k-e\\*', '') # Remove self-closing keyterm milestones

            # Then do line-by-line changes
            needs_new_line = False
            adjusted_file_contents = ''
            C = V = ''
            for line in preadjusted_file_contents.split('\n'):
                if not line: continue # Ignore blank lines
                # AppSettings.logger.debug(f"Processing line: {line!r}")

                # Get C,V for debug messages
                if line.startswith('\\c '):
                    C = line[3:]
                elif line.startswith('\\v '):
                    V = line[3:].split(' ')[0]

                adjusted_line = line
                if '\\k' in adjusted_line: # Delete these fields
                    # AppSettings.logger.debug(f"Processing user-defined line: {line}")
                    ix = adjusted_line.find('\\k-s ')
                    if ix != -1:
                        adjusted_line = adjusted_line[:ix] # Remove k-s field right up to end of line
                    ix = adjusted_line.find('\\k-s')
                    if ix != -1:
                        AppSettings.logger.error(f"Non-closed \\k-s milestone in {B} {C}:{V} adjusted line: '{adjusted_line}'")
                        self.warnings.append(f"{B} {C}:{V} - Non-closed \\k-s milestone")
                        adjusted_line = adjusted_line[:ix] # Remove k-s field right up to end of line
                if '\\k-s' in adjusted_line:
                    AppSettings.logger.error(f"Remaining \\k-s in {B} {C}:{V} adjusted line: '{adjusted_line}'")
                    self.warnings.append(f"{B} {C}:{V} - Remaining \\k-s field")
                if '\\k-e' in adjusted_line:
                    AppSettings.logger.error(f"Remaining \\k-e in {B} {C}:{V} adjusted line: '{adjusted_line}'")
                    self.warnings.append(f"{B} {C}:{V} - Remaining \\k-e field")

                # Find and save any RC links (from inside \w fields)
                if (match := re.search(r'x-tw="(.+?)"', line)):
                    # print(f"Found RC link {match.group(1)} at {B} {C}:{V}")
                    self.RC_links.append( (B,C,V, 'tW', match.group(1)) )

                # Remove any \w fields (just leaving the word)
                adjusted_line = self.remove_closed_w_field(B, C, V, line, 'w', adjusted_line)
                # Remove any \+w fields (just leaving the word)
                adjusted_line = self.remove_closed_w_field(B, C, V, line, '+w', adjusted_line)
                # Be careful not to mess up on \wj
                # assert '\\w ' not in adjusted_line and '\\w\t' not in adjusted_line and '\\w\n' not in adjusted_line
                # assert '\\w*' not in adjusted_line
                for illegal_sequence in ('\\w ', '\\w\t', '\\w\n',
                                         '\\+w ', '\\+w\t', '\\+w\n', ):
                    if illegal_sequence in adjusted_line:
                        AppSettings.logger.error(f"Missing \\w* in {B} {C}:{V} line: '{line}'")
                        self.warnings.append(f"{B} {C}:{V} - Unprocessed '{illegal_sequence}' in line")
                        adjusted_line = adjusted_line.replace(illegal_sequence, '') # Attempt to limp on
                if adjusted_line != line: # it's non-blank and it changed
                    # if 'EPH' in file_name:
                        #  AppSettings.logger.debug(f"Adjusted {B} {C}:{V} \\w line from {line!r} to {adjusted_line!r}")
                    adjusted_file_contents += ('' if adjusted_line.startswith('\\v ') or adjusted_line.startswith('\\f ')
                                                    else ' ') \
                                                + adjusted_line
                    needs_new_line = True
                else: # the line didn't change (no \k \w or \z fields encountered)
                    if needs_new_line:
                        adjusted_file_contents += '\n'
                        needs_new_line = False
                        needs_global_check = True
                    # Copy across unchanged lines
                    adjusted_file_contents += line + '\n'


        else: # Not marked as USFM3
            # old code to handle bad tC USFM
            # First do global fixes to bad tC USFM
            # Hide good \q# markers
            preadjusted_file_contents = re.sub(r'\\q([1234acdmrs]?)\n', r'\\QQQ\1\n', preadjusted_file_contents) # Hide valid \q# markers
            # Invalid \q… markers
            preadjusted_file_contents, n1 = re.subn(r'\\q([^ 1234acdmrs])', r'\\q \1', preadjusted_file_contents) # Fix bad USFM \q without following space
            # \q markers with following text but missing the space in-betweeb
            preadjusted_file_contents, n2 = re.subn(r'\\(q[1234])([^ ])', r'\\\1 \2', preadjusted_file_contents) # Fix bad USFM \q without following space
            if n1 or n2: self.warnings.append(f"{B} - {n1+n2:,} badly formed \\q markers")
            # Restore good \q# markers
            preadjusted_file_contents = re.sub(r'\\QQQ([1234acdmrs]?)\n', r'\\q\1\n', preadjusted_file_contents) # Repair valid \q# markers

            # Hide empty \p markers
            preadjusted_file_contents = re.sub(r'\\p\n', r'\\PPP\n', preadjusted_file_contents) # Hide valid \p markers
            # Invalid \p… markers
            preadjusted_file_contents, n = re.subn(r'\\p([^ chimor])', r'\\p \1', preadjusted_file_contents) # Fix bad USFM \p without following space
            if n: self.warnings.append(f"{B} - {n:,} badly formed \\p markers")
            # Restore empty \p markers
            preadjusted_file_contents = re.sub(r'\\PPP\n', r'\\p\n', preadjusted_file_contents) # Repair valid \p markers

            # Find and warn about (useless) paragraph formatting before a section break
            #  (probably should be after break)
            ps_count = len(re.findall(r'\\p *\n?\\s', preadjusted_file_contents))
            if ps_count:
                s_suffix = '' if ps_count==1 else 's'
                self.warnings.append(f"{B} - {ps_count:,} useless \\p marker{s_suffix} before \\s# marker{s_suffix}")
            qs_count = len(re.findall(r'\\q1* *\n?\\s', preadjusted_file_contents))
            if qs_count:
                s_suffix = '' if qs_count==1 else 's'
                self.warnings.append(f"{B} - {qs_count:,} useless \\q# marker{s_suffix} before \\s# marker{s_suffix}")

            # Then do other global clean-ups
            ks_count = preadjusted_file_contents.count('\\k-s\\*')
            if not ks_count:
                ks_count = preadjusted_file_contents.count('\\k-s')
            ke_count = preadjusted_file_contents.count('\\k-e\\*')
            if not ke_count:
                ke_count = preadjusted_file_contents.count('\\k-e')
            zs_count = preadjusted_file_contents.count('\\zaln-s')
            ze_count = preadjusted_file_contents.count('\\zaln-e')
            if ks_count or zs_count or zs_count or ze_count: # Assume it's USFM3
                if not has_USFM3_line:
                    self.warnings.append(f"{B} - '\\usfm 3.0' line seems missing")
            close_count = preadjusted_file_contents.count('\\*')
            expected_close_count = ks_count + ke_count + zs_count + ze_count
            if close_count < expected_close_count:
                self.warnings.append(f"{B} - {expected_close_count-close_count:,} unclosed \\k or \\zaln milestone markers")
            preadjusted_file_contents = preadjusted_file_contents.replace('\\k-e\\*', '') # Remove self-closing keyterm milestones
            preadjusted_file_contents = preadjusted_file_contents.replace('\\zaln-e\\*', '') # Remove self-closing alignment milestones
            if preadjusted_file_contents != file_contents:
                needs_global_check = True


            # Then do line-by-line changes
            needs_new_line = False
            adjusted_file_contents = ''
            C = V = ''
            for line in preadjusted_file_contents.split('\n'):
                if not line: continue # Ignore blank lines
                # AppSettings.logger.debug(f"Processing line: {line!r}")

                # Get C,V for debug messages
                if line.startswith('\\c '):
                    C = line[3:]
                elif line.startswith('\\v '):
                    V = line[3:].split(' ')[0]

                adjusted_line = line
                if '\\k' in adjusted_line: # Delete these fields
                    # TODO: These milestone fields in the source texts should be self-closing
                    # AppSettings.logger.debug(f"Processing user-defined line: {line}")
                    ix = adjusted_line.find('\\k-s')
                    if ix != -1:
                        adjusted_line = adjusted_line[:ix] # Remove k-s field right up to end of line
                assert '\\k-s' not in adjusted_line
                assert '\\k-e' not in adjusted_line
                # HANDLE FAULTY USFM IN UGNT
                if '\\w ' in adjusted_line and adjusted_line.endswith('\\w'):
                    AppSettings.logger.warning(f"Attempting to fix \\w error in {B} {C}:{V} line: '{line}'")
                    adjusted_line += '*' # Try a change to a closing marker

                # Remove \w fields (just leaving the word)
                adjusted_line = self.remove_closed_w_field(B, C, V, line, 'w', adjusted_line)
                # Remove \+w fields (just leaving the word)
                adjusted_line = self.remove_closed_w_field(B, C, V, line, '+w', adjusted_line)
                # Don't mess up on \wj
                for illegal_sequence in ('\\w ', '\\w\t', '\\w\n',
                                         '\\+w ', '\\+w\t', '\\+w\n', ):
                    if illegal_sequence in adjusted_line:
                        AppSettings.logger.error(f"Unclosed '{illegal_sequence}' in {B} {C}:{V} line: '{line}'")
                        self.warnings.append(f"{B} {C}:{V} - Unprocessed '{illegal_sequence}' in line")
                        adjusted_line = adjusted_line.replace(illegal_sequence, '') # Attempt to limp on
                # assert '\\w*' not in adjusted_line
                if '\\z' in adjusted_line: # Delete these user-defined fields
                    # TODO: These milestone fields in the source texts should be self-closing
                    # AppSettings.logger.debug(f"Processing user-defined line: {line}")
                    ix = adjusted_line.find('\\zaln-s')
                    if ix != -1:
                        adjusted_line = adjusted_line[:ix] # Remove zaln-s field right up to end of line
                if '\\z' in adjusted_line:
                    AppSettings.logger.error(f"Remaining \\z in {B} {C}:{V} adjusted line: '{adjusted_line}'")
                    self.warnings.append(f"{B} {C}:{V} - Remaining \\z field")
                if not adjusted_line: # was probably just a \zaln-s milestone with nothing else
                    continue
                if adjusted_line != line: # it's non-blank and it changed
                    # if 'EPH' in file_name:
                        #  AppSettings.logger.debug(f"Adjusted {B} {C}:{V} \\w line from {line!r} to {adjusted_line!r}")
                    adjusted_file_contents += ' ' + adjusted_line
                    needs_new_line = True
                    continue
                assert adjusted_line == line # No \k \w or \z fields encountered

                if needs_new_line:
                    adjusted_file_contents += '\n'
                    needs_new_line = False
                    needs_global_check = True

                # Copy across unchanged lines
                adjusted_file_contents += line + '\n'

        if needs_global_check: # Do some file-wide clean-up
            # AppSettings.logger.debug(f"Doing global fixes for {B} …")
            adjusted_file_contents = re.sub(r'([^\n])\\v ', r'\1\n\\v ', adjusted_file_contents) # Make sure \v goes onto separate line
            adjusted_file_contents = adjusted_file_contents.replace('\n ',' ') # Move lines starting with space up to the previous line
            adjusted_file_contents = adjusted_file_contents.replace('\n\\va ',' \\va ') # Move lines starting with \va up to the previous line
            adjusted_file_contents = re.sub(r'\n([,.;:?])', r'\1', adjusted_file_contents) # Bring leading punctuation up onto the previous line
            adjusted_file_contents = re.sub(r'([^\n])\\s5', r'\1\n\\s5', adjusted_file_contents) # Make sure \s5 goes onto separate line
            while '\n\n' in adjusted_file_contents:
                adjusted_file_contents = adjusted_file_contents.replace('\n\n','\n') # Delete blank lines
            adjusted_file_contents = adjusted_file_contents.replace(' ," ',', "') # Fix common tC quotation punctuation mistake
            adjusted_file_contents = adjusted_file_contents.replace(",' ",",' ") # Fix common tC quotation punctuation mistake
            adjusted_file_contents = adjusted_file_contents.replace(' " ',' "') # Fix common tC quotation punctuation mistake
            adjusted_file_contents = adjusted_file_contents.replace(" ' "," '") # Fix common tC quotation punctuation mistake

        # Write the modified USFM
        if prefix and debug_mode_flag:
            if '\\w ' in adjusted_file_contents or '\\w\t' in adjusted_file_contents or '\\w\n' in adjusted_file_contents:
                AppSettings.logger.debug(f"Writing {file_name}: {adjusted_file_contents}")
            assert '\\w ' not in adjusted_file_contents and '\\w\t' not in adjusted_file_contents and '\\w\n' not in adjusted_file_contents # Raise error
        with open(file_name, 'wt', encoding='utf-8') as out_file:
            out_file.write(adjusted_file_contents)
    # end of BiblePreprocessor.write_clean_file function


    def clean_copy(self, source_pathname: str, destination_pathname: str) -> None:
        """
        Cleans the USFM file as it copies it.
        """
        # AppSettings.logger.debug(f"clean_copy( {source_pathname}, {destination_pathname} )")

        # Replacing this code:
        # copy(source_pathname, destination_pathname)
        # return

        with open(source_pathname, 'rt') as in_file:
            source_contents = in_file.read()
        self.write_clean_file(destination_pathname, source_contents)
    # end of BiblePreprocessor.clean_copy function


    def process_RC_links(self):
        """
        Process the RC links that have been stored in self.RC_links.
        """
        num_links = len(self.RC_links)
        AppSettings.logger.info(f"process_RC_links for {num_links:,} links…")
        done_type_error = False
        cached_links = set()
        handled_count = 0
        for B,C,V, link_type,link_text in self.RC_links:
            handled_count += 1
            if handled_count % 1_000 == 0:
                AppSettings.logger.info(f"  Handled {handled_count:,} links = {handled_count*100 // num_links}% (Got {len(cached_links):,} in cache)")
            # AppSettings.logger.debug(f"Got {B} {C}:{V} {link_type}={link_text}")
            if not link_type == 'tW':
                if not done_type_error:
                    err_msg = f"{B} {C}:{V} - Unexpected '{link_type}' error '{link_text}'"
                    AppSettings.logger.error(err_msg)
                    self.warnings.append(err_msg)
                    done_type_error = True
                    continue
            if not link_text.startswith('rc://*/tw/dict/bible/'):
                err_msg = f"{B} {C}:{V} - Bad {link_type} link format: '{link_text}'"
                AppSettings.logger.error(err_msg)
                self.warnings.append(err_msg)
                continue
            link_word = link_text[21:]
            if link_word in cached_links: # we've already checked it
                # print(f"Found '{link_word}' in cache")
                continue
            # TODO: How can we know what the language should be ???
            link_url = f'https://git.door43.org/unfoldingWord/en_tw/raw/branch/master/bible/{link_word}.md'
            # file_contents = get_url(url)
            try:
                with urlopen(link_url) as f:
                    file_start_byte = f.read(1) # Only need to download/read one byte from the file
            except HTTPError:
                cached_links.add(link_word) # So we only get one error per link_word
                err_msg = f"{B} {C}:{V} - Missing {link_type} file for '{link_word}' expected at {link_url[8:]}" # Skip https:// part
                AppSettings.logger.error(err_msg)
                self.warnings.append(err_msg)
                continue
            # print(url, repr(file_start_byte))
            if file_start_byte == b'#': # Expected start of markdown file
                cached_links.add(link_word)
                # if not len(cached_links) % 100:
                #     AppSettings.logger.info(f"  Got {len(cached_links)} links in cache")
            else:
                cached_links.add(link_word) # So we only get one error per link_word
                err_msg = f"{B} {C}:{V} - Possible bad {link_type} file: '{link_text}'"
                AppSettings.logger.error(err_msg)
                self.warnings.append(err_msg)
        # Not needed any more -- empty the list to mark them as "processed"
        self.RC_links = []
    # end of BiblePreprocessor.clean_copy function


    def run(self) -> Tuple[int, List[str]]:
        AppSettings.logger.debug(f"Bible preprocessor starting with {self.source_dir} = {os.listdir(self.source_dir)} …")
        for idx, project in enumerate(self.rc.projects):
            project_path = os.path.join(self.source_dir, project.path)
            file_format = '{0}-{1}.usfm'

            # Case #1: The project path is a file, and thus is one book of the Bible, copy to standard filename
            # AppSettings.logger.debug(f"Bible preprocessor case #1: Copying single Bible file for '{project.identifier}' …")
            if os.path.isfile(project_path):
                if project.identifier.lower() in BOOK_NUMBERS:
                    filename = file_format.format(BOOK_NUMBERS[project.identifier.lower()], project.identifier.upper())
                else:
                    filename = file_format.format(str(idx+1).zfill(2), project.identifier.upper())
                self.clean_copy(project_path, os.path.join(self.output_dir, filename))
                self.book_filenames.append(filename)
                self.num_files_written += 1
            else:
                # Case #2: Project path is a dir with one or more USFM files, is one or more books of the Bible
                AppSettings.logger.debug(f"Bible preprocessor case #2: Copying Bible files for '{project.identifier}' …")
                usfm_files = glob(os.path.join(project_path, '*.usfm'))
                if usfm_files:
                    for usfm_path in usfm_files:
                        book_code = os.path.splitext(os.path.basename(usfm_path))[0].split('-')[-1].lower()
                        if book_code in BOOK_NUMBERS:
                            filename = file_format.format(BOOK_NUMBERS[book_code], book_code.upper())
                        else:
                            filename = f'{os.path.splitext(os.path.basename(usfm_path))[0]}.usfm'
                        output_file_path = os.path.join(self.output_dir, filename)
                        if os.path.isfile(usfm_path) and not os.path.exists(output_file_path):
                            self.clean_copy(usfm_path, output_file_path)
                        self.book_filenames.append(filename)
                        self.num_files_written += 1
                else:
                    # Case #3: Project path is a dir with one or more chapter dirs with chunk & title files
                    AppSettings.logger.debug(f"Bible preprocessor case #3: Combining Bible chapter files for '{project.identifier}' …")
                    chapters = self.rc.chapters(project.identifier)
                    # print("chapters3:", chapters)
                    if chapters:
                        #          Piece the USFM file together
                        title_file = os.path.join(project_path, chapters[0], 'title.txt')
                        if os.path.isfile(title_file):
                            title = read_file(title_file)
                            title = re.sub(r' \d+$', '', title).strip()
                            # print("title1", title)
                        else:
                            title = project.title
                            # print("title2", title)
                        if not title and os.path.isfile(os.path.join(project_path, 'title.txt')):
                            title = read_file(os.path.join(project_path, 'title.txt'))
                            # print("title3", title)
                        # if not title and os.path.isfile(os.path.join(project_path, 'headers.json')):
                        #     headers_text = read_file(os.path.join(project_path, 'headers.json'))
                        #     headers_list = json.loads(headers_text)
                        #     print("headers_list", headers_list)
                        #     for headers_dict in headers_list:
                        #         if headers_dict and 'tag' in headers_dict and headers_dict['tag']=='toc1' and content in headers_dict:
                        #             title = headers_dict['content']
                        #         print("title4", title)
                        usfm = f"""
\\id {project.identifier.upper()} {self.rc.resource.title}
\\ide UTF-8
\\h {title}
\\toc1 {title}
\\toc2 {title}
\\mt {title}
"""
                        # print("chapters:", chapters)
                        for chapter in chapters:
                            if chapter in self.ignoreDirectories:
                                continue
                            chapter_num = chapter.lstrip('0')
                            chunks = self.rc.chunks(project.identifier, chapter)
                            if not chunks:
                                continue
                            try: first_chunk = read_file(os.path.join(project_path, chapter, chunks[0]))
                            except Exception as e:
                                self.warnings.append(f"Error reading {chapter}/{chunks[0]}: {e}")
                                continue
                            usfm += '\n\n'
                            if f'\\c {chapter_num}' not in first_chunk:
                                usfm += f'\\c {chapter_num}\n'
                            if os.path.isfile(os.path.join(project_path, chapter, 'title.txt')):
                                translated_title = read_file(os.path.join(project_path, chapter, 'title.txt'))
                                book_name = re.sub(r' \d+$', '', translated_title).strip()
                                if book_name.lower() != title.lower():
                                    usfm += f'\\cl {translated_title}\n'
                            for chunk in chunks:
                                if chunk in self.ignoreFiles:
                                    continue
                                chunk_num = os.path.splitext(chunk)[0].lstrip('0')
                                try: chunk_content = read_file(os.path.join(project_path, chapter, chunk))
                                except Exception as e:
                                    self.warnings.append(f"Error reading {chapter}/{chunk}: {e}")
                                    continue
                                if f'\\v {chunk_num} ' not in chunk_content:
                                    chunk_content = f'\\v {chunk_num} ' + chunk_content
                                usfm += chunk_content+"\n"
                        if project.identifier.lower() in BOOK_NUMBERS:
                            filename = file_format.format(BOOK_NUMBERS[project.identifier.lower()],
                                                          project.identifier.upper())
                        else:
                            filename = file_format.format(str(idx + 1).zfill(2), project.identifier.upper())
                        self.write_clean_file(os.path.join(self.output_dir, filename), usfm)
                        self.book_filenames.append(filename)
                        self.num_files_written += 1
        if self.num_files_written == 0:
            AppSettings.logger.error(f"Bible preprocessor didn't write any usfm files")
            self.warnings.append("No Bible source files discovered")
        else:
            AppSettings.logger.debug(f"Bible preprocessor wrote {self.num_files_written} usfm files with {len(self.warnings)} warnings")

        if self.RC_links:
            self.process_RC_links()

        AppSettings.logger.debug(f"Bible preprocessor returning with {self.output_dir} = {os.listdir(self.output_dir)}")
        # AppSettings.logger.debug(f"Bible preprocessor returning {self.warnings if self.warnings else True}")
        return self.num_files_written, self.warnings
    # end of BiblePreprocessor.run()
# end of class BiblePreprocessor



class TaPreprocessor(Preprocessor):
    manual_title_map = {
        'checking': 'Checking Manual',
        'intro': 'Introduction to translationAcademy',
        'process': 'Process Manual',
        'translate': 'Translation Manual'
    }

    def __init__(self, *args, **kwargs) -> None:
        super(TaPreprocessor, self).__init__(*args, **kwargs)
        self.section_container_id = 1

    def get_title(self, project, link:str, alt_title:Optional[str]=None) -> str:
        proj = None
        project_config = project.config()
        if project_config and link in project_config:
            proj = project
        else:
            for p in self.rc.projects:
                p_config = p.config()
                if p_config and link in p_config:
                    proj = p
        if proj:
            title_file = os.path.join(self.source_dir, proj.path, link, 'title.md')
            if os.path.isfile(title_file):
                return read_file(title_file)
        if alt_title:
            return alt_title
        else:
            return link.replace('-', ' ').title()

    def get_ref(self, project, link:str) -> str:
        project_config = project.config()
        if project_config and link in project_config:
            return f'#{link}'
        for p in self.rc.projects:
            p_config = p.config()
            if p_config and link in p_config:
                return f'{p.identifier}.html#{link}'
        return f'#{link}'

    def get_question(self, project, slug:str) -> str:
        subtitle_file = os.path.join(self.source_dir, project.path, slug, 'sub-title.md')
        if os.path.isfile(subtitle_file):
            return read_file(subtitle_file)

    def get_content(self, project, slug):
        content_file = os.path.join(self.source_dir, project.path, slug, '01.md')
        if os.path.isfile(content_file):
            return read_file(content_file)

    def compile_ta_section(self, project, section, level):
        """
        Recursive section markdown creator

        :param project:
        :param dict section:
        :param int level:
        :return:
        """
        # if prefix and debug_mode_flag:
        #     AppSettings.logger.debug(f"{'  '*level}compile_ta_section for '{section['title']}' {level=} …")
        if 'link' in section:
            link = section['link']
        else:
            link = f'section-container-{self.section_container_id}'
            self.section_container_id = self.section_container_id + 1
        try:
            markdown = f"""{'#' * level} <a id="{link}"/>{self.get_title(project, link, section['title'])}\n\n"""
        except KeyError: # probably missing section title
            msg = f"Title seems missing for '{project.identifier}' level {level} '{link}'"
            AppSettings.logger.warning(msg)
            self.warnings.append(msg)
            markdown = f"""{'#' * level} <a id="{link}"/>MISSING TITLE???\n\n"""

        if 'link' in section:
            top_box = ""
            bottom_box = ""
            question = self.get_question(project, link)
            if question:
                # TODO: Shouldn't text like this be translated ???
                top_box += f"This page answers the question: *{question}*\n\n"
            config = project.config()
            if config and link in config:
                if 'dependencies' in config[link] and config[link]['dependencies']:
                    top_box += 'In order to understand this topic, it would be good to read:\n\n'
                    for dependency in config[link]['dependencies']:
                        top_box += '  * *[{0}]({1})*\n'.\
                            format(self.get_title(project, dependency), self.get_ref(project, dependency))
                if 'recommended' in config[link] and config[link]['recommended']:
                    bottom_box += 'Next we recommend you learn about:\n\n'
                    for recommended in config[link]['recommended']:
                        bottom_box += '  * *[{0}]({1})*\n'.\
                            format(self.get_title(project, recommended), self.get_ref(project, recommended))
            if top_box:
                markdown += f'<div class="top-box box" markdown="1">\n{top_box}\n</div>\n\n'
            content = self.get_content(project, link)
            if content:
                markdown += f'{content}\n\n'
            if bottom_box:
                markdown += f'<div class="bottom-box box" markdown="1">\n{bottom_box}\n</div>\n\n'
            markdown += '---\n\n'  # horizontal rule
        if 'sections' in section:
            if section['sections']:
                for subsection in section['sections']:
                    markdown += self.compile_ta_section(project, subsection, level + 1)
            else: # why is it empty? probably user error
                msg = f"'sections' seems empty for '{project.identifier}' toc.yaml: '{section['title']}'"
                AppSettings.logger.warning(msg)
                self.warnings.append(msg)
        return markdown
    # end of compile_ta_section(self, project, section, level)


    def run(self) -> Tuple[int, List[str]]:
        AppSettings.logger.debug(f"tA preprocessor starting with {self.source_dir} = {os.listdir(self.source_dir)} …")
        for idx, project in enumerate(self.rc.projects):
            AppSettings.logger.debug(f"tA preprocessor: Copying files for '{project.identifier}' …")
            self.section_container_id = 1
            toc = self.rc.toc(project.identifier)
            if project.identifier in self.manual_title_map:
                title = self.manual_title_map[project.identifier]
            else:
                title = f'{project.identifier.title()} Manual'
            markdown = f'# {title}\n\n'
            if toc:
                for section in toc['sections']:
                    markdown += self.compile_ta_section(project, section, 2)
            markdown = self.fix_links(markdown, self.repo_owner)
            output_file = os.path.join(self.output_dir, f'{str(idx+1).zfill(2)}-{project.identifier}.md')
            write_file(output_file, markdown)
            self.num_files_written += 1

            # tA: Copy the toc and config.yaml file to the output dir so they can be used to
            #       generate the ToC on door43.org
            toc_file = os.path.join(self.source_dir, project.path, 'toc.yaml')
            if os.path.isfile(toc_file):
                copy(toc_file, os.path.join(self.output_dir, f'{str(idx+1).zfill(2)}-{project.identifier}-toc.yaml'))
            config_file = os.path.join(self.source_dir, project.path, 'config.yaml')
            if os.path.isfile(config_file):
                copy(config_file, os.path.join(self.output_dir, f'{str(idx+1).zfill(2)}-{project.identifier}-config.yaml'))
            elif project.path!='./':
                self.warnings.append(f"Possible missing config.yaml file in {project.path} folder")
        if self.num_files_written == 0:
            AppSettings.logger.error("tA preprocessor didn't write any markdown files")
            self.warnings.append("No tA source files discovered")
        else:
            AppSettings.logger.debug(f"tA preprocessor wrote {self.num_files_written} markdown files with {len(self.warnings)} warnings")
        AppSettings.logger.debug(f"tA preprocessor returning with {self.output_dir} = {os.listdir(self.output_dir)}")
        return self.num_files_written, self.warnings
    # end of TaPreprocessor run()


    def fix_links(self, content:str, repo_owner:str) -> str:
        """
        For tA
        """
        # convert RC links, e.g. rc://en/tn/help/1sa/16/02
        #                           => https://git.door43.org/{repo_owner}/en_tn/1sa/16/02.md
        content = re.sub(r'rc://([^/]+)/([^/]+)/([^/]+)/([^\s\\p{P})\]\n$]+)',
                         rf'https://git.door43.org/{repo_owner}/\1_\2/src/branch/master/\4.md', content, flags=re.IGNORECASE)
        # fix links to other sections within the same manual (only one ../ and a section name)
        # e.g. [Section 2](../section2/01.md) => [Section 2](#section2)
        content = re.sub(r'\]\(\.\./([^/)]+)/01.md\)', r'](#\1)', content)
        # fix links to other manuals (two ../ and a manual name and a section name)
        # e.g. [how to translate](../../translate/accurate/01.md) => [how to translate](translate.html#accurate)
        for idx, project in enumerate(self.rc.projects):
            pattern = re.compile(r'\]\(\.\./\.\./{0}/([^/)]+)/01.md\)'.format(project.identifier))
            replace = r']({0}-{1}.html#\1)'.format(str(idx+1).zfill(2), project.identifier)
            content = re.sub(pattern, replace, content)
        # fix links to other sections that just have the section name but no 01.md page (preserve http:// links)
        # e.g. See [Verbs](figs-verb) => See [Verbs](#figs-verb)
        content = re.sub(r'\]\(([^# :/)]+)\)', r'](#\1)', content)
        # convert URLs to links if not already
        content = re.sub(r'([^"(\[])((http|https|ftp)://[A-Z0-9/?&_.:=#-]+[A-Z0-9/?&_:=#-])', r'\1[\2](\2)',
                         content, flags=re.IGNORECASE)
        # URLS wth just www at the start, no http
        content = re.sub(r'([^A-Z0-9"(/])(www\.[A-Z0-9/?&_.:=#-]+[A-Z0-9/?&_:=#-])', r'\1[\2](http://\2)',
                         content, flags=re.IGNORECASE)
        return content
    # end of TaPreprocessor fix_links(content)
# end of class TaPreprocessor



class TqPreprocessor(Preprocessor):

    def run(self) -> Tuple[int, List[str]]:
        AppSettings.logger.debug(f"tQ preprocessor starting with {self.source_dir} = {os.listdir(self.source_dir)} …")
        index_json = {
            'titles': {},
            'chapters': {},
            'book_codes': {}
        }
        headers_re = re.compile('^(#+) +(.+?) *#*$', flags=re.MULTILINE)
        for project in self.rc.projects:
            AppSettings.logger.debug(f"tQ preprocessor: Combining chapters for '{project.identifier}' …")
            if project.identifier in BOOK_NAMES:
                markdown = ''
                book = project.identifier.lower()
                html_file = f'{BOOK_NUMBERS[book]}-{book.upper()}.html'
                index_json['book_codes'][html_file] = book
                name = BOOK_NAMES[book]
                index_json['titles'][html_file] = name
                chapter_dirs = sorted(glob(os.path.join(self.source_dir, project.path, '*')))
                markdown += f'# <a id="tq-{book}"/> {name}\n\n'
                index_json['chapters'][html_file]:List[str] = []
                if chapter_dirs:
                    for chapter_dir in chapter_dirs:
                        # AppSettings.logger.debug(f"tQ preprocessor: Processing {chapter_dir} for '{project.identifier}' {book} …")
                        chapter = os.path.basename(chapter_dir)
                        if chapter in self.ignoreFiles or chapter == 'manifest.json':
                            # NOTE: Would it have been better to check for file vs folder here (and ignore files) ???
                            continue

                        chunk_txt_filepaths = sorted(glob(os.path.join(chapter_dir, '*.txt')))
                        # If there are JSON txt files in chapter folders, convert them to md format (and delete the original)
                        #   (These are created by tS)
                        if chunk_txt_filepaths:
                            txt2md(chapter_dir)
                            # convertedCount = txt2md(chapter_dir)
                            # if convertedCount:
                            #     AppSettings.logger.debug(f"tQ preprocessor: Converted {convertedCount} txt files in {chapter} to JSON")

                        link = f'tq-chapter-{book}-{chapter.zfill(3)}'
                        index_json['chapters'][html_file].append(link)
                        markdown += f"""## <a id="{link}"/> {name} {chapter.lstrip('0')}\n\n"""
                        chunk_filepaths = sorted(glob(os.path.join(chapter_dir, '*.md')))
                        if chunk_filepaths:
                            for chunk_idx, chunk_filepath in enumerate(chunk_filepaths):
                                # AppSettings.logger.debug(f"tQ preprocessor: Processing {chunk_file} in {chapter_dir} for '{project.identifier}' {book} …")
                                start_verse = os.path.splitext(os.path.basename(chunk_filepath))[0].lstrip('0')
                                if chunk_idx < len(chunk_filepaths)-1:
                                    try:
                                        end_verse = str(int(os.path.splitext(os.path.basename(chunk_filepaths[chunk_idx+1]))[0])-1)
                                    except ValueError:
                                        # Can throw a ValueError if chunk is not an integer, e.g., '5&8' or contains \u00268 (ɨ)
                                        initial_string = os.path.splitext(os.path.basename(chunk_filepaths[chunk_idx+1]))[0]
                                        msg = f"{book} {chapter} had a problem handling '{initial_string}'"
                                        AppSettings.logger.critical(msg)
                                        self.warnings.append(msg)
                                        # TODO: The following is probably not the best/right thing to do???
                                        end_verse = BOOK_CHAPTER_VERSES[book][chapter.lstrip('0')]
                                else:
                                    try:
                                        end_verse = BOOK_CHAPTER_VERSES[book][chapter.lstrip('0')]
                                    except KeyError:
                                        AppSettings.logger.critical(f"{book} does not normally contain chapter '{chapter}'")
                                        self.warnings.append(f"{book} does not normally contain chapter '{chapter}'")
                                        # TODO: The following is probably not the best/right thing to do???
                                        end_verse = '199'
                                link = f'tq-chunk-{book}-{str(chapter).zfill(3)}-{str(start_verse).zfill(3)}'
                                markdown += '### <a id="{0}"/>{1} {2}:{3}{4}\n\n'.\
                                    format(link, name, chapter.lstrip('0'), start_verse,
                                        '-'+end_verse if start_verse != end_verse else '')
                                try: text = read_file(chunk_filepath) + '\n\n'
                                except Exception as e:
                                    self.warnings.append(f"Error reading {os.path.basename(chunk_filepath)}: {e}")
                                    continue
                                text = headers_re.sub(r'\1### \2', text)  # This will bump any header down 3 levels
                                markdown += text
                        else: # no chunk files
                            msg = f"No .md chunk files found in {book} {chapter} folder"
                            AppSettings.logger.warning(msg)
                            self.warnings.append(msg)
                else: # no chapter dirs
                    msg = f"No chapter folders found in {book} folder"
                    AppSettings.logger.warning(msg)
                    self.warnings.append(msg)
                file_path = os.path.join(self.output_dir, f'{BOOK_NUMBERS[book]}-{book.upper()}.md')
                write_file(file_path, markdown)
                self.num_files_written += 1
            else:
                AppSettings.logger.debug(f'TqPreprocessor: extra project found: {project.identifier}')

        if self.num_files_written == 0:
            AppSettings.logger.error(f"tQ preprocessor didn't write any markdown files")
            self.warnings.append("No tQ source files discovered")
        else:
            AppSettings.logger.debug(f"tQ preprocessor wrote {self.num_files_written} markdown files with {len(self.warnings)} warnings")

        # Write out index.json
        output_file = os.path.join(self.output_dir, 'index.json')
        write_file(output_file, index_json)
        AppSettings.logger.debug(f"tQ preprocessor returning with {self.output_dir} = {os.listdir(self.output_dir)}")
        return self.num_files_written, self.warnings
    # end of TqPreprocessor run()
# end of class TqPreprocessor



class TwPreprocessor(Preprocessor):
    section_titles = {
        'kt': 'Key Terms',
        'names': 'Names',
        'other': 'Other'
    }

    def run(self) -> Tuple[int, List[str]]:
        AppSettings.logger.debug(f"tW preprocessor starting with {self.source_dir} = {os.listdir(self.source_dir)} …")
        index_json = {
            'titles': {},
            'chapters': {},
            'book_codes': {}
            }

        # Handle a specific non-conformant tW output (JSON in .txt files in 01/ folder) by ts-desktop
        dir_list = os.listdir(self.source_dir)
        if len(dir_list)>=3 \
        and '01' in dir_list and 'LICENSE.md' in dir_list and 'manifest.json' in dir_list:
            # TODO: Is the above the best way to detect these types of repos (One has .DS_Store file also)
            # Handle tW json (.txt) files containing "title" and "body" fields
            AppSettings.logger.info(f"tW preprocessor moving to '01' folder (had {dir_list})…")
            assert len(self.rc.projects) == 1
            project = self.rc.projects[0]
            AppSettings.logger.debug(f"tW preprocessor 01: Copying files for '{project.identifier}' …")
            # AppSettings.logger.debug(f"tW preprocessor 01: project.path='{project.path}'")
            # Collect all the JSON MD data from the text files into dictionaries
            term_text = {}
            section = 'other' # since we don't have any other info to set this inteligently (e.g., KTs, names)
            key = f'{section}.html'
            index_json['titles'][key] = self.section_titles[section]
            index_json['chapters'][key] = {}
            index_json['book_codes'][key] = section
            term_files = sorted(glob(os.path.join(self.source_dir, '01/', '*.txt')))
            for term_filepath in term_files:
                # These .txt files actually contain JSON (which contains markdown)
                AppSettings.logger.debug(f"tW preprocessor 01: processing '{term_filepath}' …")
                term = os.path.splitext(os.path.basename(term_filepath))[0]
                try: text = read_file(term_filepath)
                except Exception as e:
                    self.warnings.append(f"Error reading {os.path.basename(term_filepath)}: {e}")
                    continue
                try:
                    json_data = json.loads(text)
                except json.decoder.JSONDecodeError as e:
                    # Clean-up the filepath for display (mostly removing /tmp folder names)
                    adjusted_filepath = '/'.join(term_filepath.split('/')[6:]) #.replace('/./','/')
                    error_message = f"Badly formed tW json file '{adjusted_filepath}': {e}"
                    AppSettings.logger.error(error_message)
                    self.warnings.append(error_message)
                    json_data = {}
                if json_data:
                    unit_count = 0
                    title = body_text = None
                    for tw_unit in json_data:
                        if 'title' in tw_unit and 'body' in tw_unit:
                            title = tw_unit['title']
                            if not title:
                                self.warnings.append(f"Missing tW title in {term}.txt: {tw_unit}")
                            elif '\n' in title:
                                self.warnings.append(f"Badly formatted tW title in {term}.txt: {title!r}")
                            raw_body_text = tw_unit['body']
                            if not raw_body_text:
                                self.warnings.append(f"Missing tW body in {term}.txt: {tw_unit}")
                            body_text = f'### <a id="{term}"/>{title}\n\n{raw_body_text}'
                            unit_count += 1
                        else:
                            self.warnings.append(f"Unexpected tW unit in {term}.txt: {tw_unit}")
                    assert unit_count == 1 # Only expect one title/body set I think
                    index_json['chapters'][key][term] = title
                    term_text[term] = body_text
                else:
                    error_message = f"No tW json data found in file '{adjusted_filepath}'"
                    AppSettings.logger.error(error_message)
                    self.warnings.append(error_message)
            # Now process the dictionaries to sort terms by title and add to markdown
            markdown = ''
            titles = index_json['chapters'][key]
            terms_sorted_by_title = sorted(titles, key=lambda i: titles[i].lower())
            for term in terms_sorted_by_title:
                if markdown:
                    markdown += '<hr>\n\n'
                markdown += f"{term_text[term]}\n\n"
            markdown = f'# <a id="tw-section-{section}"/>{self.section_titles[section]}\n\n{markdown}'
            markdown = self.fix_links(markdown, section, self.repo_owner)
            output_file = os.path.join(self.output_dir, f'{section}.md')
            write_file(output_file, markdown)
            self.num_files_written += 1
            config_file = os.path.join(self.source_dir, project.path, 'config.yaml')
            if os.path.isfile(config_file):
                copy(config_file, os.path.join(self.output_dir, 'config.yaml'))
            elif project.path!='./':
                self.warnings.append(f"Possible missing config.yaml file in {project.path} folder")
            output_file = os.path.join(self.output_dir, 'index.json')
            write_file(output_file, index_json)

        else: # handle tW markdown files
            title_re = re.compile('^# +(.*?) *#*$', flags=re.MULTILINE)
            headers_re = re.compile('^(#+) +(.+?) *#*$', flags=re.MULTILINE)
            for project in self.rc.projects:
                AppSettings.logger.debug(f"tW preprocessor 02: Copying files for '{project.identifier}' …")
                term_text = {}
                section_dirs = sorted(glob(os.path.join(self.source_dir, project.path, '*')))
                for section_dir in section_dirs:
                    section = os.path.basename(section_dir)
                    if section not in self.section_titles:
                        continue
                    key = f'{section}.html'
                    index_json['titles'][key] = self.section_titles[section]
                    index_json['chapters'][key] = {}
                    index_json['book_codes'][key] = section
                    term_files = sorted(glob(os.path.join(section_dir, '*.md')))
                    for term_filepath in term_files:
                        AppSettings.logger.debug(f"tW preprocessor 02: processing '{term_filepath}' …")
                        term = os.path.splitext(os.path.basename(term_filepath))[0]
                        try: text = read_file(term_filepath)
                        except Exception as e:
                            self.warnings.append(f"Error reading {os.path.basename(term_filepath)}: {e}")
                            continue
                        if title_re.search(text):
                            title = title_re.search(text).group(1)
                            text = title_re.sub(r'# <a id="{0}"/>\1 #'.format(term), text)  # inject the term by the title
                        else:
                            title = os.path.splitext(os.path.basename(term_filepath))[0]  # No title found, so using term
                        text = headers_re.sub(r'#\1 \2', text)
                        index_json['chapters'][key][term] = title
                        term_text[term] = text
                    # Sort terms by title and add to markdown
                    markdown = ''
                    titles = index_json['chapters'][key]
                    terms_sorted_by_title = sorted(titles, key=lambda i: titles[i].lower())
                    for term in terms_sorted_by_title:
                        if markdown:
                            markdown += '<hr>\n\n'
                        markdown += term_text[term] + '\n\n'
                    markdown = f'# <a id="tw-section-{section}"/>{self.section_titles[section]}\n\n' + markdown
                    markdown = self.fix_links(markdown, section, self.repo_owner)
                    output_file = os.path.join(self.output_dir, f'{section}.md')
                    write_file(output_file, markdown)
                    self.num_files_written += 1
                    config_file = os.path.join(self.source_dir, project.path, 'config.yaml')
                    if os.path.isfile(config_file):
                        copy(config_file, os.path.join(self.output_dir, 'config.yaml'))
                    elif project.path!='./':
                        self.warnings.append(f"Possible missing config.yaml file in {project.path} folder")
                output_file = os.path.join(self.output_dir, 'index.json')
                write_file(output_file, index_json)

        if self.num_files_written == 0:
            AppSettings.logger.error(f"tW preprocessor didn't write any markdown files")
            self.warnings.append("No tW source files discovered")
        else:
            AppSettings.logger.debug(f"tW preprocessor wrote {self.num_files_written} markdown files with {len(self.warnings)} warnings")
        AppSettings.logger.debug(f"tW preprocessor returning with {self.output_dir} = {os.listdir(self.output_dir)}")
        return self.num_files_written, self.warnings
    # end of TwPreprocessor run()


    def fix_links(self, content:str, section:str, repo_owner:str) -> str:
        """
        For tW
        """
        # convert tA RC links, e.g. rc://en/ta/man/translate/figs-euphemism
        #                           => https://git.door43.org/{repo_owner}/en_ta/translate/figs-euphemism/01.md
        content = re.sub(r'rc://([^/]+)/ta/([^/]+)/([^\s)\]\n$]+)',
                         rf'https://git.door43.org/{repo_owner}/\1_ta/src/branch/master/\3/01.md', content,
                         flags=re.IGNORECASE)
        # convert other RC links, e.g. rc://en/tn/help/1sa/16/02
        #                           => https://git.door43.org/{repo_owner}/en_tn/1sa/16/02.md
        content = re.sub(r'rc://([^/]+)/([^/]+)/([^/]+)/([^\s)\]\n$]+)',
                         rf'https://git.door43.org/{repo_owner}/\1_\2/src/branch/master/\4.md', content,
                         flags=re.IGNORECASE)
        # fix links to other sections within the same manual (only one ../ and a section name that matches section_link)
        # e.g. [covenant](../kt/covenant.md) => [covenant](#covenant)
        pattern = r'\]\(\.\.\/{0}\/([^/]+).md\)'.format(section)
        content = re.sub(pattern, r'](#\1)', content)
        # fix links to other sections within the same manual (only one ../ and a section name)
        # e.g. [commit](../other/commit.md) => [commit](other.html#commit)
        for s in TwPreprocessor.section_titles:
            pattern = re.compile(r'\]\(\.\./{0}/([^/]+).md\)'.format(s))
            replace = r']({0}.html#\1)'.format(s)
            content = re.sub(pattern, replace, content)
        # fix links to other sections that just have the section name but no 01.md page (preserve http:// links)
        # e.g. See [Verbs](figs-verb) => See [Verbs](#figs-verb)
        content = re.sub(r'\]\(([^# :/)]+)\)',
                         r'](#\1)', content)
        # convert URLs to links if not already
        content = re.sub(r'([^"(\[])((http|https|ftp)://[A-Z0-9/?&_.:=#-]+[A-Z0-9/?&_:=#-])',
                         r'\1[\2](\2)',
                         content, flags=re.IGNORECASE)
        # URLs wth just www at the start, no http
        content = re.sub(r'([^A-Z0-9"(/])(www\.[A-Z0-9/?&_.:=#-]+[A-Z0-9/?&_:=#-])',
                         r'\1[\2](http://\2)',
                         content, flags=re.IGNORECASE)
        return content
# end of class TwPreprocessor



class TnPreprocessor(Preprocessor):
    index_json:Dict[str,Any] = {
        'titles': {},
        'chapters': {},
        'book_codes': {}
    }

    def __init__(self, *args, **kwargs) -> None:
        super(TnPreprocessor, self).__init__(*args, **kwargs)
        self.book_filenames:List[str] = []
        self.title_cache = {}

    def is_multiple_jobs(self):
        return True

    def get_book_list(self):
        return self.book_filenames


    def run(self) -> Tuple[int, List[str]]:
        AppSettings.logger.debug(f"tN preprocessor starting with {self.source_dir} = {os.listdir(self.source_dir)} …")
        index_json = {
            'titles': {},
            'chapters': {},
            'book_codes': {}
        }
        try:
            language_id = self.rc.manifest['dublin_core']['language']['identifier']
            AppSettings.logger.debug(f"tN preprocessor: Got {language_id=}")
        except (KeyError, TypeError):
            language_id = 'en'
            AppSettings.logger.debug(f"tN preprocessor: Defaulted to {language_id=}")
        EXPECTED_TSV_SOURCE_TAB_COUNT = 8 # So there's one more column than this
        headers_re = re.compile('^(#+) +(.+?) *#*$', flags=re.MULTILINE)
        for project in self.rc.projects:
            AppSettings.logger.debug(f"tN preprocessor: Adjusting/Copying file(s) for '{project.identifier}' …")
            if project.identifier in BOOK_NAMES:
                book = project.identifier.lower()
                html_file = f'{BOOK_NUMBERS[book]}-{book.upper()}.html'
                index_json['book_codes'][html_file] = book
                name = BOOK_NAMES[book]
                index_json['titles'][html_file] = name
                # If there's a TSV file, copy it across
                found_tsv = False
                tsv_filename_end = f'{BOOK_NUMBERS[book]}-{book.upper()}.tsv'
                for this_filepath in glob(os.path.join(self.source_dir, '*.tsv')):
                    if this_filepath.endswith(tsv_filename_end): # We have the tsv file
                        found_tsv = True
                        AppSettings.logger.debug(f"tN preprocessor got {this_filepath}")
                        line_number = 1
                        lastB = lastC = lastV = None
                        field_id_list:List[str] = []
                        with open(this_filepath, 'rt') as tsv_source_file:
                            with open(os.path.join(self.output_dir, os.path.basename(this_filepath)), 'wt') as tsv_output_file:
                                for tsv_line in tsv_source_file:
                                    tsv_line = tsv_line.rstrip('\n')
                                    tab_count = tsv_line.count('\t')
                                    if line_number == 1:
                                        # AppSettings.logger.debug(f"TSV header line is '{tsv_line}")
                                        if tsv_line != 'Book	Chapter	Verse	ID	SupportReference	OrigQuote	Occurrence	GLQuote	OccurrenceNote':
                                            self.warnings.append(f"Unexpected TSV header line: '{tsv_line}' in {os.path.basename(this_filepath)}")
                                    elif tab_count != EXPECTED_TSV_SOURCE_TAB_COUNT:
                                        # NOTE: This is not added to warnings because that will be done at convert time (don't want double warnings)
                                        AppSettings.logger.debug(f"Unexpected line #{line_number} with {tab_count} tabs (expected {EXPECTED_TSV_SOURCE_TAB_COUNT}): '{tsv_line}'")
                                    B, C, V, field_id, _SupportReference, OrigQuote, _Occurrence, _GLQuote, OccurrenceNote = tsv_line.split('\t')
                                    if B!=lastB or C!=lastC or V!=lastV:
                                        field_id_list:List[str] = [] # IDs only need to be unique within each verse
                                        lastB, lastC, lastV = B, C, V
                                    if field_id in field_id_list:
                                        self.warnings.append(f"Duplicate ID at {B} {C}:{V} with '{field_id}'")
                                    field_id_list.append(field_id)
                                    if '://' in OccurrenceNote or '[[' in OccurrenceNote:
                                        OccurrenceNote = self.fix_links(f'{B} {C}:{V}', OccurrenceNote, self.repo_owner, language_id)
                                    if 'rc://' in OccurrenceNote:
                                        self.warnings.append(f"Unable to process link at {B} {C}:{V} in '{OccurrenceNote}'")
                                    tsv_output_file.write(f'{B}\t{C}\t{V}\t{OrigQuote}\t{OccurrenceNote}\n')
                                    line_number += 1
                        AppSettings.logger.info(f"Loaded {line_number:,} TSV lines from {os.path.basename(this_filepath)}.")
                        self.num_files_written += 1
                        break # Don't bother looking for other files since we found our TSV one for this book
                # NOTE: This code will create an .md file if there is a missing TSV file
                if not found_tsv: # Look for markdown or json .txt
                    markdown = ''
                    chapter_dirs = sorted(glob(os.path.join(self.source_dir, project.path, '*')))
                    markdown += f'# <a id="tn-{book}"/> {name}\n\n'
                    index_json['chapters'][html_file]:List[str] = []
                    for move_str in ['front', 'intro']:
                        self.move_to_front(chapter_dirs, move_str)
                    found_something = False
                    for chapter_dir in chapter_dirs:
                        chapter = os.path.basename(chapter_dir)
                        if chapter in self.ignoreFiles or chapter == 'manifest.json':
                            # NOTE: Would it have been better to check for file vs folder here (and ignore files) ???
                            continue
                        link = f'tn-chapter-{book}-{chapter.zfill(3)}'
                        index_json['chapters'][html_file].append(link)
                        markdown += f"""## <a id="{link}"/> {name} {chapter.lstrip('0')}\n\n"""
                        chunk_filepaths = sorted(glob(os.path.join(chapter_dir, '*.md')))
                        if chunk_filepaths: # We have .md files
                            # AppSettings.logger.debug(f"tN preprocessor: got {len(chunk_filepaths)} md chunk files: {chunk_filepaths}")
                            found_something = True
                            for move_str in ['front', 'intro']:
                                self.move_to_front(chunk_filepaths, move_str)
                            for chunk_idx, chunk_filepath in enumerate(chunk_filepaths):
                                if os.path.basename(chunk_filepath) in self.ignoreFiles:
                                    continue
                                start_verse = os.path.splitext(os.path.basename(chunk_filepath))[0].lstrip('0')
                                if chunk_idx < len(chunk_filepaths)-1:
                                    base_file_name = os.path.splitext(os.path.basename(chunk_filepaths[chunk_idx + 1]))[0]
                                    if base_file_name.isdigit():
                                        end_verse = str(int(base_file_name) - 1)
                                    else:
                                        end_verse = start_verse
                                else:
                                    chapter_str = chapter.lstrip('0')
                                    chapter_verses = BOOK_CHAPTER_VERSES[book]
                                    end_verse = chapter_verses[chapter_str] if chapter_str in chapter_verses else start_verse

                                start_verse_str = str(start_verse).zfill(3) if start_verse.isdigit() else start_verse
                                link = f'tn-chunk-{book}-{str(chapter).zfill(3)}-{start_verse_str}'
                                markdown += '### <a id="{0}"/>{1} {2}:{3}{4}\n\n'. \
                                    format(link, name, chapter.lstrip('0'), start_verse,
                                        '-'+end_verse if start_verse != end_verse else '')
                                try: text = read_file(chunk_filepath) + '\n\n'
                                except Exception as e:
                                    self.warnings.append(f"Error reading {os.path.basename(chunk_filepath)}: {e}")
                                    continue
                                text = headers_re.sub(r'\1## \2', text)  # This will bump any header down 2 levels
                                markdown += text
                        else: # See if there's .txt files (as no .md files found)
                            # NOTE: These seem to actually be json files (created by tS)
                            chunk_filepaths = sorted(glob(os.path.join(chapter_dir, '*.txt')))
                            # AppSettings.logger.debug(f"tN preprocessor: got {len(chunk_filepaths)} txt chunk files: {chunk_filepaths}")
                            if chunk_filepaths: found_something = True
                            for move_str in ['front', 'intro']:
                                self.move_to_front(chunk_filepaths, move_str)
                            for chunk_idx, chunk_filepath in enumerate(chunk_filepaths):
                                if os.path.basename(chunk_filepath) in self.ignoreFiles:
                                    # AppSettings.logger.debug(f"tN preprocessor: ignored {chunk_filepath}")
                                    continue
                                start_verse = os.path.splitext(os.path.basename(chunk_filepath))[0].lstrip('0')
                                if chunk_idx < len(chunk_filepaths)-1:
                                    base_file_name = os.path.splitext(os.path.basename(chunk_filepaths[chunk_idx + 1]))[0]
                                    if base_file_name.isdigit():
                                        end_verse = str(int(base_file_name) - 1)
                                    else:
                                        end_verse = start_verse
                                else:
                                    chapter_str = chapter.lstrip('0')
                                    chapter_verses = BOOK_CHAPTER_VERSES[book]
                                    end_verse = chapter_verses[chapter_str] if chapter_str in chapter_verses else start_verse

                                start_verse_str = str(start_verse).zfill(3) if start_verse.isdigit() else start_verse
                                link = f'tn-chunk-{book}-{str(chapter).zfill(3)}-{start_verse_str}'
                                markdown += '### <a id="{0}"/>{1} {2}:{3}{4}\n\n'. \
                                    format(link, name, chapter.lstrip('0'), start_verse,
                                        '-'+end_verse if start_verse != end_verse else '')
                                text = read_file(chunk_filepath)
                                try:
                                    json_data = json.loads(text)
                                except json.decoder.JSONDecodeError as e:
                                    # Clean-up the filepath for display (mostly removing /tmp folder names)
                                    adjusted_filepath = '/'.join(chunk_filepath.split('/')[6:]) #.replace('/./','/')
                                    error_message = f"Badly formed tN json file '{adjusted_filepath}': {e}"
                                    AppSettings.logger.error(error_message)
                                    self.warnings.append(error_message)
                                    json_data = {}
                                for tn_unit in json_data:
                                    if 'title' in tn_unit and 'body' in tn_unit:
                                        markdown += f"### {tn_unit['title']}\n\n"
                                        markdown += f"{tn_unit['body']}\n\n"
                                    else:
                                        self.warnings.append(f"Unexpected tN unit in {chunk_filepath}: {tn_unit}")
                    if not found_something:
                        self.warnings.append(f"tN Preprocessor didn't find any valid source files for {book}")
                    markdown = self.fix_links(book, markdown, self.repo_owner, language_id)
                    if 'rc://' in markdown:
                        self.warnings.append(f"Unable to all process 'rc://' links in {book}")
                    book_file_name = f'{BOOK_NUMBERS[book]}-{book.upper()}.md'
                    self.book_filenames.append(book_file_name)
                    file_path = os.path.join(self.output_dir, book_file_name)
                    # AppSettings.logger.debug(f"tN preprocessor: writing {file_path} with: {markdown}")
                    write_file(file_path, markdown)
                    self.num_files_written += 1
            else:
                AppSettings.logger.debug(f"TnPreprocessor: extra project found: {project.identifier}")

        if self.num_files_written == 0:
            AppSettings.logger.error(f"tN preprocessor didn't write any markdown files")
            self.warnings.append("No tN source files discovered")
        else:
            AppSettings.logger.debug(f"tN preprocessor wrote {self.num_files_written} markdown files with {len(self.warnings)} warnings")

        # Write out index.json
        output_file = os.path.join(self.output_dir, 'index.json')
        write_file(output_file, index_json)
        # AppSettings.logger.debug(f"tN Preprocessor returning with {self.output_dir} = {os.listdir(self.output_dir)}")
        return self.num_files_written, self.warnings
    # end of TnPreprocessor run()


    def move_to_front(self, files:List[str], move_str:str) -> None:
        if files:
            last_file = files[-1]
            if move_str in last_file:  # move intro to front
                files.pop()
                files.insert(0, last_file)


    compiled_re1 = re.compile(r'\[\[https://([^ ]+?)/src/branch/master/([^ .]+?)/01\.md\]\]',
                                            flags=re.IGNORECASE)
    compiled_re2 = re.compile(r'\[\[https://([^ ]+?)/src/branch/master/([^ .]+?)\.md\]\]',
                                            flags=re.IGNORECASE)
    def fix_links(self, BCV:str, content:str, repo_owner:str, language_code:str) -> str:
        """
        For tN (MD and TSV)
        """
        # assert content.count('(') == content.count(')')
        # assert content.count('[') == content.count(']')

        # Convert wildcard tA RC links, e.g. rc://*/ta/man/translate/figs-euphemism
        #               => https://git.door43.org/{repo_owner}/LL_ta/src/branch/master/translate/figs-euphemism/01.md
        # content1 = content
        content = re.sub(r'rc://\*/ta/([^/]+)/([^\s)\]\n$]+)',
                         rf'https://git.door43.org/{repo_owner}/{language_code}_ta/src/branch/master/\2/01.md',
                         content, flags=re.IGNORECASE)
        # if content != content1: print(f"1: was {content1}\nnow {content}")
        # Convert non-wildcard tA RC links, e.g. rc://en/ta/man/translate/figs-euphemism
        #               => https://git.door43.org/{repo_owner}/en_ta/src/branch/master/translate/figs-euphemism/01.md
        # content2 = content
        content = re.sub(r'rc://([^/]+)/ta/([^/]+)/([^\s)\]\n$]+)',
                         rf'https://git.door43.org/{repo_owner}/\1_ta/src/branch/master/\3/01.md',
                         content, flags=re.IGNORECASE)
        # if content != content2: print(f"2: was {content2}\nnow {content}")
        # Convert other wildcard RC links, e.g. rc://*/tn/help/1sa/16/02
        #               => https://git.door43.org/{repo_owner}/LL_tn/src/branch/master/1sa/16/02.md
        # content3 = content
        content = re.sub(r'rc://\*/([^/]+)/([^/]+)/([^\s)\]\n$]+)',
                         rf'https://git.door43.org/{repo_owner}/{language_code}_\1/src/branch/master/\3.md',
                         content, flags=re.IGNORECASE)
        # if content != content3: print(f"3: was {content3}\nnow {content}")
        # Convert other non-wildcard RC links, e.g. rc://en/tn/help/1sa/16/02
        #               => https://git.door43.org/{repo_owner}/en_tn/src/branch/master/1sa/16/02.md
        # content4 = content
        content = re.sub(r'rc://([^/]+)/([^/]+)/([^/]+)/([^\s)\]\n$]+)',
                         rf'https://git.door43.org/{repo_owner}/\1_\2/src/branch/master/\4.md',
                         content, flags=re.IGNORECASE)
        # if content != content4: print(f"4: was {content4}\nnow {content}")

        # Fix links to other sections that just have the section name but no 01.md page (preserve http:// links)
        # e.g. See [Verbs](figs-verb) => See [Verbs](#figs-verb)
        # content5 = content
        content = re.sub(r'\]\(([^# :/)]+)\)',
                         r'](#\1)', content)
        # if content != content5: print(f"5: was {content5}\nnow {content}")
        # Convert URLs to links if not already
        # content6 = content
        content = re.sub(r'([^"(\[])((http|https|ftp)://[A-Z0-9/?&_.:=#-]+[A-Z0-9/?&_:=#-])',
                         r'\1[\2](\2)',
                         content, flags=re.IGNORECASE)
        # if content != content6: print(f"6: was {content6}\nnow {content}")
        # URLS wth just www at the start, no http
        # content7 = content
        content = re.sub(r'([^A-Z0-9"(/])(www\.[A-Z0-9/?&_.:=#-]+[A-Z0-9/?&_:=#-])',
                         r'\1[\2](http://\2)',
                         content, flags=re.IGNORECASE)
        # if content != content7: print(f"7: was {content7}\nnow {content}")

        # [[Links inside double-brackets]] => [short-text](url)
        # content8 = content
        # content = re.sub(r'\[\[https://(.+?)/src/branch/master/(.+?)/01\.md\]\]',
        #                  r'[\2](https://\1/src/branch/master/\2/01\.md)',
        #                  content, flags=re.IGNORECASE)
        start_index = 0
        bad_file_count = 0
        while (match := TnPreprocessor.compiled_re1.search(content, start_index)):
            # print(f"Match8a: {match.start()}:{match.end()} '{content[match.start():match.end()]}'")
            # print(f"Match8b: {match.groups()}")
            file_url = f'https://{match.group(1)}/src/branch/master/{match.group(2)}/01.md'
            # print(f"Match8c: file URL='{file_url}'")
            try:
                link_text = self.title_cache[file_url]
            except:
                repo_type = match.group(1).split('_')[-1].upper() # e.g., TA or TW
                link_text = f'{repo_type}:{match.group(2)}' # default
                title_file_url = file_url.replace('src','raw').replace('01','title')
                # print(f"Match8d: title file URL='{title_file_url}'")
                try:
                    file_contents = get_url(title_file_url)
                except Exception as e:
                    bad_file_count += 1
                    if bad_file_count < 15 and len(self.warnings) < 200:
                        AppSettings.logger.debug(f"tN {BCV} fix_linkA fetching {title_file_url} got: {e}")
                        self.warnings.append(f"{BCV} error with tA '{match.group(2)}' link {title_file_url}: {e}")
                    link_text = self.title_cache[file_url] = f'INVALID {match.group(2)}'
                    file_contents = None
                if file_contents:
                    link_text = file_contents
                    if '\n' in link_text or '\r' in link_text or '\t' in link_text:
                        AppSettings.logger.debug(f"tN {BCV} {title_file_url} contains illegal chars: {link_text!r}")
                        self.warnings.append(f"{BCV} unwanted characters in {title_file_url} title: {link_text!r}")
                        link_text = link_text.strip().replace('\n','NL').replace('\r','CR').replace('\t','TAB')
                    if not link_text:
                        AppSettings.logger.debug(f"tN {BCV} got effectively blank title from {title_file_url}")
                        self.warnings.append(f"{BCV} title in {title_file_url} seems blank")
                        link_text = f'BLANK from {match.group(2)}'
                    self.title_cache[file_url] = link_text
                    # print(f"cache length = {len(self.title_cache)}")
            # new_link_markdown = f'[{link_text}]({file_url})'
            # print(f"Match8e: New tA link = {new_link_markdown}")
            content = f'{content[:match.start()]}[{link_text}]({file_url}){content[match.end():]}'
            start_index = match.start() + 1
        # if content != content8: print(f"8: was {content8}\nnow {content}")
        # assert content.count('(') == content.count(')')
        # assert content.count('[') == content.count(']')

        # content9 = content
        # content = re.sub(r'\[\[https://([^ ]+?)/src/branch/master/([^ .]+?)\.md\]\]',
        #                  r'[\2](https://\1/src/branch/master/\2\.md)',
        #                  content, flags=re.IGNORECASE)
        start_index = 0
        bad_file_count = 0
        while (match := TnPreprocessor.compiled_re2.search(content, start_index)):
            # print(f"Match9a: {match.start()}:{match.end()} '{content[match.start():match.end()]}'")
            # print(f"Match9b: {match.groups()}")
            file_url = f'https://{match.group(1)}/src/branch/master/{match.group(2)}.md'
            # print(f"Match9c: file URL='{file_url}'")
            try:
                link_text = self.title_cache[file_url]
            except:
                repo_type = match.group(1).split('_')[-1].upper() # e.g., TA or TW
                link_text = f'{repo_type}:{match.group(2)}' # default
                title_file_url = file_url.replace('src','raw')
                # print(f"Match9d: title file URL='{title_file_url}'")
                try:
                    file_contents = get_url(title_file_url)
                except Exception as e:
                    bad_file_count += 1
                    if bad_file_count < 15 and len(self.warnings) < 200:
                        AppSettings.logger.debug(f"tN {BCV} fix_linkB fetching {title_file_url} got: {e}")
                        self.warnings.append(f"{BCV} error with tW '{match.group(2)}' link {title_file_url}: {e}")
                    link_text = self.title_cache[file_url] = f'INVALID {match.group(2)}'
                    file_contents = None
                if file_contents:
                    try:
                        link_text = file_contents.split('\n',1)[0].lstrip() # Get the first line
                        if link_text.startswith('# '):
                            link_text = link_text[2:]
                        if '\n' in link_text or '\r' in link_text or '\t' in link_text:
                            AppSettings.logger.debug(f"tN {BCV} {title_file_url} contains illegal chars: {link_text!r}")
                            self.warnings.append(f"{BCV} unwanted characters in {title_file_url} title: {link_text!r}")
                            link_text = link_text.strip().replace('\n','NL').replace('\r','CR').replace('\t','TAB')
                        if not link_text:
                            AppSettings.logger.debug(f"tN {BCV} got effectively blank title from {title_file_url}")
                            self.warnings.append(f"{BCV} title in {title_file_url} seems blank")
                            link_text = f'BLANK from {match.group(2)}'
                        self.title_cache[file_url] = link_text
                        # print(f"cache length = {len(self.title_cache)}")
                    except Exception as e:
                        AppSettings.logger.debug(f"tN fix_linkB getting title from {file_contents} got: {e}")
            # new_link_markdown = f'[{link_text}]({file_url})'
            # print(f"Match9e: New tW link = {new_link_markdown}")
            content = f'{content[:match.start()]}[{link_text}]({file_url}){content[match.end():]}'
            start_index = match.start() + 1
        # if content != content9: print(f"9: was {content9}\nnow {content}")

        # assert content.count('(') == content.count(')')
        # assert content.count('[') == content.count(']')
        return content
# end of class TnPreprocessor



class LexiconPreprocessor(Preprocessor):

    # def __init__(self, *args, **kwargs) -> None:
    #     super(LexiconPreprocessor, self).__init__(*args, **kwargs)


    def compile_lexicon_entry(self, project, folder):
        """
        Recursive section markdown creator
        Expects a folder containing only one file: 01.md

        :param project:
        :param str folder:
        :return: markdown str
        """
        # AppSettings.logger.debug(f"compile_lexicon_entry for {project} {folder} …")
        content_folderpath = os.path.join(self.source_dir, project.path, folder)
        file_list = os.listdir(content_folderpath)
        if len(file_list) != 1: # expecting '01.md'
            AppSettings.logger.error(f"Unexpected files in {folder}: {file_list}")
        markdown = "" # f"# {folder}\n" # Not needed coz Strongs number is included inside the file
        content_filepath = os.path.join(content_folderpath, '01.md')
        if os.path.isfile(content_filepath):
            try: content = read_file(content_filepath)
            except Exception as e:
                msg = f"Error reading {os.path.basename(content_folderpath)}/01.md: {e}"
                AppSettings.logger.error(msg)
                self.warnings.append(msg)
                content = None
        else:
            msg = f"compile_lexicon_entry couldn't find any files for {folder}"
            AppSettings.logger.error(msg)
            self.warnings.append(msg)
            content = None
        if content:
            # markdown += f'{content}\n\n'
            markdown = f'{content}\n'
        return markdown
    # end of compile_lexicon_entry(self, project, section, level)


    def run(self) -> Tuple[int, List[str]]:
        AppSettings.logger.debug(f"Lexicon preprocessor starting with {self.source_dir} = {os.listdir(self.source_dir)} …")
        for project in self.rc.projects:
            project_path = os.path.join(self.source_dir, project.path)
            print("project_path", project_path)

            AppSettings.logger.debug(f"Lexicon preprocessor: Copying files for '{project.identifier}' …")

            # Even though the .md files won't be converted, they still need to be copied
            #   so they can be linted
            for something in sorted(os.listdir(project_path)):
                # something can be a file or a folder containing the markdown file
                if os.path.isdir(os.path.join(project_path, something)) \
                and something not in LexiconPreprocessor.ignoreDirectories:
                    # Entries are in separate folders (like en_ugl)
                    entry_markdown = self.compile_lexicon_entry(project, something)
                    # entry_markdown = self.fix_entry_links(entry_markdown)
                    write_file(os.path.join(self.output_dir, f'{something}.md'), entry_markdown)
                    self.num_files_written += 1
                elif os.path.isfile(os.path.join(project_path, something)) \
                and something not in LexiconPreprocessor.ignoreFiles \
                and something != 'index.md':
                    # Entries are in the main folder in named .md files
                    # copy(os.path.join(project_path, something), self.output_dir)
                    # entry_markdown = read_file(something)
                    with open(os.path.join(project_path, something), 'rt') as ef:
                        entry_markdown = ef.read()
                    # entry_markdown = self.fix_entry_links(entry_markdown)
                    write_file(os.path.join(self.output_dir, f'{something}.md'), entry_markdown)
                    self.num_files_written += 1

            # Now do the special stuff
            # TODO: Ideally most of the English strings below should be translated for other lexicons
            # Create two index files -- one by Strongs number (the original) and one by lemma
            index_filepath = os.path.join(project_path, 'index.md')
            if os.path.isfile(index_filepath):
                with open(index_filepath, 'rt') as ixf:
                    index_markdown = ixf.read()
                index_markdown = self.fix_index_links(index_markdown)
                number_index_markdown = f"# Number index\n\n{index_markdown}"
                write_file(os.path.join(self.output_dir, 'number_index.md'), number_index_markdown)
                self.num_files_written += 1
                word_index_markdown = f"# Word/lemma index\n\n{self.change_index_entries(project_path, index_markdown)}"
                write_file(os.path.join(self.output_dir, 'word_index.md'), word_index_markdown)
                self.num_files_written += 1

            # Copy the README as index.md, adding some extra links at top and bottom
            readme_filepath = os.path.join(self.source_dir, 'README.md')
            if os.path.isfile(readme_filepath):
                with open(readme_filepath, 'rt') as rmf:
                    readme_markdown = rmf.read()
            else:
                AppSettings.logger.error("Lexicon preprocessor cannot find README.md")
                readme_markdown = "No README.md found\n"
            link1 = "* [Lexicon entries (by number)](number_index.html)\n"
            link2 = "* [Lexicon entries (by word)](word_index.html)\n"
            readme_markdown = f"{link1}{link2}\n\n{readme_markdown}\n{link1}{link2}"
            write_file(os.path.join(self.output_dir, 'index.md'), readme_markdown)
            self.num_files_written += 1

        if self.num_files_written == 0:
            AppSettings.logger.error("Lexicon preprocessor didn't write any markdown files")
            self.warnings.append("No lexicon source files discovered")
        else:
            AppSettings.logger.debug(f"Lexicon preprocessor wrote {self.num_files_written} markdown files with {len(self.warnings)} warnings")

        str_list = str(os.listdir(self.output_dir))
        str_list_adjusted = str_list if len(str_list)<1500 \
                                else f'{str_list[:1000]} …… {str_list[-500:]}'
        AppSettings.logger.debug(f"Lexicon preprocessor returning with {self.output_dir} = {str_list_adjusted}")
        return self.num_files_written, self.warnings
    # end of LexiconPreprocessor run()


    def fix_index_links(self, content):
        """
        Changes the actual links to point to the original .md files.
        """
        # AppSettings.logger.debug("LexiconPreprocessor.fix_index_links(…)…")

        newLines:List[str] = []
        for line in content.split('\n'):
            # print("line:", line)
            if '](' in line:
                bits = line.split('](', 1)
                # print("bits", bits)
                assert bits[0][0]=='[' and bits[1][-1]==')'
                strongs, filenameOrFolder = bits[0][1:], bits[1][:-1]
                # print("strongs", strongs, "filenameOrFolder", filenameOrFolder)
                assert filenameOrFolder.startswith('./')
                filenameOrFolder = f"{self.commit_url}/content/{filenameOrFolder[2:]}" \
                                    .replace('/commit/', '/raw/commit/') \
                                    .replace('/commit/master/', '/branch/master/')
                # print("filenameOrFolder", filenameOrFolder)
                line = f"[{strongs[:-3] if strongs.endswith('.md') else strongs}]" \
                       f"({filenameOrFolder}{'/01.md' if not filenameOrFolder.endswith('.md') else ''})"
            newLines.append(line)
        content = '\n'.join(newLines)
        return content
    # end of LexiconPreprocessor fix_index_links(content)


    def change_index_entries(self, project_path:str, content:str) -> str:
        """
        """
        # AppSettings.logger.debug(f"LexiconPreprocessor.change_index_entries({project_path}, …)…")

        # Change Strongs numbers to lemma entries
        newLines:List[str] = []
        for line in content.split('\n'):
            # print("line:", line)
            if '](' in line:
                bits = line.split('](', 1)
                # print("bits", bits)
                assert bits[0][0]=='[' and bits[1][-1]==')'
                strongs, fileURL = bits[0][1:], bits[1][:-1]
                # print("strongs", strongs, ' ', "fileURL", fileURL)
                fileURLbits = fileURL.split('/')
                filepath = os.path.join(project_path, fileURLbits[-1])
                # print("filepath1", filepath)
                if not os.path.isfile(filepath):
                    filepath = os.path.join(project_path, fileURLbits[-2], fileURLbits[-1])
                    # print("filepath2", filepath)
                title = None
                try:
                    with open(filepath, 'rt') as lex_file:
                        lex_content = lex_file.read()
                except FileNotFoundError:
                    AppSettings.logger.error(f"LexiconPreprocessor.change_index_entries could not find {filepath}")
                    self.warnings.append(f"No lexicon entry file found for {strongs}")
                    lex_content = None
                    title = "-BAD-"
                if lex_content and lex_content[0]=='#' and lex_content[1]==' ':
                    title = lex_content[2:8].replace('\n', ' ').replace(' ', ' ') # non-break space
                # print("title", repr(title))
                if lex_content and title is None: # Why?
                    AppSettings.logger.error("LexiconPreprocessor.change_index_entries could not find lemma string")
                    title = strongs
                line = f"[{title:6}]({fileURL})"
            newLines.append(line)
        content = '\n'.join(newLines)
        return content
    # end of LexiconPreprocessor change_index_entries(project_path, content)
# end of class LexiconPreprocessor
