import os
from glob import glob

from bs4 import BeautifulSoup

from global_settings.global_settings import GlobalSettings
from general_tools import file_utils
from general_tools.file_utils import write_file
from resource_container.ResourceContainer import RC
from general_tools.file_utils import load_yaml_object



def do_template(repo_subject, source_dir, output_dir, template_file):
    """
    Only used by test_templaters.py
    """
    templater = init_template(repo_subject, source_dir, output_dir, template_file)
    return templater.run()


def init_template(repo_subject, source_dir, output_dir, template_file):
    """
    Tries to determine the correct templater for the appropriate repo_subject
    """
    # GlobalSettings.logger.debug(f"init_template({repo_subject})")
    if repo_subject in ('Generic_Markdown','Open_Bible_Stories',
                                        'OBS_Translation_Notes','OBS_Translation_Questions',):
        GlobalSettings.logger.info(f"Using ObsTemplater for '{repo_subject}' …")
        templater = ObsTemplater(repo_subject, source_dir, output_dir, template_file)
    elif repo_subject in ('Translation_Academy',):
        GlobalSettings.logger.info(f"Using TaTemplater for '{repo_subject}' …")
        templater = TaTemplater(repo_subject, source_dir, output_dir, template_file)
    elif repo_subject in ('Translation_Questions',):
        GlobalSettings.logger.info(f"Using TqTemplater for '{repo_subject}' …")
        templater = TqTemplater(repo_subject, source_dir, output_dir, template_file)
    elif repo_subject in ('Translation_Words',):
        GlobalSettings.logger.info(f"Using TwTemplater for '{repo_subject}' …")
        templater = TwTemplater(repo_subject, source_dir, output_dir, template_file)
    elif repo_subject in ('Translation_Notes',):
        GlobalSettings.logger.info(f"Using TnTemplater for '{repo_subject}' …")
        templater = TnTemplater(repo_subject, source_dir, output_dir, template_file)
    else:
        if repo_subject in ('Bible', 'Aligned_Bible', 'Greek_New_Testament', 'Hebrew_Old_Testament'):
            GlobalSettings.logger.info(f"Using BibleTemplater for '{repo_subject}' …")
        else:
            GlobalSettings.logger.error(f"Choosing BibleTemplater for unexpected repo_subject='{repo_subject}'")
        templater = BibleTemplater(repo_subject, source_dir, output_dir, template_file)
    return templater



class Templater:
    NO_NAV_TITLES = ['', 'Conversion requested…', 'Conversion started…', 'Conversion successful',
                     'Conversion successful with warnings', 'Index']

    def __init__(self, repo_subject, source_dir, output_dir, template_file):
        self.repo_subject = repo_subject
        # This templater_CSS_class is used to set the html body class
        #   so it must match the css in door43.org/_site/css/project-page.css
        assert self.templater_CSS_class # Must be set by subclass
        GlobalSettings.logger.debug(f"Using '{self.templater_CSS_class}' templater…")
        if self.templater_CSS_class not in ('obs','ta','tq','tw','tn','bible'):
            GlobalSettings.logger.error(f"Unexpected templater_CSS_class='{self.templater_CSS_class}'")
        self.classes = [] # These get appended to the templater_CSS_class

        self.source_dir = source_dir  # Local directory
        self.output_dir = output_dir  # Local directory
        self.template_file = template_file  # Local file of template

        self.files = sorted(glob(os.path.join(self.source_dir, '*.html')))
        self.rc = None
        self.template_html = ''
        self.already_converted = []
        self.titles = {}
        self.chapters = {}
        self.book_codes = {}


    def run(self):
        # GlobalSettings.logger.debug("Templater.run()")
        # Get the resource container
        self.rc = RC(self.source_dir)
        with open(self.template_file) as template_file:
            self.template_html = template_file.read()
            soup = BeautifulSoup(self.template_html, 'html.parser')
            soup.body['class'] = soup.body.get('class', []) + [self.templater_CSS_class]
            if self.classes:
                for some_class in self.classes: # Check that we don't double unnecessarily
                    assert some_class != self.templater_CSS_class
                soup.body['class'] = soup.body.get('class', []) + self.classes
            GlobalSettings.logger.info(f"Have {self.template_file.split('/')[-1]} body class(es)={soup.body.get('class', [])}")
            self.template_html = str(soup)
        self.apply_template()
        return True


    @staticmethod
    def build_left_sidebar(filename=None):
        html = """
            <nav class="affix-top hidden-print hidden-xs hidden-sm" id="left-sidebar-nav">
                <div class="nav nav-stacked" id="revisions-div">
                    <h1>Revisions</h1>
                    <table width="100%" id="revisions"></table>
                </div>
            </nav>
            """
        return html


    def build_right_sidebar(self, filename=None):
        html = self.build_page_nav(filename)
        return html


    def build_page_nav(self, filename=None):
        html = """
            <nav class="affix-top hidden-print hidden-xs hidden-sm content-nav" id="right-sidebar-nav">
              <ul id="sidebar-nav" class="nav nav-stacked">
                <li><h1>Navigation</h1></li>
            """
        for fname in self.files:
            key = os.path.basename(fname)
            title = ""
            if key in self.titles:
                title = self.titles[key]
            if title in self.NO_NAV_TITLES:
                continue
            if filename != fname:
                html += f'<li><a href="{os.path.basename(fname)}">{title}</a></li>'
            else:
                html += f'<li>{title}</li>'
        html += """
                </ul>
            </nav>
            """
        return html


    def get_page_navigation(self):
        for fname in self.files:
            key = os.path.basename(fname)
            if key in self.titles:  # skip if we already have data
                continue

            with open(fname, 'r') as f:
                soup = BeautifulSoup(f, 'html.parser')
            if soup.select('div#content h1'):
                title = soup.select('div#content h1')[0].text.strip()
            else:
                title = os.path.splitext(os.path.basename(fname))[0].replace('_', ' ').capitalize()

            self.titles[key] = title


    def apply_template(self):
        # GlobalSettings.logger.debug("Templater.apply_template()")
        language_code = self.rc.resource.language.identifier
        language_name = self.rc.resource.language.title
        language_dir = self.rc.resource.language.direction
        resource_title = self.rc.resource.title

        self.get_page_navigation()

        heading = f'{language_name}: {resource_title}'
        title = ''
        canonical = ''

        # soup is the template that we will replace content of for every file
        soup = BeautifulSoup(self.template_html, 'html.parser')
        left_sidebar_div = soup.body.find('div', id='left-sidebar')
        outer_content_div = soup.body.find('div', id='outer-content')
        right_sidebar_div = soup.body.find('div', id='right-sidebar')

        # Find the outer-content div in the template
        if not outer_content_div:
            raise Exception('No div tag with id "outer-content" was found in the template')

        # Get the canonical UTL
        if not canonical:
            links = soup.head.find_all('link[rel="canonical"]')
            if len(links) == 1:
                canonical = links[0]['href']

        # Loop through the html files
        for filename in self.files:
            if filename not in self.already_converted:
                GlobalSettings.logger.debug(f"Applying template to {filename} …")

                # Read the downloaded file into a dom abject
                with open(filename, 'r') as f:
                    file_soup = BeautifulSoup(f, 'html.parser')

                # get the title from the raw html file
                if not title and file_soup.head and file_soup.head.title:
                    title = file_soup.head.title.text
                else:
                    title = os.path.basename(filename)

                # get the language code, if we haven't yet
                if not language_code:
                    if 'lang' in file_soup.html:
                        language_code = file_soup.html['lang']
                    else:
                        language_code = 'en'

                # get the body of the raw html file
                if not file_soup.body:
                    body = BeautifulSoup('<div>No content</div>', 'html.parser')
                else:
                    body = BeautifulSoup(''.join(['%s' % x for x in file_soup.body.contents]), 'html.parser')

                # insert new HTML into the template
                outer_content_div.clear()
                outer_content_div.append(body)
                soup.html['lang'] = language_code
                soup.html['dir'] = language_dir

                soup.head.title.clear()
                soup.head.title.append(heading+' - '+title)

                # set the page heading
                heading_span = soup.body.find('span', id='h1')
                heading_span.clear()
                heading_span.append(heading)

                if left_sidebar_div:
                    left_sidebar_html = self.build_left_sidebar(filename)
                    left_sidebar = BeautifulSoup(left_sidebar_html, 'html.parser').nav.extract()
                    left_sidebar_div.clear()
                    left_sidebar_div.append(left_sidebar)

                if right_sidebar_div:
                    right_sidebar_div.clear()
                    right_sidebar_html = self.build_right_sidebar(filename)
                    if right_sidebar_html:
                        right_sidebar = BeautifulSoup(right_sidebar_html, 'html.parser')
                        if right_sidebar and right_sidebar.nav:
                            right_sidebar_nav = right_sidebar.nav.extract()
                            right_sidebar_div.append(right_sidebar_nav)

                # Render the html as a unicode string
                html = str(soup)

                # fix the footer message, removing the title of this page in parentheses as it doesn't get filled
                html = html.replace(
                    '("<a xmlns:dct="http://purl.org/dc/terms/" href="https://live.door43.org/templates/project-page.html" rel="dct:source">{{ HEADING }}</a>") ',
                    '')
                # update the canonical URL - it is in several different locations
                html = html.replace(canonical, canonical.replace('/templates/', f'/{language_code}/'))

                # Replace HEADING with page title in footer
                html = html.replace('{{ HEADING }}', title)

                # write to output directory
                out_file = os.path.join(self.output_dir, os.path.basename(filename))
                GlobalSettings.logger.debug(f'Writing {out_file} …')
                # write_file(out_file, html.encode('ascii', 'xmlcharrefreplace'))
                write_file(out_file, html)

            else:  # if already templated, need to update navigation bar
                # Read the templated file into a dom abject
                with open(filename, 'r') as f:
                    soup = BeautifulSoup(f, 'html.parser')

                right_sidebar_div = soup.body.find('div', id='right-sidebar')
                if right_sidebar_div:
                    right_sidebar_html = self.build_right_sidebar(filename)
                    right_sidebar = BeautifulSoup(right_sidebar_html, 'html.parser').nav.extract()
                    right_sidebar_div.clear()
                    right_sidebar_div.append(right_sidebar)

                    # render the html as a unicode string
                    html = str(soup)

                    # write to output directory
                    out_file = os.path.join(self.output_dir, os.path.basename(filename))
                    GlobalSettings.logger.debug(f'Updating nav in {out_file} …')
                    # write_file(out_file, html.encode('ascii', 'xmlcharrefreplace'))
                    write_file(out_file, html)
# end of class Templater


class ObsTemplater(Templater):
    def __init__(self, *args, **kwargs):
        self.templater_CSS_class = 'obs'
        super(ObsTemplater, self).__init__(*args, **kwargs)
        if self.repo_subject == 'OBS_Translation_Notes':
            self.classes=['tn']
        elif self.repo_subject == 'OBS_Translation_Questions':
            self.classes=['tq']


class TqTemplater(Templater):
    def __init__(self, *args, **kwargs):
        self.templater_CSS_class = 'tq'
        super(TqTemplater, self).__init__(*args, **kwargs)
        index = file_utils.load_json_object(os.path.join(self.source_dir, 'index.json'))
        if index:
            self.titles = index['titles']
            self.chapters = index['chapters']
            self.book_codes = index['book_codes']

    def get_page_navigation(self):
        for fname in self.files:
            key = os.path.basename(fname)
            if key in self.titles:  # skip if we already have data
                continue
            filebase = os.path.splitext(os.path.basename(fname))[0]
            # Getting the book code for HTML tag references
            fileparts = filebase.split('-')
            if len(fileparts) == 2:
                # Assuming filename of ##-<name>.usfm, such as 01-GEN.usfm
                book_code = fileparts[1].lower()
            else:
                # Assuming filename of <name.usfm, such as GEN.usfm
                book_code = fileparts[0].lower()
            book_code.replace(' ', '-').replace('.', '-')  # replacing spaces and periods since used as tag class
            with open(fname, 'r') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            if soup.select('div#content h1'):
                title = soup.select('div#content h1')[0].text.strip()
            else:
                title = f'{book_code}.'
            self.titles[key] = title
            self.book_codes[key] = book_code
            chapters = soup.find_all('h2')
            self.chapters[key] = [c['id'] for c in chapters]


    def build_page_nav(self, filename=None):
        html = """
        <nav class="hidden-print hidden-xs hidden-sm content-nav" id="right-sidebar-nav">
            <ul id="sidebar-nav" class="nav nav-stacked books panel-group">
            """
        for fname in self.files:
            key = os.path.basename(fname)
            book_code = ""
            if key in self.book_codes:
                book_code = self.book_codes[key]
            title = ""
            if key in self.titles:
                title = self.titles[key]
            if title in self.NO_NAV_TITLES:
                continue
            html += f"""
                <div class="panel panel-default">
                    <div class="panel-heading">
                        <h4 class="panel-title">
                            <a class="accordion-toggle" data-toggle="collapse" data-parent="#sidebar-nav" href="#collapse{book_code}">{title}</a>
                        </h4>
                    </div>
                    <div id="collapse{book_code}" class="panel-collapse collapse{' in' if fname == filename else ''}">
                        <ul class="panel-body chapters">
                    """
            chapters = {}
            if key in self.chapters:
                chapters = self.chapters[key]
            for chapter in chapters:
                chapter_parts = chapter.split('-')
                label = chapter if len(chapter_parts) < 4 else chapter_parts[3].lstrip('0')
                html += f"""
                       <li class="chapter"><a href="{os.path.basename(fname) if fname != filename else ''}#{chapter}">{label}</a></li>
                    """
            html += """
                        </ul>
                    </div>
                </div>
                    """
        html += """
            </ul>
        </nav>
            """
        return html
# end of class TqTemplater



class TwTemplater(Templater):
    def __init__(self, *args, **kwargs):
        self.templater_CSS_class = 'tw'
        super(TwTemplater, self).__init__(*args, **kwargs)
        index = file_utils.load_json_object(os.path.join(self.source_dir, 'index.json'))
        if index:
            self.titles = index['titles']
            self.chapters = index['chapters']


    def build_page_nav(self, filename=None):
        if not self.files or not self.titles or not self.chapters:
            return ""
        html = """
            <nav class="hidden-print hidden-xs hidden-sm content-nav" id="right-sidebar-nav">
                <ul class="nav nav-stacked">
        """
        for fname in self.files:
            key = os.path.basename(fname)
            section = os.path.splitext(key)[0]
            html += f"""
                    <li{' class="active"' if fname == filename else ''}><a href="{key if fname != filename else ''}#tw-section-{section}">{self.titles[key]}</a>
                        <a class="content-nav-expand collapsed" data-target="#section-{section}-sub" data-toggle="collapse" href="#"></a>
                        <ul class="collapse" id="section-{section}-sub">
            """
            titles = self.chapters[key]
            terms_sorted_by_title = sorted(titles, key=lambda i: titles[i].lower())
            for term in terms_sorted_by_title:
                html += f"""
                            <li><a href="{key if fname != filename else ''}#{term}">{titles[term]}</a></li>
                """
            html += """
                        </ul>
                    </li>
            """
        html += """
                </ul>
            </nav>
        """
        return html
# end of class TwTemplater



class TnTemplater(Templater):
    def __init__(self, *args, **kwargs):
        self.templater_CSS_class = 'tn'
        super(TnTemplater, self).__init__(*args, **kwargs)
        index = file_utils.load_json_object(os.path.join(self.source_dir, 'index.json'))
        if index:
            self.titles = index['titles']
            self.chapters = index['chapters']
            self.book_codes = index['book_codes']


    def get_page_navigation(self):
        for fname in self.files:
            key = os.path.basename(fname)
            if key in self.titles:  # skip if we already have data
                continue
            filebase = os.path.splitext(os.path.basename(fname))[0]
            # Getting the book code for HTML tag references
            fileparts = filebase.split('-')
            if len(fileparts) == 2:
                # Assuming filename of ##-<name>.usfm, such as 01-GEN.usfm
                book_code = fileparts[1].lower()
            else:
                # Assuming filename of <name.usfm, such as GEN.usfm
                book_code = fileparts[0].lower()
            book_code.replace(' ', '-').replace('.', '-')  # replacing spaces and periods since used as tag class
            with open(fname, 'r') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            if soup.select('div#content h1'):
                title = soup.select('div#content h1')[0].text.strip()
            else:
                title = f'{book_code}.'
            self.titles[key] = title
            self.book_codes[key] = book_code
            chapters = soup.find_all('h2')
            self.chapters[key] = [c['id'] for c in chapters]


    def build_page_nav(self, filename=None):
        html = """
        <nav class="hidden-print hidden-xs hidden-sm content-nav" id="right-sidebar-nav">
            <ul id="sidebar-nav" class="nav nav-stacked books panel-group">
            """
        for fname in self.files:
            key = os.path.basename(fname)
            book_code = ""
            if key in self.book_codes:
                book_code = self.book_codes[key]
            title = ""
            if key in self.titles:
                title = self.titles[key]
            if title in self.NO_NAV_TITLES:
                continue
            html += f"""
                <div class="panel panel-default">
                    <div class="panel-heading">
                        <h4 class="panel-title">
                            <a class="accordion-toggle" data-toggle="collapse" data-parent="#sidebar-nav" href="#collapse{book_code}">{title}</a>
                        </h4>
                    </div>
                    <div id="collapse{book_code}" class="panel-collapse collapse{' in' if fname == filename else ''}">
                        <ul class="panel-body chapters">
                    """
            chapters = {}
            if key in self.chapters:
                chapters = self.chapters[key]
            for chapter in chapters:
                chapter_parts = chapter.split('-')
                label = chapter if len(chapter_parts) < 4 else chapter_parts[3].lstrip('0')
                html += f"""
                       <li class="chapter"><a href="{os.path.basename(fname) if fname != filename else ''}#{chapter}">{label}</a></li>
                    """
            html += """
                        </ul>
                    </div>
                </div>
                    """
        html += """
            </ul>
        </nav>
            """
        return html
# end of class TnTemplater



class BibleTemplater(Templater):
    def __init__(self, *args, **kwargs):
        self.templater_CSS_class = 'bible'
        super(BibleTemplater, self).__init__(*args, **kwargs)
        # if self.templater_CSS_class != 'bible': # avoid "bible bible"
            # self.classes = ['bible'] # These get appended to the html body class


    def get_page_navigation(self):
        for fname in self.files:
            key = os.path.basename(fname)
            if key in self.titles:  # skip if we already have data
                continue
            filebase = os.path.splitext(os.path.basename(fname))[0]
            # Getting the book code for HTML tag references
            fileparts = filebase.split('-')
            if len(fileparts) == 2:
                # Assuming filename of ##-<name>.usfm, such as 01-GEN.usfm
                book_code = fileparts[1].lower()
            else:
                # Assuming filename of <name.usfm, such as GEN.usfm
                book_code = fileparts[0].lower()
            book_code.replace(' ', '-').replace('.', '-')  # replacing spaces and periods since used as tag class
            with open(fname, 'r') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            if soup.select('div#content h1'):
                title = soup.select('div#content h1')[0].text.strip()
            else:
                title = f'{book_code}.'
            self.titles[key] = title
            self.book_codes[key] = book_code
            chapters = soup.find_all('h2', {'c-num'})
            self.chapters[key] = [c['id'] for c in chapters]


    def build_page_nav(self, filename=None):
        html = """
        <nav class="hidden-print hidden-xs hidden-sm content-nav" id="right-sidebar-nav">
            <ul id="sidebar-nav" class="nav nav-stacked books panel-group">
            """
        for fname in self.files:
            key = os.path.basename(fname)
            book_code = ""
            if key in self.book_codes:
                book_code = self.book_codes[key]
            title = ""
            if key in self.titles:
                title = self.titles[key]
            if title in self.NO_NAV_TITLES:
                continue
            html += f"""
                <div class="panel panel-default">
                    <div class="panel-heading">
                        <h4 class="panel-title">
                            <a class="accordion-toggle" data-toggle="collapse" data-parent="#sidebar-nav" href="#collapse{book_code}">{title}</a>
                        </h4>
                    </div>
                    <div id="collapse{book_code}" class="panel-collapse collapse{' in' if fname == filename else ''}">
                        <ul class="panel-body chapters">
                    """

            chapters = {}
            if key in self.chapters:
                chapters = self.chapters[key]

            for chapter in chapters:
                chapter_parts = chapter.split('-')
                label = chapter if len(chapter_parts) < 3 else chapter_parts[2].lstrip('0')
                html += f"""
                       <li class="chapter"><a href="{os.path.basename(fname) if fname != filename else ''}#{chapter}">{label}</a></li>
                    """
            html += """
                        </ul>
                    </div>
                </div>
                    """
        html += """
            </ul>
        </nav>
            """
        return html
# end of class BibleTemplater



class TaTemplater(Templater):
    def __init__(self, *args, **kwargs):
        self.templater_CSS_class = 'ta'
        super(TaTemplater, self).__init__(*args, **kwargs)
        self.section_container_id = 1


    def build_section_toc(self, section):
        """
        Recursive section toc builder
        :param dict section:
        :return:
        """
        if 'link' in section:
            link = section['link']
        else:
            link = f'section-container-{self.section_container_id}'
            self.section_container_id = self.section_container_id + 1
        html = f"""
            <li>
                <a href="#{link}">{section['title']}</a>
            """
        if 'sections' in section:
            html += f"""
                <a href="#" data-target="#{link}-sub" data-toggle="collapse" class="content-nav-expand collapsed"></a>
                <ul id="{link}-sub" class="collapse">
            """
            for subsection in section['sections']:
                html += self.build_section_toc(subsection)
            html += """
                </ul>
            """
        html += """
            </li>
        """
        return html


    def build_page_nav(self, filename=None):
        self.section_container_id = 1
        html = """
            <nav class="hidden-print hidden-xs hidden-sm content-nav" id="right-sidebar-nav">
                <ul class="nav nav-stacked">
        """
        for fname in self.files:
            with open(fname, 'r') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            if soup.select('div#content h1'):
                title = soup.select('div#content h1')[0].text.strip()
            else:
                title = os.path.splitext(os.path.basename(fname))[0].title()
            if title in self.NO_NAV_TITLES:
                continue
            if fname != filename:
                html += f"""
                <h4><a href="{os.path.basename(fname)}">{title}</a></h4>
                """
            else:
                html += f"""
                <h4>{title}</h4>
                """
                toc = load_yaml_object(os.path.join(f'{os.path.splitext(fname)[0]}-toc.yaml'))
                if toc:
                    for section in toc['sections']:
                        html += self.build_section_toc(section)
                html += """
                """
        html += """
                </ul>
            </nav>
        """
        return html
# end of class TaTemplater
