print( "webhook.py got loaded")
# NOTE: This module name and function name are defined by the rq package
# This code adapted by RJH June 2018 from tx-manager/client_webhook/ClientWebhook/process_webhook

# Python imports
import os
import shutil
import tempfile
import logging
import ssl
import urllib.request as urllib2
import zipfile


MY_NAME = 'tx-job-handler'
pre_convert_bucket = MY_NAME
aws_region_name = 'us-west-2'


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
#logger.debug = print


def unzip(source_file, destination_dir):
    """
    Unzips <source_file> into <destination_dir>.

    :param str|unicode source_file: The name of the file to read
    :param str|unicode destination_dir: The name of the directory to write the unzipped files

    NOTE: This is unsafe if the zipfile comes from an untrusted source
            as it may contain absolute paths outside of the desired folder.
        The zipfile should really be examined first.
    """
    with zipfile.ZipFile(source_file) as zf:
        zf.extractall(destination_dir)


def download_file(url, outfile):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urllib2.urlopen(url) as request:
        with open(outfile, 'wb') as fp:
            shutil.copyfileobj(request, fp)
# end of download_file


def download_repo(base_temp_dir, commit_url, repo_dir):
    """
    Downloads and unzips a git repository from Github or git.door43.org

    :param str|unicode commit_url: The URL of the repository to download
    :param str|unicode repo_dir:   The directory where the downloaded file should be unzipped
    :return: None
    """
    repo_zip_url = commit_url.replace('commit', 'archive') + '.zip'
    repo_zip_file = os.path.join(base_temp_dir, repo_zip_url.rpartition(os.path.sep)[2])

    try:
        logger.debug('Downloading {0}...'.format(repo_zip_url))

        # if the file already exists, remove it, we want a fresh copy
        if os.path.isfile(repo_zip_file):
            os.remove(repo_zip_file)

        download_file(repo_zip_url, repo_zip_file)
    finally:
        logger.debug('finished.')

    try:
        logger.debug('Unzipping {0}...'.format(repo_zip_file))
        # NOTE: This is unsafe if the zipfile comes from an untrusted source
        unzip(repo_zip_file, repo_dir)
    finally:
        logger.debug('finished.')

    # clean up the downloaded zip file
    if os.path.isfile(repo_zip_file):
        os.remove(repo_zip_file)
#end of download_repo


def get_repo_files(base_temp_dir, commit_url, repo_name):
    temp_dir = tempfile.mkdtemp(dir=base_temp_dir, prefix='{0}_'.format(repo_name))
    download_repo(base_temp_dir,commit_url, temp_dir)
    repo_dir = os.path.join(temp_dir, repo_name.lower())
    if not os.path.isdir(repo_dir):
        repo_dir = temp_dir
    return repo_dir
# end of get_repo_files


def job(queued_json_payload):
    print("Got a job from the Redis queue: {}".format(queued_json_payload))

    # Setup a temp folder to use
    if pre_convert_bucket:
        source_url_base = 'https://s3-{0}.amazonaws.com/{1}'.format(aws_region_name, pre_convert_bucket)
    else:
        source_url_base = None
    # Move everything down one directory level for simple delete
    intermediate_dir = MY_NAME
    base_temp_dir = os.path.join(tempfile.gettempdir(), intermediate_dir)
    try:
        os.makedirs(base_temp_dir)
    except:
        pass

    # Get the commit_id, commit_url
    commit_id = queued_json_payload['after']
    commit = None
    for commit in queued_json_payload['commits']:
        if commit['id'] == commit_id:
            break
    commit_id = commit_id[:10]  # Only use the short form
    commit_url = commit['url']


    # Gather other details from the commit that we will note for the job(s)
    user_name = queued_json_payload['repository']['owner']['username']
    repo_name = queued_json_payload['repository']['name']
    compare_url = queued_json_payload['compare_url']
    commit_message = commit['message']

    if 'pusher' in queued_json_payload:
        pusher = queued_json_payload['pusher']
    else:
        pusher = {'username': commit['author']['username']}
    pusher_username = pusher['username']

    # Download and unzip the repo files
    repo_dir = get_repo_files(base_temp_dir, commit_url, repo_name)

    if 0: # Not fixed up yet -- how much of this code does the job handler need to do???
        # Get the resource container
        rc = RC(repo_dir, repo_name)

        # Save manifest to manifest table
        manifest_data = {
            'repo_name': repo_name,
            'user_name': user_name,
            'lang_code': rc.resource.language.identifier,
            'resource_id': rc.resource.identifier,
            'resource_type': rc.resource.type,
            'title': rc.resource.title,
            'manifest': json.dumps(rc.as_dict()),
            'last_updated': datetime.utcnow()
        }
        print("client_webhook got manifest_data:", manifest_data ) # RJH
        # First see if manifest already exists in DB and update it if it is
        print("client_webhook getting manifest for {!r} with user {!r}".format(repo_name,user_name)) # RJH
        # RJH: Next line always fails on the first call! Why?
        tx_manifest = TxManifest.get(repo_name=repo_name, user_name=user_name)
        if tx_manifest:
            for key, value in manifest_data.items():
                setattr(tx_manifest, key, value)
            App.logger.debug('Updating manifest in manifest table: {0}'.format(manifest_data))
            tx_manifest.update()
        else:
            tx_manifest = TxManifest(**manifest_data)
            App.logger.debug('Inserting manifest into manifest table: {0}'.format(tx_manifest))
            tx_manifest.insert()

        # Preprocess the files
        preprocess_dir = tempfile.mkdtemp(dir=self.base_temp_dir, prefix='preprocess_')
        results, preprocessor = do_preprocess(rc, repo_dir, preprocess_dir)

        # Zip up the massaged files
        zip_filepath = tempfile.mktemp(dir=self.base_temp_dir, suffix='.zip')
        App.logger.debug('Zipping files from {0} to {1}...'.format(preprocess_dir, zip_filepath))
        add_contents_to_zip(zip_filepath, preprocess_dir)
        App.logger.debug('finished.')

        # Upload zipped file to the S3 bucket
        file_key = self.upload_zip_file(commit_id, zip_filepath)

        #print("ClientWebhook.process_webhook setting up TxJob with username={0}...".format(user.username))
        print("ClientWebhook.process_webhook setting up TxJob...")
        job = TxJob()
        job.job_id = self.get_unique_job_id()
        job.identifier = job.job_id
        job.user_name = user_name
        job.repo_name = repo_name
        job.commit_id = commit_id
        job.manifests_id = tx_manifest.id
        job.created_at = datetime.utcnow()
        # Seems never used (RJH)
        #job.user = user.username  # Username of the token, not necessarily the repo's owner
        job.input_format = rc.resource.file_ext
        job.resource_type = rc.resource.identifier
        job.source = self.source_url_base + "/" + file_key
        job.cdn_bucket = App.cdn_bucket
        job.cdn_file = 'tx/job/{0}.zip'.format(job.job_id)
        job.output = 'https://{0}/{1}'.format(App.cdn_bucket, job.cdn_file)
        job.callback = App.api_url + '/client/callback'
        job.output_format = 'html'
        job.links = {
            "href": "{0}/tx/job/{1}".format(App.api_url, job.job_id),
            "rel": "self",
            "method": "GET"
        }
        job.success = False

        converter = self.get_converter_module(job)
        linter = self.get_linter_module(job)

        if converter:
            job.convert_module = converter.name
            job.started_at = datetime.utcnow()
            job.expires_at = job.started_at + timedelta(days=1)
            job.eta = job.started_at + timedelta(minutes=5)
            job.status = 'started'
            job.message = 'Conversion started...'
            job.log_message('Started job for {0}/{1}/{2}'.format(job.user_name, job.repo_name, job.commit_id))
        else:
            job.error_message('No converter was found to convert {0} from {1} to {2}'.format(job.resource_type,
                                                                                                job.input_format,
                                                                                                job.output_format))
            job.message = 'No converter found'
            job.status = 'failed'

        if linter:
            job.lint_module = linter.name
        else:
            App.logger.debug('No linter was found to lint {0}'.format(job.resource_type))

        job.insert()

        # Get S3 bucket/dir ready
        s3_commit_key = 'u/{0}/{1}/{2}'.format(job.user_name, job.repo_name, job.commit_id)
        self.clear_commit_directory_in_cdn(s3_commit_key)

        # Create a build log
        build_log_json = self.create_build_log(commit_id, commit_message, commit_url, compare_url, job,
                                                pusher_username, repo_name, user_name)
        # Upload an initial build_log
        self.upload_build_log_to_s3(build_log_json, s3_commit_key)

        # Update the project.json file
        self.update_project_json(commit_id, job, repo_name, user_name)

    print("  Ok, job completed now!")
# end of job

# end of webhook.py
