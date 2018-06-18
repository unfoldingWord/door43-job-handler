import sys
import os
import logging
import re
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from src.aws_tools.s3_handler import S3Handler
from src.aws_tools.dynamodb_handler import DynamoDBHandler
#from src.aws_tools.lambda_handler import LambdaHandler
from src.gogs_tools.gogs_handler import GogsHandler


#def resetable(cls):
    #cls._resetable_cache_ = cls.__dict__.copy()
    #return cls


#def reset_class(cls):
    #print("reset_class()!!!")
    #try: print("  was db_user={!r}".format(cls.db_user))
    #except: pass
    #cache = cls._resetable_cache_  # raises AttributeError on class without decorator
    #for key in [key for key in cls.__dict__ if key not in cache and key != '_resetable_cache_']:
        #delattr(cls, key)
    #for key, value in cache.items():  # reset the items to original values
        #try:
            #if key != '_resetable_cache_':
                #setattr(cls, key, value)
        #except AttributeError:
            #pass
    #try: print("  now db_user={!r}".format(cls.db_user))
    #except: pass
    #cls.dirty = False


def setup_logger(logger, level):
    """
    Logging for the App, and turn off boto logging.
    Set here so automatically ready for any logging calls
    :param logger:
    :param level:
    :return:
    """
    for h in logger.handlers:
        logger.removeHandler(h)
    sh = logging.StreamHandler(sys.stdout)
    head = '%(asctime)s - %(levelname)s: %(message)s'
    sh.setFormatter(logging.Formatter(head))
    logger.addHandler(sh)
    logger.setLevel(level)
    # Change these loggers to only report errors:
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)


#@resetable
class App(object):
    """
    For all things used for by this app, from DB connection to global handlers
    """
    def __init__(self):
        #_resetable_cache_ = {}
        self.name = 'tx-new'
        #dirty = False

        # Stage Variables, defaults
        self.prefix = os.getenv('TX_PREFIX','') # expects 'dev-' for development mode, empty/missing for production mode
        self.api_url = 'https://api.door43.org'
        self.pre_convert_bucket = 'tx-webhook-client'
        self.cdn_bucket = 'cdn.door43.org'
        self.door43_bucket = 'door43.org'
        self.gogs_user_token = None
        self.gogs_url = 'https://git.door43.org'
        self.gogs_domain_name = 'git.door43.org'
        self.gogs_ip_address = '127.0.0.1'
        self.module_table_name = 'modules'
        self.language_stats_table_name = 'language-stats'
        self.linter_messaging_name = 'linter_complete'

        # DB setup -- get the pw from the environment variable
        self.db_protocol = 'mysql+pymysql'
        self.db_user = 'tx'
        self.db_pass = os.environ['TX_DATABASE_PW']
        self.db_end_point = 'd43-gogs.ccidwldijq9p.us-west-2.rds.amazonaws.com'
        self.db_port = '3306'
        self.db_name = 'tx'
        self.db_connection_string = None
        self.db_connection_string_params = 'charset=utf8mb4&use_unicode=0'

        # Prefixing vars
        # All variables that we change based on production, development and testing environments.
        prefixable_vars = ['api_url', 'pre_convert_bucket', 'cdn_bucket', 'door43_bucket', 'language_stats_table_name',
                        'linter_messaging_name', 'db_name', 'db_user']
        url_re = re.compile(r'^(https*://)')  # Current prefix in URLs
        for var_name in prefixable_vars:
            value = getattr(self, var)
            if re.match(url_re, value):
                value = re.sub(url_re, r'\1{0}'.format(prefix), value)
            else:
                value = prefix + value
            print("  With prefix now {}={!r}".format(var,value))
            setattr(self, var, value)

        # DB related
        self.Base = declarative_base()  # To be used in all model classes as the parent class: App.ModelBase
        self.auto_setup_db = True
        self.manifest_table_name = 'manifests'
        self.job_table_name = 'jobs'
        self.db_echo = False  # Whether or not to echo DB queries to the debug log. Useful for debugging. Set before setup_db()
        self.echo = False

        # Credentials -- get the secret ones from environment variables
        self.aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        self.aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.aws_region_name = 'us-west-2'

        # Handlers
        self._db_engine = None
        self._db_session = None
        self._cdn_s3_handler = None
        self._door43_s3_handler = None
        self._pre_convert_s3_handler = None
        self._language_stats_db_handler = None
        #_lambda_handler = None
        self._gogs_handler = None

        # Logger
        self.logger = logging.getLogger()
        setup_logger(self.logger, logging.DEBUG)

    #def __init__(self, **kwargs):
        #"""
        #Using init to set the class variables with App(var=value)
        #:param kwargs:
        #"""
        #self.init(**kwargs)

    #@classmethod
    #def init(cls, reset=True, **kwargs):
        #"""
        #Class init method to set all vars
        #:param bool reset:
        #:param kwargs:
        #"""
        #print("App.init() with kwargs=",kwargs)
        #if cls.dirty and reset:
            #App.db_close()
            #reset_class(App)
        #if 'prefix' in kwargs and kwargs['prefix'] != cls.prefix:
            #cls.prefix_vars(kwargs['prefix'])
        #cls.set_vars(**kwargs)

    #@classmethod
    #def prefix_vars(cls, prefix):
        #"""
        #Prefixes any variables in App.prefixable_variables. This includes URLs
        #:return:
        #"""
        #print("App.prefix_vars with {!r}".format(prefix))
        #url_re = re.compile(r'^(https*://)')  # Current prefix in URLs
        #for var in cls.prefixable_vars:
            #value = getattr(App, var)
            #if re.match(url_re, value):
                #value = re.sub(url_re, r'\1{0}'.format(prefix), value)
            #else:
                #value = prefix + value
            #print("  With prefix now {}={!r}".format(var,value))
            #setattr(App, var, value)
        #cls.prefix = prefix
        #cls.dirty = True

    #@classmethod
    #def set_vars(cls, **kwargs):
        ## Sets all the given variables for the class, and then marks it as dirty
        #for var, value in kwargs.iteritems():
            #if hasattr(App, var):
                #setattr(App, var, value)
                #cls.dirty = True

    @classmethod
    def cdn_s3_handler(cls):
        if not cls._cdn_s3_handler:
            cls._cdn_s3_handler = S3Handler(bucket_name=cls.cdn_bucket,
                                            aws_access_key_id=cls.aws_access_key_id,
                                            aws_secret_access_key=cls.aws_secret_access_key,
                                            aws_region_name=cls.aws_region_name)
        return cls._cdn_s3_handler

    @classmethod
    def door43_s3_handler(cls):
        if not cls._door43_s3_handler:
            cls._door43_s3_handler = S3Handler(bucket_name=cls.door43_bucket,
                                               aws_access_key_id=cls.aws_access_key_id,
                                               aws_secret_access_key=cls.aws_secret_access_key,
                                               aws_region_name=cls.aws_region_name)
        return cls._door43_s3_handler

    @classmethod
    def pre_convert_s3_handler(cls):
        if not cls._pre_convert_s3_handler:
            cls._pre_convert_s3_handler = S3Handler(bucket_name=cls.pre_convert_bucket,
                                                    aws_access_key_id=cls.aws_access_key_id,
                                                    aws_secret_access_key=cls.aws_secret_access_key,
                                                    aws_region_name=cls.aws_region_name)
        return cls._pre_convert_s3_handler

    @classmethod
    def language_stats_db_handler(cls):
        if not cls._language_stats_db_handler:
            cls._language_stats_db_handler = DynamoDBHandler(table_name=cls.language_stats_table_name,
                                                             aws_access_key_id=cls.aws_access_key_id,
                                                             aws_secret_access_key=cls.aws_secret_access_key,
                                                             aws_region_name=cls.aws_region_name)
        return cls._language_stats_db_handler

    #@classmethod
    #def lambda_handler(cls):
        #if not cls._lambda_handler:
            #cls._lambda_handler = LambdaHandler(aws_access_key_id=cls.aws_access_key_id,
                                                #aws_secret_access_key=cls.aws_secret_access_key,
                                                #aws_region_name=cls.aws_region_name)
        #return cls._lambda_handler

    @classmethod
    def gogs_handler(cls):
        if not cls._gogs_handler:
            cls._gogs_handler = GogsHandler(gogs_url=cls.gogs_url)
        return cls._gogs_handler

    @classmethod
    def db_engine(cls, echo=None):
        """
        :param mixed echo:
        """
        if echo is None or not isinstance(echo, bool):
            echo = cls.echo
        if not cls._db_engine:
            if not cls.db_connection_string:
                cls.db_connection_string = cls.construct_connection_string()
            if not cls.db_connection_string.startswith('sqlite://'):
                cls._db_engine = create_engine(cls.db_connection_string, echo=echo, poolclass=NullPool)
            else:
                cls._db_engine = create_engine(cls.db_connection_string, echo=echo)
        return cls._db_engine

    @classmethod
    def db(cls, echo=None):
        """
        :param mixed echo:
        """
        print("App.db(echo={0}) class method running...".format(echo))
        if not cls._db_session:
            print("  db() has user {!r}".format(cls.db_user))
            assert cls.db_user.startswith('dev-') # for RJH testing assurance only XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
            cls._db_session = sessionmaker(bind=cls.db_engine(echo), expire_on_commit=False)()
            # RJH: Why does this import always fail the first time (at the TxManifest class declaration)???
            # Try moving the import to the top of file -- no seems it's here because of circular imports
            from models.manifest import TxManifest
            TxManifest.__table__.name = cls.manifest_table_name
            from models.job import TxJob
            TxJob.__table__.name = cls.job_table_name
            from models.module import TxModule
            TxModule.__table__.name = cls.module_table_name
            cls.db_create_tables([TxManifest.__table__, TxJob.__table__, TxModule.__table__])
        return cls._db_session

    @classmethod
    def db_close(cls):
        if cls._db_session:
            cls._db_session.close_all()
            cls._db_session = None
        if cls._db_engine:
            cls._db_engine.dispose()
            cls._db_engine = None

    @classmethod
    def db_create_tables(cls, tables=None):
        cls.Base.metadata.create_all(cls.db_engine(), tables=tables)

    @classmethod
    def construct_connection_string(cls):
        print("App.construct_connection_string()...")
        db_connection_string = cls.db_protocol+'://'
        if cls.db_user:
            print("  construct_connection_string has user {!r}".format(cls.db_user))
            assert cls.db_user.startswith('dev-') # for RJH testing assurance only XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
            db_connection_string += cls.db_user
            if cls.db_pass:
                db_connection_string += ':'+cls.db_pass
            if cls.db_end_point:
                db_connection_string += '@'
        if cls.db_end_point:
            db_connection_string += cls.db_end_point
            if cls.db_port:
                db_connection_string += ':'+cls.db_port
        if cls.db_name:
            db_connection_string += '/'+cls.db_name
        if cls.db_connection_string_params:
            db_connection_string += '?'+cls.db_connection_string_params
        print( "  Returning", db_connection_string )
        return db_connection_string
