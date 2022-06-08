from datetime import datetime

from sqlalchemy import Column, String, Integer, UniqueConstraint, DateTime, UnicodeText

from general_tools.data_utils import convert_string_to_date
from models.tx_model import TxModel
from app_settings.app_settings import dcs_url
from models.text_pickle_type import TextPickleType


class TxManifest(dcs_url.Base, TxModel):
    __tablename__ = dcs_url.manifest_table_name
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    repo_name = Column(String(100), nullable=False)
    user_name = Column(String(100), nullable=False)
    lang_code = Column(String(32), nullable=False)
    resource_id = Column(String(32), nullable=False)
    resource_type = Column(String(32), nullable=False)
    title = Column(String(500), nullable=False)
    views = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_updated = Column(DateTime, default=datetime.utcnow, nullable=False)
    manifest = Column(TextPickleType, default={}, nullable=True)
    __table_args__ = (
        UniqueConstraint('repo_name', 'user_name'),
    )

    def __init__(self, **kwargs):
        # Init attributes
        super(TxManifest, self).__init__(**kwargs)
        self.created_at = convert_string_to_date(self.created_at)
        self.last_updated = convert_string_to_date(self.last_updated)
