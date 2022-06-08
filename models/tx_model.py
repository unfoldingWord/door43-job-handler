from datetime import datetime, date

from sqlalchemy import inspect

from app_settings.app_settings import dcs_url


class TxModel:

    def __init__(self, **kwargs):
        pass

    def insert(self):
        dcs_url.db().add(self)
        dcs_url.db().commit()
        dcs_url.db().close()

    def update(self):
        dcs_url.db().merge(self)
        dcs_url.db().commit()
        dcs_url.db().close()

    def delete(self):
        dcs_url.db().delete(self)
        dcs_url.db().commit()
        dcs_url.db().close()

    @classmethod
    def get(cls, *args, **kwargs):
        """
        :param args:
        :param kwargs:
        :return TxModel:
        """
        if args:
            kwargs[inspect(cls).primary_key[0].name] = args[0]
        item = cls.query(**kwargs).first()
        dcs_url.db().close()
        return item

    @classmethod
    def query(cls, **kwargs):
        items = dcs_url.db().query(cls).filter_by(**kwargs)
        return items

    def __iter__(self):
        for c in inspect(self).mapper.column_attrs:
            value = getattr(self, c.key)
            if isinstance(value, (datetime, date)):
                value = value.strftime('%Y-%m-%dT%H:%M:%SZ')
            yield (c.key, value)

    def clone(self):
        return self.__class__(**dict(self))
