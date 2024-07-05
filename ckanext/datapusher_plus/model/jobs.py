from datetime import datetime
from sqlalchemy import types, Column, Table, ForeignKey

from ckan.model import meta, DomainObject


__all__ = ["Jobs", "jobs_table", "Metadata", "metadata_table", "Logs", "logs_table"]

"""Initialise the "jobs" table in the db."""
jobs_table = Table("jobs",
    meta.metadata,
    Column("job_id", types.UnicodeText, primary_key=True),
    Column("job_type", types.UnicodeText),
    Column("status", types.UnicodeText, index=True),
    Column("data", types.UnicodeText),
    Column("error", types.UnicodeText),
    Column("requested_timestamp", types.DateTime),
    Column("finished_timestamp", types.DateTime),
    Column("sent_data", types.UnicodeText),
    Column("aps_job_id", types.UnicodeText),
    # Callback URL:
    Column("result_url", types.UnicodeText),
    # CKAN API key:
    Column("api_key", types.UnicodeText),
    # Key to administer job:
    Column("job_key", types.UnicodeText),
)

metadata_table = Table(
        "metadata",
        meta.metadata,
        Column(
            "job_id",
            ForeignKey("jobs.job_id", ondelete="CASCADE"),
            nullable=False,
            primary_key=True,
        ),
        Column("key", types.UnicodeText, primary_key=True),
        Column("value", types.UnicodeText, index=True),
        Column("type", types.UnicodeText),
    )

"""Initialise the "logs" table in the db."""
logs_table = Table(
    "logs",
    meta.metadata,
    Column("id", types.Integer, primary_key=True, autoincrement=True),
    Column(
        "job_id",
        ForeignKey("jobs.job_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("timestamp", types.DateTime),
    Column("message", types.UnicodeText),
    Column("level", types.UnicodeText),
    Column("module", types.UnicodeText),
    Column("funcName", types.UnicodeText),
    Column("lineno", types.Integer),
)

class Jobs(DomainObject):
    def __init__(self, job_id, job_type, status, data, error, requested_timestamp, finished_timestamp, sent_data, aps_job_id, result_url, api_key, job_key):
        self.job_id = job_id
        self.job_type = job_type
        self.status = status
        self.data = data
        self.error = error
        self.requested_timestamp = requested_timestamp
        self.finished_timestamp = finished_timestamp
        self.sent_data = sent_data
        self.aps_job_id = aps_job_id
        self.result_url = result_url
        self.api_key = api_key
        self.job_key = job_key

    def as_dict(self):
        return {
            'job_id': self.job_id,
            'job_type': self.job_type,
            'status': self.status,
            'data': self.data,
            'error': self.error,
            'requested_timestamp': self.requested_timestamp,
            'finished_timestamp': self.finished_timestamp,
            'sent_data': self.sent_data,
            'aps_job_id': self.aps_job_id,
            'result_url': self.result_url,
            'api_key': self.api_key,
            'job_key': self.job_key
        }

    @classmethod
    def get(cls, job_id):
        if not job_id:
            return None

        return meta.Session.query(cls).get(job_id)

    @classmethod
    def get_by_job_key(cls, job_key):
        if not job_key:
            return None

        return meta.Session.query(cls).filter(cls.job_key == job_key).first()


    @classmethod
    def get_by_status(cls, status):
        if not status:
            return None

        return meta.Session.query(cls).filter(cls.status == status).all()

    @classmethod
    def update(cls, job_dict):
        job = cls.get(job_dict['job_id'])
        if job:
            for key, value in job_dict.items():
                setattr(job, key, value)
            # Assuming meta.Session has a commit method to save changes to the DB
            meta.Session.commit()
        else:
            raise Exception("Job not found")
    

class Metadata(DomainObject):
    def __init__(self, job_id, key, value, type):
        self.job_id = job_id
        self.key = key
        self.value = value
        self.type = type

    @classmethod
    def get(cls, job_id, key):
        if not job_id:
            return None

        return meta.Session.query(cls).filter(cls.job_id == job_id).filter(cls.key == key).first()


class Logs(DomainObject):
    def __init__(self, job_id, timestamp, message, level, module, funcName, lineno):
        self.job_id = job_id
        self.timestamp = timestamp
        self.message = message
        self.level = level
        self.module = module
        self.funcName = funcName
        self.lineno = lineno

    @classmethod
    def get(cls, job_id):
        if not job_id:
            return None

        return meta.Session.query(cls).filter(cls.job_id == job_id).all()
    
    # Return any logs for the given job_id from the logs table.
    @classmethod
    def get_logs(cls, job_id):
        if not job_id:
            return None

        return meta.Session.query(cls).filter(cls.job_id == job_id).all()
    
    @classmethod
    def get_logs_by_limit(cls, job_id, limit):
        if not job_id:
            return None

        return meta.Session.query(cls).filter(cls.job_id == job_id).order_by(cls.timestamp.desc()).limit(limit).all()
    

meta.mapper(Jobs, jobs_table)
meta.mapper(Metadata, metadata_table)
meta.mapper(Logs, logs_table)


def init_tables():
    jobs_table.create(meta.engine)
    metadata_table.create(meta.engine)
    logs_table.create(meta.engine)
