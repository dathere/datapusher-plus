# encoding: utf-8
# flake8: noqa: E501

import json

from ckan.model import meta
from ckan.model.domain_object import DomainObject
from sqlalchemy import types, Column, ForeignKey
from ckan.plugins.toolkit import BaseModel


class Jobs(DomainObject, BaseModel):
    __tablename__ = "jobs"
    job_id = Column("job_id", types.UnicodeText, primary_key=True)
    job_type = Column("job_type", types.UnicodeText)
    status = Column("status", types.UnicodeText, index=True)
    data = Column("data", types.UnicodeText)
    error = Column("error", types.UnicodeText)
    requested_timestamp = Column("requested_timestamp", types.DateTime)
    finished_timestamp = Column("finished_timestamp", types.DateTime)
    sent_data = Column("sent_data", types.UnicodeText)
    aps_job_id = Column("aps_job_id", types.UnicodeText)
    result_url = Column("result_url", types.UnicodeText)
    api_key = Column("api_key", types.UnicodeText)
    job_key = Column("job_key", types.UnicodeText)

    def __init__(
        self,
        job_id,
        job_type,
        status,
        data,
        error,
        requested_timestamp,
        finished_timestamp,
        sent_data,
        aps_job_id,
        result_url,
        api_key,
        job_key,
    ):
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
            "job_id": self.job_id,
            "job_type": self.job_type,
            "status": self.status,
            "data": self.data,
            "error": self.error,
            "requested_timestamp": self.requested_timestamp,
            "finished_timestamp": self.finished_timestamp,
            "sent_data": self.sent_data,
            "aps_job_id": self.aps_job_id,
            "result_url": self.result_url,
            "api_key": self.api_key,
            "job_key": self.job_key,
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
        job = cls.get(job_dict["job_id"])
        if job:
            for key, value in job_dict.items():
                setattr(job, key, value)
            # Assuming meta.Session has a commit method to save changes to the DB
            meta.Session.commit()
        else:
            raise Exception("Job not found")


class Metadata(DomainObject, BaseModel):
    __tablename__ = "metadata"
    id = Column("id", types.Integer, primary_key=True)
    job_id = Column(
        "job_id", types.UnicodeText, ForeignKey("jobs.job_id", ondelete="CASCADE")
    )
    key = Column("key", types.UnicodeText)
    value = Column("value", types.UnicodeText)
    type = Column("type", types.UnicodeText)

    def __init__(self, job_id, key, value, type):
        self.job_id = job_id
        self.key = key
        self.value = value
        self.type = type

    @classmethod
    def get(cls, id):
        if not id:
            return None

        return meta.Session.query(cls).get(id)

    @classmethod
    def get_all(cls, job_id):
        if not job_id:
            return None
        result = meta.Session.query(cls).filter(cls.job_id == job_id).all()
        return result

    @classmethod
    def get_by_key(cls, key):
        if not key:
            return None

        return meta.Session.query(cls).filter(cls.key == key).all()


class Logs(DomainObject, BaseModel):
    __tablename__ = "logs"
    id = Column("id", types.Integer, primary_key=True)
    job_id = Column(
        "job_id", types.UnicodeText, ForeignKey("jobs.job_id", ondelete="CASCADE")
    )
    timestamp = Column("timestamp", types.DateTime)
    message = Column("message", types.UnicodeText)
    level = Column("level", types.UnicodeText)
    module = Column("module", types.UnicodeText)
    funcName = Column("funcName", types.UnicodeText)
    lineno = Column("lineno", types.Integer)

    def __init__(self, job_id, timestamp, message, level, module, funcName, lineno):
        self.job_id = job_id
        self.timestamp = timestamp
        self.message = message
        self.level = level
        self.module = module
        self.funcName = funcName
        self.lineno = lineno

    def as_dict(self):
        return {
            "job_id": self.job_id,
            "timestamp": self.timestamp,
            "message": self.message,
            "level": self.level,
            "module": self.module,
            "funcName": self.funcName,
            "lineno": self.lineno,
        }

    @classmethod
    def get(cls, id):
        if not id:
            return None

        return meta.Session.query(cls).get(id)

    # Return any logs for the given job_id from the logs table.
    @classmethod
    def get_logs(cls, job_id):
        if not job_id:
            return None
        result = meta.Session.query(cls).filter(cls.job_id == job_id).all()
        return result if result else None

    @classmethod
    def get_logs_by_limit(cls, job_id, limit):
        if not job_id:
            return None

        result = (
            meta.Session.query(cls)
            .filter(cls.job_id == job_id)
            .order_by(cls.timestamp.desc())
            .limit(limit)
            .all()
        )
        return result


def get_job_details(job_id):
    result_dict = {}
    job = Jobs.get(job_id)
    if not job:
        return result_dict
    for field in list(job.as_dict().keys()):
        result_dict[field] = getattr(job, field)
    metadata = Metadata.get_all(job_id)
    if metadata:
        result_dict["metadata"] = _get_metadata(metadata)
    logs = Logs.get_logs(job_id)
    if logs:
        result_dict["logs"] = _get_logs(logs)

    return result_dict


def _get_metadata(metadata):
    metadata_dict = {}
    for row in list(metadata):
        value = row.value
        if row.type == "json":
            value = json.loads(value)
        metadata_dict[row.key] = value
    return metadata_dict


def _get_logs(logs):
    logs_list = []
    for log in list(logs):
        logs_list.append(log.as_dict())

    for log in logs_list:
        log.pop("job_id")
    return logs_list
