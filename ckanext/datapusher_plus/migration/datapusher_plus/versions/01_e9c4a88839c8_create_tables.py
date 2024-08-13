"""Upgrade jobs table

Revision ID: e9c4a88839c8
Revises: 
Create Date: 2023-09-22 22:14:35.137116

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e9c4a88839c8"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    if not _check_table_exists("jobs"):
        op.create_table(
            "jobs",
            sa.Column("job_id", sa.UnicodeText, primary_key=True),
            sa.Column("job_type", sa.UnicodeText),
            sa.Column("status", sa.UnicodeText, index=True),
            sa.Column("data", sa.UnicodeText),
            sa.Column("error", sa.UnicodeText),
            sa.Column("requested_timestamp", sa.DateTime),
            sa.Column("finished_timestamp", sa.DateTime),
            sa.Column("sent_data", sa.UnicodeText),
            sa.Column("aps_job_id", sa.UnicodeText),
            sa.Column("result_url", sa.UnicodeText),
            sa.Column("api_key", sa.UnicodeText),
            sa.Column("job_key", sa.UnicodeText),
        )

    if not _check_table_exists("metadata"):
        op.create_table(
            "metadata",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("job_id", sa.UnicodeText,
                      sa.ForeignKey("jobs.job_id", ondelete="CASCADE")),
            sa.Column("key", sa.UnicodeText),
            sa.Column("value", sa.UnicodeText),
            sa.Column("type", sa.UnicodeText),
        )

    if not _check_table_exists("logs"):
        op.create_table(
            "logs",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("job_id", sa.UnicodeText,
                      sa.ForeignKey("jobs.job_id", ondelete="CASCADE")),
            sa.Column("timestamp", sa.DateTime),
            sa.Column("level", sa.UnicodeText),
            sa.Column("message", sa.UnicodeText),
            sa.Column("module", sa.UnicodeText),
            sa.Column("funcName", sa.UnicodeText),
            sa.Column("lineno", sa.Integer),
        )


def downgrade():
    if _check_table_exists("jobs"):
        op.drop_table("jobs")
    if _check_table_exists("metadata"):
        op.drop_table("metadata")
    if _check_table_exists("logs"):
        op.drop_table("logs")


def _check_table_exists(table_name):
    bind = op.get_bind()
    insp = sa.engine.reflection.Inspector.from_engine(bind)
    return table_name in insp.get_table_names()
