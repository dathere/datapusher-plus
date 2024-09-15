"""Create tables

Revision ID: 201f7ead1850
Revises: e9c4a88839c8
Create Date: 2024-08-07 19:49:55.324826

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '201f7ead1850'
down_revision = 'e9c4a88839c8'
branch_labels = None
depends_on = None


def upgrade():
    # upgrade jobs table if it not exists
    if not _check_column_exists("jobs", "aps_job_id"):
        op.add_column(
            "jobs",
            sa.Column("aps_job_id", sa.UnicodeText),
        )
    # upgrade metadata table if it not exists
    if not _check_column_exists("metadata", "id"):
        op.add_column(
            "metadata",
            sa.Column("id", sa.UnicodeText, primary_key=True, autoincrement=True),
        )

    # upgrade logs table
    if not _check_column_exists("logs", "id"):
        op.add_column(
            "logs",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        )


def downgrade():
    # downgrade jobs table
    if _check_column_exists("jobs", "aps_job_id"):
        op.drop_column("jobs", "aps_job_id")

    # downgrade metadata table
    if _check_column_exists("metadata", "id"):
        op.drop_column("metadata", "id")

    # downgrade logs table
    if _check_column_exists("logs", "id"):
        op.drop_column("logs", "id")


def _check_column_exists(table_name, column_name):
    bind = op.get_bind()
    insp = sa.engine.reflection.Inspector.from_engine(bind)
    columns = insp.get_columns(table_name)
    return column_name in [column["name"] for column in columns]
