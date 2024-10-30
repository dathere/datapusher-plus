"""Add job_key

Revision ID: 23d45fa26447
Revises: 201f7ead1850
Create Date: 2024-10-30 21:05:09.015907

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '23d45fa26447'
down_revision = '201f7ead1850'
branch_labels = None
depends_on = None


def upgrade():
    if not _check_column_exists('jobs', 'job_key'):
       op.add_column('jobs', sa.Column('job_key', sa.UnicodeText(), nullable=True))


def downgrade():
    op.drop_column('jobs', 'job_key')


def _check_column_exists(table_name, column_name):
    bind = op.get_bind()
    insp = sa.engine.reflection.Inspector.from_engine(bind)
    columns = insp.get_columns(table_name)
    return column_name in [column["name"] for column in columns]
