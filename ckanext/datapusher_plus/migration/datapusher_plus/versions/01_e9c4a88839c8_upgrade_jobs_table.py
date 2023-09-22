"""Upgrade jobs table

Revision ID: e9c4a88839c8
Revises: 
Create Date: 2023-09-22 22:14:35.137116

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e9c4a88839c8'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    #upgrade jobs table
    op.add_column(
        u'jobs',
        sa.Column(
            'aps_job_id',
            sa.UnicodeText),
    )
    


def downgrade():
    pass
