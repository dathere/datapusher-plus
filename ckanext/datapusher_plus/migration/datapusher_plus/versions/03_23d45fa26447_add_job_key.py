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
    op.add_column('jobs', sa.Column('job_key', sa.UnicodeText(), nullable=True))


def downgrade():
    op.drop_column('jobs', 'job_key')
