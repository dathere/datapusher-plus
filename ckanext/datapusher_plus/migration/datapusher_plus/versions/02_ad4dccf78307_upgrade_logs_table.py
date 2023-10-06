"""empty message

Revision ID: ad4dccf78307
Revises: e9c4a88839c8
Create Date: 2023-10-06 21:32:16.409225

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ad4dccf78307'
down_revision = 'e9c4a88839c8'
branch_labels = None
depends_on = None


def upgrade():
    #upgrade logs table
    op.add_column(
        'logs',
        sa.Column(
            'id',
            sa.Integer,
            primary_key=True,
            autoincrement=True),
    )


def downgrade():
    op.drop_column('logs', 'id')
