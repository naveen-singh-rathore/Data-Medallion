"""empty baseline

Revision ID: 9f0fdba92220
Revises: None
Create Date: 2026-11-12 12:41:05.955822

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa



revision: str = '9f0fdba92220'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
