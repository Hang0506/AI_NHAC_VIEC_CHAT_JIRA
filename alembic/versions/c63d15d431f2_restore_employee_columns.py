"""restore employee columns

Revision ID: c63d15d431f2
Revises: 0001_init_tables
Create Date: 2025-11-05 01:25:08.913159

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa



# revision identifiers, used by Alembic.
revision = 'c63d15d431f2'
down_revision = '0001_init_tables'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Restore columns on existing employee table and add indexes if missing
    op.execute("ALTER TABLE employee ADD COLUMN IF NOT EXISTS group_id VARCHAR(64)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_employee_group_id ON employee (group_id)")

    op.execute("ALTER TABLE employee ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITHOUT TIME ZONE")
    op.execute("CREATE INDEX IF NOT EXISTS ix_employee_created_at ON employee (created_at)")

    op.execute("ALTER TABLE employee ADD COLUMN IF NOT EXISTS created_by VARCHAR(100)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_employee_created_at")
    op.execute("DROP INDEX IF EXISTS ix_employee_group_id")
    op.execute("ALTER TABLE employee DROP COLUMN IF EXISTS created_by")
    op.execute("ALTER TABLE employee DROP COLUMN IF EXISTS created_at")
    op.execute("ALTER TABLE employee DROP COLUMN IF EXISTS group_id")


