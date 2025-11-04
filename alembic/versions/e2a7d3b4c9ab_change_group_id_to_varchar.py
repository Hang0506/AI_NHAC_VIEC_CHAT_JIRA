from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "e2a7d3b4c9ab"
down_revision = "c63d15d431f2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Change employee.group_id to VARCHAR(64) using Postgres cast
    op.execute(
        "ALTER TABLE employee ALTER COLUMN group_id TYPE VARCHAR(64) USING group_id::text"
    )


def downgrade() -> None:
    # Revert to INTEGER if needed (possible data loss if non-numeric)
    op.execute(
        "ALTER TABLE employee ALTER COLUMN group_id TYPE INTEGER USING NULLIF(group_id, '')::integer"
    )


