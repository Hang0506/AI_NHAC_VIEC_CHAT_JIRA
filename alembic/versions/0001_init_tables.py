from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0001_init_tables"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create tables via SQLAlchemy model definitions
    from db.base import Base
    from db.models import employee as _employee  # noqa: F401
    from db.models import reminder as _reminder  # noqa: F401
    from db.models import log as _log  # noqa: F401

    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    from db.base import Base
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
