# tests/admin/conftest.py
import uuid
import pytest
from db import DB

_FASTDB_MAIN_TABLES = [
    'diaobject_snapshot', 'diasource_snapshot', 'diaforcedsource_snapshot',
    'diasource', 'diaforcedsource', 'diaobject', 'host_galaxy',
    'processing_version','snapshot',
]

def _truncate_fastdb_main_tables():
    with DB() as conn:
        cur = conn.cursor()
        cur.execute(
            "TRUNCATE TABLE "
            + ", ".join(_FASTDB_MAIN_TABLES)
            + " RESTART IDENTITY CASCADE"
        )
        conn.commit()

@pytest.fixture(autouse=True)
def fastdb_isolated():
    """Clean out FASTDB tables before and after each loader test."""
    _truncate_fastdb_main_tables()
    yield
    _truncate_fastdb_main_tables()
