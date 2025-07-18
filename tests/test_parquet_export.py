import pandas as pd

from parquet_export import dump_to_parquet
from db import DB


def test_dump_to_parquet(alerts_90days_sent_received_and_imported, tmp_path):
    filepath = tmp_path / "test.parquet"
    with DB() as conn, open(filepath, "wb") as fp:
        dump_to_parquet(fp, procver=1, connection=conn)
    df = pd.read_parquet(filepath, dtype_backend='pyarrow')
    assert df.shape[0] == 181
