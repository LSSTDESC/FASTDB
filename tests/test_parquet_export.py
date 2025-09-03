import io

import nested_pandas as npd

from parquet_export import dump_to_parquet


# def test_dump_to_parquet(alerts_90days_sent_received_and_imported, tmp_path):
#     filepath = tmp_path / "test.parquet"
#     with DB() as conn, open(filepath, "wb") as fp:
#         dump_to_parquet(fp, procver=1, connection=conn)
#     nf = npd.read_parquet(filepath)
#     assert nf.shape[0] == 37
#     assert nf["diasource"].nest.flat_length == 181
#     assert nf["diaforcedsource"].nest.flat_length == 855


def test_dump_to_parquet( set_of_lightcurves ):
    roots = set_of_lightcurves
    bio = io.BytesIO()

    dump_to_parquet( bio, 'realtime' )
    bio.seek(0)
    nf = npd.read_parquet( bio )

    import pdb; pdb.set_trace()
    pass
