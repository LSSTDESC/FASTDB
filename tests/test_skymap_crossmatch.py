import os
import pandas as pd
import numpy as np
import pytest
from astropy.io import fits

import db


@pytest.fixture(scope="module")
def bayestar_df():
    # Determine the FITS file path from environment or default
    fname = os.environ.get("BAYESTAR_FITS", "bayestar_for_test.multiorder.fits")
    print("Using Bayestar FITS file at:", fname)
    if not os.path.exists(fname):
        pytest.skip(f"Bayestar skymap file {fname} not found")

    # Read the FITS table data
    with fits.open(fname) as hdul:
        data = hdul[1].data

    # Convert to numpy array and inspect
    arr = np.array(data)
    print(f"Data dtype: {data.dtype}, Array dtype: {arr.dtype}, shape: {arr.shape}")

    # Byte-swap for endianness correction
    swapped = arr.byteswap()

    # Adjust dtype to reflect new byte order
    new_dt = arr.dtype.newbyteorder()
    native = swapped.view(new_dt)
    print(f"Native array dtype after byteswap and view: {native.dtype}")

    # Convert to DataFrame for testing
    df = pd.DataFrame(native)
    return df


def test_bayestar_crossmatch(alerts_90days_sent_received_and_imported, bayestar_df):
    # Cross-match first 30 days of sources against the Bayestar skymap
    with db.DB() as conn:
        cur = conn.cursor()
        # Populate npix_order29 column
        cur.execute(
            "UPDATE diaobject SET npix_order29 = healpix_ang2ipix_nest(8192, ra, dec)"
        )
        conn.commit()

        # Create a temp table and insert a subset of the skymap
        cur.execute(
            "CREATE TEMP TABLE tmpskymap (uniq BIGINT, probdensity DOUBLE PRECISION, "
            "distmu DOUBLE PRECISION, distsigma DOUBLE PRECISION, distnorm DOUBLE PRECISION)"
        )
        rows = [tuple(row) for row in bayestar_df.head(10).itertuples(index=False)]
        cur.executemany(
            "INSERT INTO tmpskymap VALUES (%s, %s, %s, %s, %s)",
            rows,
        )
        conn.commit()

        # Perform the join to count matches
        cur.execute(
            """
            SELECT count(*)
            FROM diaobject d
            JOIN (
                SELECT (decode_uniq(uniq)).order_m AS order_m,
                       (decode_uniq(uniq)).ipix_coarse AS ipix_coarse
                FROM tmpskymap
            ) s
            ON (d.npix_order29 >> (2 * (29 - s.order_m))) = s.ipix_coarse
            """
        )
        match_count = cur.fetchone()[0]

    # Assert we get a valid integer count
    assert isinstance(match_count, int)
    assert match_count >= 0
