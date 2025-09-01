import math
import numpy as np
import pytest

import db


def _decode_uniq_py(uniq: int) -> tuple[int, int]:
    """Return (order, ipix) for a HEALPix nested NUNIQ index.
    NUNIQ = 4 * NSIDE^2 + ipix = 4^(order+1) + ipix, with NSIDE=2^order.
    """
    order = int(math.floor(math.log(uniq, 4))) - 1
    ipix = int(uniq - 4 ** (order + 1))
    # optional sanity checks:
    # assert 0 <= ipix < 12 * (4 ** order)
    return order, ipix


@pytest.mark.parametrize("orders", [[13], [12], [9], [13, 11, 9, 6]])
def test_crossmatch_with_synthetic_skymap(alerts_90days_sent_received_and_imported, orders):
    """
    Build a synthetic MOHEALPix skymap from the actual diaobject npix_order13 values
    at several coarser orders, plus some deliberately off-by-one pixels (no-overlap),
    then verify:
      (1) SQL join count == Python manual count;
      (2) There is at least one match.
    This tests the shifting/coarsening logic robustly without relying on external FITS.
    """
    with db.DB() as conn:
        cur = conn.cursor()
        # Ensure npix_order13 exists/updated
        cur.execute(
            "UPDATE diaobject SET npix_order13 = healpix_ang2ipix_nest(8192, ra, dec)"
        )
        conn.commit()

        # Pull a small, stable set of pixels to build UNIQs from
        cur.execute("SELECT npix_order13 FROM diaobject LIMIT 50")
        npix_list = [row[0] for row in cur.fetchall()]
        assert len(npix_list) > 0, "No DIA objects available for test."

        # Create temp skymap table (same shape as Bayestar columns)
        cur.execute(
            "CREATE TEMP TABLE tmpskymap ("
            "  uniq BIGINT,"
            "  probdensity DOUBLE PRECISION,"
            "  distmu DOUBLE PRECISION,"
            "  distsigma DOUBLE PRECISION,"
            "  distnorm DOUBLE PRECISION)"
        )

        rows = []
        for order_m in orders:
            assert 0 <= order_m <= 13, "This test only exercises orders <= 13."
            shift = 2 * (13 - order_m)
            npix_m_list = [p >> shift for p in npix_list] if shift >= 0 else [p << (-shift) for p in npix_list]

            # Number of pixels at this order
            npix_total_m = 12 * (4 ** order_m)

            # Matching UNIQs (guaranteed to overlap)
            for ipix_m in npix_m_list:
                uniq = 4 * (4 ** order_m) + int(ipix_m)
                rows.append((uniq, 1.0, 100.0, 10.0, 1.0))

            # Non-matching UNIQs (off-by-one => should not match)
            for ipix_m in npix_m_list[: len(npix_m_list) // 2]:
                uniq_bad = 4 * (4 ** order_m) + int((ipix_m + 1) % npix_total_m)
                rows.append((uniq_bad, 1.0, 100.0, 10.0, 1.0))

        # Insert synthetic skymap
        cur.executemany(
            "INSERT INTO tmpskymap VALUES (%s, %s, %s, %s, %s)",
            rows,
        )
        conn.commit()

        # Compute SQL join count using the exact same logic you use in production
        cur.execute(
            """
            SELECT count(*)
            FROM diaobject d
            JOIN (
                SELECT (decode_uniq(uniq)).order_m AS order_m,
                       (decode_uniq(uniq)).ipix_coarse AS ipix_coarse
                FROM tmpskymap
            ) s
            ON (d.npix_order13 >> (2 * (13 - s.order_m))) = s.ipix_coarse
            """
        )
        match_count_sql = cur.fetchone()[0]

        # Fetch diaobject pixels for manual computation
        cur.execute("SELECT npix_order13 FROM diaobject")
        dia_npix = np.array([row[0] for row in cur.fetchall()], dtype=np.int64)

    # Manual cross-match count in Python (must equal SQL)
    # (d.npix_order13 >> (2*(13 - order))) == ipix   where ipix is already at 'order'
    match_count_py = 0
    for (uniq, *_rest) in rows:
        order, ipix = _decode_uniq_py(int(uniq))  # ipix is defined at 'order'
        shift = 2 * (13 - order)
        if shift >= 0:
            left = dia_npix >> shift      # coarsen 13->order
        else:
            left = dia_npix << (-shift)   # (rare; order>13)
        right = ipix
        match_count_py += np.count_nonzero(left == right)

    assert match_count_sql == match_count_py, f"SQL={match_count_sql} vs Python={match_count_py}"
    assert match_count_sql > 0, "Synthetic skymap should produce at least one overlap."