-- Helper functions for skymap cross-matching

-- Unpack a NUNIQ value into (order_m, ipix_coarse)
DROP FUNCTION IF EXISTS decode_uniq(BIGINT);
CREATE FUNCTION decode_uniq(nuniq BIGINT)
RETURNS TABLE(order_m INT, ipix_coarse BIGINT)
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    m INT;
BEGIN
    -- order_m is floor(log4(nuniq/4)) = floor(log2(nuniq/4)/2)
    m := FLOOR(LOG(4.0, nuniq::numeric / 4));
    order_m := m;
    -- ipix_coarse is the remainder once the order prefix is removed
    ipix_coarse := nuniq - (4 * CAST(POWER(2, m * 2) AS BIGINT));
    RETURN NEXT;
END;
$$;

-- Check if a fine pixel belongs to the coarse pixel defined by order and ipix
DROP FUNCTION IF EXISTS is_within(BIGINT, INT, BIGINT);
CREATE FUNCTION is_within(npix BIGINT, order_m INT, ipix_coarse BIGINT)
RETURNS BOOLEAN
LANGUAGE SQL IMMUTABLE STRICT AS $$
    SELECT (npix >> (2 * (29 - order_m))) = ipix_coarse;
$$;