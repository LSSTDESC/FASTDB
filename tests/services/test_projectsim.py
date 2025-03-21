import pytest

from services.projectsim import AlertReconstructor


def test_reconstruct_alert( snana_fits_ppdb_loaded ):
    recon = AlertReconstructor()

    alert = recon.reconstruct( 169694900014 )

    assert alert['alertId'] == 169694900014

    assert alert['diaObject']['diaObjectId'] == 1696949
    assert alert['diaObject']['ra'] == pytest.approx( 210.234375, abs=1e-5 )
    assert alert['diaObject']['dec'] == pytest.approx( 4.031936, abs=1e-5 )
    assert alert['diaSource']['diaSourceId'] == 169694900014
    assert alert['diaSource']['diaObjectId'] == alert['diaObject']['diaObjectId']
    assert alert['diaSource']['ra'] == pytest.approx( alert['diaObject']['ra'], abs=1e-5 )
    assert alert['diaSource']['dec'] == pytest.approx( alert['diaObject']['dec'], abs=1e-5 )
    assert alert['diaSource']['midpointMjdTai'] == pytest.approx( 60371.3728, abs=0.0001 )
    assert alert['diaSource']['psfFlux'] == pytest.approx( 18833.877, abs=0.1 )
    assert alert['diaSource']['psfFluxErr'] == pytest.approx( 1107.8115, abs=0.1 )
    assert alert['diaSource']['band'] == 'Y'
    assert alert['diaSource']['snr'] == pytest.approx( alert['diaSource']['psfFlux']
                                                       / alert['diaSource']['psfFluxErr'], rel=1e-3 )
    assert len( alert['prvDiaSources'] ) == 12
    assert len( alert['prvDiaForcedSources'] ) == 7

    assert all( a['midpointMjdTai'] < alert['diaSource']['midpointMjdTai'] for a in alert['prvDiaSources'] )
    assert all( a['midpointMjdTai'] >= alert['diaSource']['midpointMjdTai'] -365 for a in alert['prvDiaSources'] )
    assert all( a['midpointMjdTai'] < alert['diaSource']['midpointMjdTai'] -1 for a in alert['prvDiaForcedSources'] )
    assert all( a['midpointMjdTai'] >= alert['diaSource']['midpointMjdTai'] -365 for a in alert['prvDiaForcedSources'] )

    # Try reconstructing with a different lookback time

    recon = AlertReconstructor( prevsrc=10, prevfrced=17, prevfrced_gap=10 )
    alert = recon.reconstruct( 169694900014 )

    assert len( alert['prvDiaSources'] ) == 9
    assert len( alert['prvDiaForcedSources'] ) == 4

    assert all( a['midpointMjdTai'] < alert['diaSource']['midpointMjdTai'] for a in alert['prvDiaSources'] )
    assert all( a['midpointMjdTai'] >= alert['diaSource']['midpointMjdTai'] -10 for a in alert['prvDiaSources'] )
    assert all( a['midpointMjdTai'] < alert['diaSource']['midpointMjdTai'] -10 for a in alert['prvDiaForcedSources'] )
    assert all( a['midpointMjdTai'] >= alert['diaSource']['midpointMjdTai'] -17 for a in alert['prvDiaForcedSources'] )
