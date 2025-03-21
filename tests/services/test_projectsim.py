from services.projectsim import AlertReconstructor, AlertSender

def test_reconstruct_alert( snana_fits_ppdb_loaded ):
    recon = AlertReconstructor()

    alert = recon.reconstruct( 169694900014 )
    import pdb; pdb.set_trace()
    pass
