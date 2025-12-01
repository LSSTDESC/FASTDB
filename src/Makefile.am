AUTOMAKE_OPTION = subdir-objects

installdir = @installdir@
servicesdir = @installdir@/services
admindir = @installdir@/admin

install_DATA = __init__.py config.py db.py kafka_consumer.py ltcv.py spectrum.py util.py parquet_export.py \
	../extern/rkwebutil/rkwebutil/rkwebutil.py

services_DATA = services/__init__.py \
	services/brokerconsumer.py \
	services/dr_importer.py \
	services/long_query_runner.py \
	services/projectsim.py \
	services/source_importer.py

admin_DATA = admin/csv_to_avsc.py \
	admin/dump_to_parquet.py \
	admin/fastdb_loader.py \
	admin/load_dp1_parquet.py \
	admin/load_snana_fits.py
