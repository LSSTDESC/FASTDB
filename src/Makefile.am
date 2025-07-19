AUTOMAKE_OPTION = subdir-objects

installdir = @installdir@
servicesdir = @installdir@/services

install_DATA = __init__.py config.py db.py kafka_consumer.py ltcv.py spectrum.py util.py rkwebutil.py \
	parquet_export.py

services_DATA = services/__init__.py \
	services/brokerconsumer.py \
	services/dr_importer.py \
	services/long_query_runner.py \
	services/projectsim.py \
	services/source_importer.py
