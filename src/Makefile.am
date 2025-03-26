AUTOMAKE_OPTION = subdir-objects

installdir = @installdir@
servicesdir = @installdir@/services

install_DATA = __init__.py config.py db.py kafka_consumer.py util.py

services_DATA = services/__init__.py \
	services/brokerconsumer.py \
	services/long_query_runner.py \
	services/projectsim.py \
	services/source_importer.py
