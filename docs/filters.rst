.. _creating-filters:

================
Filters Overview
================
 
This page is about filters as they are used in the context of the LSST alert stream. Essentially, a filter takes a stream of alerts from a broker, and returns a subset of those alerts based on some criteria. This is useful for narrowing down the vast stream of alerts that the Rubin Observatory outputs to something that can be more easily digested and used for specific science cases. For example, a filter could output only objects that look like supernovae, or on objects in a certain area on the sky. 

Some of the requirements for filters include: 

* **reproducible:** they should return the same objects if they were to be run multiple times on the same set of objects
* **broker-level:** filters should be applied at the broker level (i.e. within its pipeline), and create their own stream of alerts
* **provide certain alert data:** each of the alerts being output from a filter should have all of the data from the `DiaSource <https://sdm-schemas.lsst.io/apdb.html#DiaSource>`_ table. Ideally, the alerts should have all of the original data from the Rubin alert, in addition to any new data that was added by the broker or the filter itself. At a minimum, the following parameters are required in order to get some sense of the alert:

    * diaSourceId (unique identifier for the source) 
    * diaObjectId (id of the object this source was associated with, if any)
    * midpointMjdTai (Modified Julian Date of visit)
    * apFlux (flux in nJy)
    * apFluxErr (estimated flux uncertainty in nJy)
    * visit (id of the visit where the source was measured)
    * ra (Right ascension of the center of this source)
    * dec (Delination coordinate of the center of the source)



Creating new filters
====================


This section details how to create new filters at the broker level for FASTDB to subscribe to, for all of the LSST brokers where that is available. 


ALeRCE
------

**Current status as of April 2026:** No obvious way to create new filters at the broker level, beyond submitting your own 'step' in the pipeline. 

Links:
^^^^^^
* `ALeRCE <https://science.alerce.online/>`_
* `Creating a step <https://github.com/alercebroker/pipeline/tree/b58b866b410d4a414ef486d1b44ecb30f5a1aa80/libs/apf>`_


AMPEL
-----

Links:
^^^^^^
* `AMPEL Github <https://github.com/AmpelAstro/Ampel-LSST>`_


ANTARES
-------



Babamul
-------

**Current status as of April 2026:** no obvious way to create filters on Babamul

Links:
^^^^^^
* `Babamul <https://babamul.caltech.edu/>`_
* `Babamul Github <https://github.com/babamul/babamul>`_

Fink
----

The Fink broker streams alert data that has been enriched, for example with data from other catalogues and machine learning classification scores. 

Links:
^^^^^^
* `Creating a new Fink filter <https://doc.lsst.fink-broker.org/developers/filter_tutorial/>`_

Steps to create a new LSST filter for Fink:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Fork and clone https://github.com/astrolabsoftware/fink-filters.git
2. Make a new folder in ``/fink_filters/rubin/livestream`` called ``filter_[name]``, where you replace ``[name]`` with the name of your filter. Make sure that your filter name doesn't already exist by taking a look at the other filters that already exist. 
3. Create empty ``filter.py`` and ``__init__.py`` files in that folder.
4. Create a function in ``filter.py`` that performs the filtering. See `filter_uniform_sample <https://github.com/astrolabsoftware/fink-filters/blob/master/fink_filters/rubin/livestream/filter_uniform_sample/filter.py>`_ for a simple example filter.

    * The inputs should be existing data defined in the LSST Alert or Fink-specific properties (you can find a list of them on the `Schemas page <https://lsst.fink-portal.org/schemas>`_, under the **Data Transfer, Livestream & Xmatch** heading.), and these should be the quantities you want to filter on. 
    * The output should be a ``pd.Series`` of Booleans that is True for your chosen Alerts and False otherwise.
5. Create the test by copying in the following code:

.. code-block:: python

    if __name__ == "__main__":
    from fink_filters.tester import spark_unit_tests

    globs = globals()
    spark_unit_tests(globs, load_rubin_df=True)


This will load in the test dataset in ``datatest/rubin_test_data_10_0.parquet`` and use that to test your filter. If this dataset doesn't contain representative data for your test, you can `download your own data from the Fink Data Transfer service <https://doc.lsst.fink-broker.org/developers/filter_tutorial/#need-more-representative-test-data>`_ and add it to the test.

1. Set up the development environment by pulling the docker image and running it (see `how to get docker <https://docs.docker.com/get-started/get-docker/>`_ if you don't already have it): 
   
.. code-block:: bash

    # 2.3GB compressed
    docker pull gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-sentinel-rubin:latest

    # Assuming you are in /path/to/fink-filters
    docker run -t -i --rm -v \
    $PWD:/home/libs/fink-filters \ 
    gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-sentinel-rubin:latest bash

7. Once in the docker container, you can run the test on your filter to make sure that it works using the following command: ``./run_tests.sh --single_module fink_filters/rubin/livestream/filter_[name]/filter.py``. If it works, there will be some Spark UserWarnings, and then it will generate a coverage report if there are no errors in the tests. 
8. If the filter test is working, then you can create a pull request for the ``fink-filters`` repository with your new filter. 

Lasair
------

**Current status as of April 2026:** can make filters using their builder, and then convert to an active filter. Need a Lasair account. 

Links:
^^^^^^
* `Lasair <https://lasair.lsst.ac.uk/>`_
* `Making a filter <https://lasair-lsst.readthedocs.io/en/main/core_functions/make_filter.html>`_


Pitt-Google
-----------


SNAPS
-----



POI broker
----------

**Current status as of April 2026:** no clear way to make a filter on the broker stream. 

Links:
^^^^^^
* `POI <https://poibroker.uantof.cl/>`_