.. _creating-filters:

================
Filters Overview
================
 
This page discusses filters as they are used in the context of the LSST alert stream. Essentially, a filter takes a stream of alerts from a broker, and returns a subset of those alerts based on some scientific criteria. This is useful for narrowing down the vast stream of millions of alerts a day that the Rubin Observatory outputs to something that can be more easily digested and used for specific science cases. For example, a filter could output only objects that look like supernovae, or on objects in a certain area on the sky. 

Some of the requirements for filters include: 

* **reproducible:** they should return the same objects if they were to be run multiple times on the same set of objects
* **broker-level:** filters should be applied at the broker level (i.e. within its pipeline), and create their own stream topic of alerts. If that is not possible, the filter may be in a separate location, but it should *not* be located in the FASTDB repository.
* **provide certain alert data:** as there is no centrally stored database of all LSST alerts, each of the alerts being output from a filter should have all of the data from the `DiaSource <https://sdm-schemas.lsst.io/apdb.html#DiaSource>`_ and the `DiaObject <https://sdm-schemas.lsst.io/apdb.html#DiaObject>`_ schema, and all the data from the ``prvDiaSources`` and ``prvDiaForcedSource`` `arrays <https://github.com/lsst/alert_packet/blob/main/python/lsst/alert/packet/schema/10/0/lsst.v10_0.alert.avsc>`_. Ideally, the alerts should have *all* of the original data from the Rubin alert, in addition to any new data that was added by the broker or the filter itself. But at a *minimum*, the following parameters are required in order to get some sense of the alert:


.. table:: From the DiaSource schema and from the ``prvDiaSources`` array:
    :align: center

    +--------------------+----------------------------------------------------------+
    | parameter          | description                                              |
    +====================+==========================================================+
    | ``diaSourceId``    | unique identifier for the source                         | 
    +--------------------+----------------------------------------------------------+
    | ``diaObjectId``    | id of the object this source was associated with, if any | 
    +--------------------+----------------------------------------------------------+
    | ``midpointMjdTai`` | Modified Julian Date of visit                            | 
    +--------------------+----------------------------------------------------------+
    | ``apFlux``         | flux in nJy                                              | 
    +--------------------+----------------------------------------------------------+
    | ``apFluxErr``      | estimated flux uncertainty in nJy                        | 
    +--------------------+----------------------------------------------------------+
    | ``visit``          | id of the visit where the source was measured            | 
    +--------------------+----------------------------------------------------------+
    | ``ra``             | Right ascension of the center of this source (deg)       |  
    +--------------------+----------------------------------------------------------+
    | ``dec``            | Declination coordinate of the center of the source (deg) |    
    +--------------------+----------------------------------------------------------+



.. table:: From the DiaObject schema: 
    :align: center

    +-----------------+--------------------------------------------------------------------------+
    | parameter       | description                                                              |
    +=================+==========================================================================+
    | ``diaObjectId`` | id of the object this source was associated with, if any                 | 
    +-----------------+--------------------------------------------------------------------------+
    | ``ra``          | Right ascension of the center of this source (deg)                       |  
    +-----------------+--------------------------------------------------------------------------+
    | ``dec``         | Declination coordinate of the center of the source (deg)                 |    
    +-----------------+--------------------------------------------------------------------------+
    | ``raErr``       | Uncertainty of ra *(Can be omitted if absolutely necessary)*             |  
    +-----------------+--------------------------------------------------------------------------+
    | ``decErr``      | Uncertainty of dec *(Can be omitted if absolutely necessary)*            |  
    +-----------------+--------------------------------------------------------------------------+
    | ``ra_dec_cov``  | Covariance between ra and dec *(Can be omitted if absolutely necessary)* |  
    +-----------------+--------------------------------------------------------------------------+


.. table:: From the ``prvDiaForcedSources`` array (see `LSST alert packet schema <https://github.com/lsst/alert_packet/blob/main/python/lsst/alert/packet/schema/10/0/lsst.v10_0.alert.avsc>`_):
    :align: center

    +-----------------------+----------------------------------------------------------+
    | parameter             | description                                              |
    +=======================+==========================================================+
    | ``diaForcedSourceId`` | unique identifier for the source                         | 
    +-----------------------+----------------------------------------------------------+
    | ``diaObjectId``       | id of the object this source was associated with, if any | 
    +-----------------------+----------------------------------------------------------+
    | ``midpointMjdTai``    | Modified Julian Date of visit                            | 
    +-----------------------+----------------------------------------------------------+
    | ``psfFlux``           | flux in nJy                                              | 
    +-----------------------+----------------------------------------------------------+
    | ``psfFluxErr``        | estimated flux uncertainty in nJy                        | 
    +-----------------------+----------------------------------------------------------+
    | ``visit``             | id of the visit where the source was measured            | 
    +-----------------------+----------------------------------------------------------+
    | ``ra``                | Right ascension of the center of this source (deg)       |  
    +-----------------------+----------------------------------------------------------+
    | ``dec``               | Declination coordinate of the center of the source (deg) |    
    +-----------------------+----------------------------------------------------------+






Creating new filters
====================


This section details how to create new filters at the broker level for FASTDB to subscribe to, for all of the LSST brokers where that is available. Once you have created your filter, let Rob know the broker and the topic name to get FASTDB subscribed to it.

**NOTE:** Much of the broker code is still in progress (as of the writing of this), so make sure to check the linked tutorials for possible changes if you run into any difficulties.


ALeRCE
------

**Current status as of April 2026:** no immediate way to create new filters at the broker level. We think filtering would be handled through the 'step' mechanism, but this is unclear. 

ALeRCE is a Kafka-based broker that provides Kafka topic streams that users can subscribe to via a variety of methods. They also have an API interface, a Python client, and a web-based explorer that allow you to access the last 48 hours of data on demand. 


Useful Links:
^^^^^^^^^^^^^
* `ALeRCE <https://science.alerce.online/>`_
* `Creating a step <https://github.com/alercebroker/pipeline/tree/b58b866b410d4a414ef486d1b44ecb30f5a1aa80/libs/apf>`_


AMPEL
-----

**Current status as of April 2026:** have to contact the broker maintainers in order to implement filters. At the moment it looks like filters are implemented in 'Tier 0', but FASTDB might want to have an option to have filters implemented in an additional post-existing-pipeline stage (unless you can implement a filter in Tier 0 and also get all the preprocessing info)

Useful Links:
^^^^^^^^^^^^^
* `AMPEL Github <https://github.com/AmpelAstro/Ampel-LSST>`_
* `AMPEL Documentation <https://ampelproject.github.io/>`_


ANTARES
-------

The ANTARES broker runs an algorithm on its alerts that associates the alert with the nearest point of known past measurements, called a Locus. This is the object they use instead of the Alert object within the filters and send out via stream. They also filter out poor quality and bogus alerts, associate gravitational wave events, and look up associated objects. Finally, they apply the existing filters to the Locus object. The messages in the stream are the Locus objects, which have all `Locus properties <https://antares.noirlab.edu/properties>`_, as well as the alert and all past alerts associated with this object. 

Useful Links: 
^^^^^^^^^^^^^
* `Filter creation tutorial notebook <https://nsf-noirlab.gitlab.io/csdc/antares/devkit/notebooks/AntaresFilterDevKit/>`_
* `Existing ANTARES filters <https://gitlab.com/nsf-noirlab/csdc/antares/devkit/-/tree/main/antares_devkit/filters?ref_type=heads>`_

Steps to create a new LSST filter for ANTARES:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Create a `GitLab <https://about.gitlab.com>`_ account if you don't already have one. You can use your GitHub account to create your GitLab account.
2. Fork and clone https://gitlab.com/nsf-noirlab/csdc/antares/devkit
3. Pip install the package in editable mode: ``pip install -e .``
4. Create a new folder for your filter in ``/antares_devkit/filters/``, and create an ``__init__.py`` file where all your code will go. 
5. Create your filter class. It should be a class based on the ``BaseFilter`` class, and should at a minimum have a ``_run(self,locus)`` method, which is where the filter logic should go. It should run ``locus.tag('[tag_name]')`` on the loci that have been chosen by your filter code, where ``[tag_name]`` is the name you want for the stream topic that your filter creates. Start with this `filter template <https://nsf-noirlab.gitlab.io/csdc/antares/devkit/learn/structure-of-a-filter/>`_ and work from there (the Slack channel/id is optional, you should not need to worry about that). See `The Locus Object <https://nsf-noirlab.gitlab.io/csdc/antares/antares/devkit/locus.html>`_ for a description of what the 'locus' is and a reference for some of its methods and properties. There is a `full list of the locus and alert object properties <https://antares.noirlab.edu/properties>`_ for reference as well. Take a look at the `uniform_random_sample <https://gitlab.com/nsf-noirlab/csdc/antares/devkit/-/merge_requests/39>`_ filter for an example implementation.
6. Install the ANTARES client in order to get sample data for your tests by running: ``pip install antares-client``
7. Test out your filter. You can use the sample code below using the ``antares-client`` ``search.get_random_loci(n)`` function. You can also make use of the `other existing search functions <https://nsf-noirlab.gitlab.io/csdc/antares/client/api.html#module-antares_client.search>`_ to get your test data, for example ``search.cone_search()`` which searches for loci in a certain region. For more detail, take a look at the ANTARES tutorial notebook section on `testing your filter <https://nsf-noirlab.gitlab.io/csdc/antares/devkit/notebooks/AntaresFilterDevKit/#3-test-a-filter>`_.

.. code:: python

    from antares_client import search
    from antares_devkit.models import DevKitLocus
    from antares_devkit.utils import filter_report


    # Execute your_filter_class filter on 10 random loci
    for client_locus in search.get_random_loci(10):
        devkit_locus = DevKitLocus.model_validate(client_locus.to_devkit())
        report = filter_report([your_filter_class], devkit_locus)

        # `filter_report()` returns a report of what the filter did. Take a look at it:
        print(report)

8. Once you have successfully run your test, create a pull request of your forked repository. Use the following template to write out your pull request:

.. code:: markdown

    ### Summary
    Provide a brief summary of the changes introduced in this Merge Request.

    ### Changes Added
    - List the ky changes included in this MR.
    - Explain why these changes are necessary.

    ### New Filter Information (if applicable)
    - **What does the filter do?** Describe its purpose and functionality
    - **Any dependencies or configuration required?** List any additional setup needed.

    ### Testing
    - Describe how the changes were tested
    - Provide any code you used to test the filter 
    - Provide any test cases or steps to verify functionality (optional)

    ### Additional Notes


9. Once your filter pull request has been approved and merged, send the topic name and broker to Rob. 

Babamul
-------

**Current status as of April 2026:** no immediate way to create filters on Babamul. You need an account to access some of their API and their Kafka documentation, and to use their Python client to consume alerts. There is some API documentation and minimal client documentation. 


Babamul is a Kafka-based broker, written in Rust. It seems to have a specific set of `filter 'workers' <https://github.com/boom-astro/boom>`_, which is likely where new filters would be added in. 

Useful Links:
^^^^^^^^^^^^^
* `Babamul <https://babamul.caltech.edu/>`_
* `Babamul client documentation <https://pypi.org/project/babamul/>`_
* `Babamul streaming examples <https://github.com/boom-astro/babamul/blob/main/examples/>`_

Fink
----

The Fink broker is Kafka based. It streams alert data that has been enriched, for example with data from other catalogues and machine learning classification scores. 

Useful Links:
^^^^^^^^^^^^^
* `Creating a new Fink filter <https://doc.lsst.fink-broker.org/developers/filter_tutorial/>`_
* `Existing Fink filters <https://github.com/astrolabsoftware/fink-filters/tree/master/fink_filters/rubin/livestream>`_

Steps to create a new LSST filter for Fink:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Fork and clone https://github.com/astrolabsoftware/fink-filters.git
2. Make a new folder in ``/fink_filters/rubin/livestream`` called ``filter_[name]``, where you replace ``[name]`` with the name of your filter. Make sure that your filter name doesn't already exist by taking a look at the other filters that already exist. 
3. Create empty ``filter.py`` and ``__init__.py`` files in that folder.
4. Create a function in ``filter.py`` that performs the filtering. See `filter_uniform_sample <https://github.com/astrolabsoftware/fink-filters/blob/master/fink_filters/rubin/livestream/filter_uniform_sample/filter.py>`_ for a simple example filter.

    * The inputs should be existing data defined in the LSST Alert or Fink-specific properties (you can find a list of them on the `Schemas page <https://lsst.fink-portal.org/schemas>`_, under the **Data Transfer, Livestream & Xmatch** heading.), and these should be the quantities you want to filter on. 
    * The output should be a ``pd.Series`` of Booleans that is True for your chosen Alerts and False otherwise.
5. Create the test by copying the following code into your ``filter.py`` file:

.. code-block:: python

    if __name__ == "__main__":
    from fink_filters.tester import spark_unit_tests

    globs = globals()
    spark_unit_tests(globs, load_rubin_df=True)


This will load in the test dataset in ``datatest/rubin_test_data_10_0.parquet`` and use that to test your filter. If this dataset doesn't contain representative data for your test, you can `download your own data from the Fink Data Transfer service <https://doc.lsst.fink-broker.org/developers/filter_tutorial/#need-more-representative-test-data>`_ and add it to the test. This will require you to install ``fink-client`` and email them to get access.

6. Set up the development environment by pulling the docker image and running it (see `how to get docker <https://docs.docker.com/get-started/get-docker/>`_ if you don't already have it): 
   
.. code-block:: bash

    # 2.3GB compressed
    docker pull gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-sentinel-rubin:latest

    # Assuming you are in /path/to/fink-filters
    docker run -t -i --rm -v \
    $PWD:/home/libs/fink-filters \ 
    gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-sentinel-rubin:latest bash

7. Once in the docker container, you can run the test on your filter to make sure that it works using the following command: ``./run_tests.sh --single_module fink_filters/rubin/livestream/filter_[name]/filter.py``. If it works, there will be some Spark UserWarnings, and then it will generate a coverage report if there are no errors in the tests. 
8. If the filter test is working, then you can create a pull request for the ``fink-filters`` repository with your new filter. 
9. Once your filter pull request has been approved and merged, send the topic name and broker to Rob. 


Lasair
------

**Current status as of April 2026:** can make filters using their online builder, using an SQL-style query. To convert this to an active filter, you need a Lasair account. This filter will then output a Kafka topic that you can subscribe to. There is an option to send only the fields that you have filtered on, or the whole alert (without the cutout images). 

Useful Links:
^^^^^^^^^^^^^
* `Lasair <https://lasair.lsst.ac.uk/>`_
* `Making a Lasair filter <https://lasair-lsst.readthedocs.io/en/main/core_functions/make_filter.html>`_


Pitt-Google
-----------

**Current status as of April 2026:** 
Pitt-Google operates a differently than the other brokers, as it runs on Google Cloud's Pub/Sub service instead of Kafka. This means that unlike other brokers, where Python is used to create filters that build upon a Kafka package, Pitt-Google filters use the Pub/Sub-native JavaScript. 

The Pitt-Google and Google Pub/Sub documentation both discuss string-based attribute filters, however, given the limited options available within that method of filtering, and the expected desire for more complex filters, the JavaScript UDF method should be used. 

UDF filters are JavaScript functions which get passed a message object. The message contains both an ``attributes`` key with a few items that could be filtered on, and a ``data`` key. For the ``lsst-alerts-json`` Pitt-Google topic, the ``data`` key contains the alert information as set out by the LSST schema. `Other topics <https://mwvgroup.github.io/pittgoogle-client/listings.html#pub-sub-alert-streams>`_ contain subsets of the alert schema, in some cases with additional added data. Note that due to limitations in the work than can be done with a UDF filter, only the JSON streams are likely to be of any use. If this is an issue, reach out to the Pitt-Google team, as there may be workarounds available. 

.. code-block:: javascript

    function customFilter(message, _) {
      // see the Google Pub/Sub documentation for more information about the second (metadata) argument

      attributes = message.attributes; // a select few fields that could be filtered on
      payload = JSON.parse(message.data); // the contents of the alert following the LSST schema

      if (payload["diaSource"]["isNegative"]) {
        return null; // return null to stop the message from being included in the stream
      }

      // you can also add new entries into the attributes or data
      return message;
    }

In order to create a new filtered topic for FASTDB to subscribe to, you should write a new filter (following the `tutorial <https://github.com/mwvgroup/pittgoogle-user-demos/blob/main/pubsub/README.md>`_) and then upstream it to Pitt-Google by creating a GitHub issue on the `broker repository <https://github.com/mwvgroup/Pitt-Google-Broker>`_. Provided the filter is accepted, Pitt-Google will create a new Topic which can be subscribed to by FASTDB.


Links:
^^^^^^
* `Pitt-Google tutorial on pulling and filtering alerts <https://github.com/mwvgroup/pittgoogle-user-demos/blob/main/pubsub/README.md>`_
* `Pitt-Google client documentation <https://mwvgroup.github.io/pittgoogle-client/index.html>`_
* `Pitt-Google broker repository <https://github.com/mwvgroup/Pitt-Google-Broker>`_
* `Google Pub/Sub documentation on UDF filters <https://docs.cloud.google.com/pubsub/docs/smts/udfs-overview>`_

