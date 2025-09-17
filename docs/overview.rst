.. contents::

===============
FASTDB Overview
===============


FASTDB runs with two database backends, a PostgreSQL server and a Mongodb server.  Neither database server is directly accessible; rather, you access FASTDB through a webserver.  As of this writing, a few instances of FASTDB exist; not all of them are running the latest verson of the code....

* ``https://fastdb-dp1.lbl.gov`` has the differential imaging catalogs (diaobjects, diasources, diaforcedsources) from DP1 loaded into it.  If you want an account on this, talk to Rob.

* ``https://fastdb-rknop-dev.lbl.gov`` is Rob's development/test instance.  This gets wiped out frequently, and may well have code in a completely broken state running on it.

* ``https://fastdb-resspect-test.lbl.gov`` is a test instance of FASTDB used by Rob and Amanda Wasserman for RESSPECT development.  It is also highly volatile.

You may create your own instance of FASTDB on your own machine; see :doc:`developers`.

While there will is interactive UI on the webserver, the primary way you connect to FASTDB is using the web API.  For more information, see :doc:`usage`.  To simplify this, there is a :ref:`python client <the-fastdb-client>` that handles logging in and sending requests to the web server.

To use a FASTDB instance, you must have an account on it.  At the moment, Rob is not setting up general users on any of the test installs, but hopefully that will change relatively soon.

Contact Rob to ask for an account; he will need the username you want, and the email you want associated with it.  When first created, your account will not have a password.  Point your web browser at the webserver's URL, and you will see an option to request a password reset link.

For developers wanting to set up a test installation on their own machine, see :doc:`developers`.


Database Tables Overview
========================

The core tables of the database are ``diaobject``, ``diasource``, and ``diaforcedsource``.  These nomenclature follow LSST terminology (or, at least, an earlier verson of LSST terminology).  A ``diaobject`` is a single transient or varaible object somewhere on the sky; it's a supernova, or a quasar, or something like that.  (``dia`` = "differential imaging analysis.)  A ``diasource`` is a single detection of a diaobject.  When the LSST project does difference imaging, they will scan the difference images for detections.  When they find one, they create a ``diasource``.  They then look to see if there is already a ``diaobject`` at the position on the sky of this ``diasource``; if so, this ``diasource`` is associated with that ``diaobject``.  If not, a new ``diaobject`` is created, and the ``diasource`` is associated with that new ``diaobject``.  Finally, a ``diaforcedsource`` represents forced photometry, where an aperture (or PSF model) is placed down at a predetermined known position of a ``diaobject`` on a single image.

There will be multiple ``diaobject`` entries for the same physical object on the sky, because each LSST release will assign new ``diaobjectid`` values.  (Indeed, some of the bits of the 64-bit integer ``diabojectid`` value encode the release.)  As such, FASTDB also has a ``root_diaobject`` table indexed by a UUID.  Ideally, one physical object on the sky will only ever have one ``root_diaobject``.  The ``diaobject`` table has a ``rootid`` field that points back to the ``root_diaobject`` table.

The ``diabobject`` table is indexed (i.e. primary key) by its ``diaobjectid``.  We have been told by the project that these are guaranteed to be globally unique; that is, the same ``diaobjectid`` will not show up in two different releases.  (In fact, some of the bits of the number encode the release.)  The ``diasource`` and ``diaforcedsoure`` tables are indexed by a compound (``diaobjectid``, ``visit``) key.  ``visit`` represents a single LSST exposure.  (I believe that ``diasource`` will also have a ``diasourceid`` field, but I do not believe that this will be unique between different releases.  That is, the same ``diasourceid`` value will be used in two different releases, pointing to two different sources.  ``diaforcedsource`` will *not* have an id, so ``must`` be indexed by the combination of ``diaobject`` and ``visit``.)

In addition to those four core tables, there is a ``host_galaxy`` table.  ROB WRITE MORE.

All of the rows of the photometric tables have a processing version associated with them; see :ref:`processing-versions` below.  This makes things much more complicated.

There are a set of tables designed for tracking spectrum information.  FASTDB is designed around LSST photometry releases, as LSST is an imaging telescope.  Any spectra we get will be from external resources, such as from the TiDES project on 4MOST.  The spectrum tables are designed around the RESSPECT system where we will produce prioritized lists of transients and hosts whose spectra we want (``wantedspectra``), spectra that external resources have told us they plan to take (``planeedspectra``), and information about spectra that have been reported by external resources (``spectruminfo``).

Finally, there are a set of system tables.  This includes a table for tracking database migrations, tables for tracking users, tables for tracking database queries submitted through the web API, and a set of test "PPDB" tables used for tests where we don't have the PPDB or alerts from the project, but must produce our own to test the rest of the system.


.. _processing-versions:

Processing Versions
===================

FASTDB was designed from the beginning to be able to store multiple different versions of the same data.  Use cases include:

* The ability to store the realtime (alerts/ppdb) LSST photometry, in addition to multiple data releases.  This means storing multiple measurements of the same lightcurve point.

* The ability to store DESC-reprocessed photometry alongside the LSST photometry.

* The ability to patch DESC-reprocessed photometry when, for instance, we discover a bug that requires redoing 5% of the sample, without having to upload another copy of the entire sample, and without having to delete previous rows from the database.

* The ability to query the database and get lightcurves for only one processing version.

When searching the database for a lightcurve, externally you specify a processing version.  Ideally, the database will be set up so that if you don't, it just uses the ``default`` processing version (which will really be an alias for a more specifically named processing version), so users don't have to think about it too hard if they don't want to.  (In some cases, e.g. with the interfaces to the spectrum table, it defaults to the ``realtime`` processing version rather than the ``default`` processing version.)  However, a user may well want to specify a different processing version (something like ``dr1`` or ``dr1_descsmp`` or some such).

**If all you're ever going to do is pull data from FASTDB, and if you're never going to use the interface for sending direct SQL queries, this is as much as you need to know about processing versions.** You can stop reading this section.  In fact, you probably should.  However, if you are going to write SQL queries (either because you're writing backend FASTDB code, or because you're using the direct SQL query interface), or if you're going to upload data to FASTDB, then you need to understand more about how processing versions work.

Processing Versions for Data Uploaders and Developers
-----------------------------------------------------

The database defines the concepts of  *processing version* and *base processing version*.  The *processing version* is what interfaces to the outside world; it's what users will specify when calling the various web APIs.  The *base processing version* is what each row in one of the photometry tables is associated with.  (So, the ``diaobject``, ``diasource``, and ``diaforcedsource`` tables (at least) all have a ``base_procver_id`` column, which is a foreign key into the ``base_processing_version`` table.)  Finally, there is a table ``base_procver_of_procver`` that holds a prioritized list of base procesising versions that go with each processing version.

Database queries will take this processing version, and figure out which base processing versions go with it.  It will then pull photometry from the database, ensuring that a given ``(diaobjectid,visit)`` combination only shows up once in the lightcurve.  (That is, the returned lightcurve will not include redudant photometry from the multiple different versions that are stored in the database.)  It's possible that there may be multiple base processing versions associated with a single processing version.  For example, suppose that DESC uploads a set of SMP photometry and wants this to be processing version ``pv_smp1``.  The first time it's uploaded, we create a base processing version ``bpv_smp1`` and a processing version ``pv_smp1``.  (One entry in each of two different tables.)  Later, we realize we have to redo 5% of the photometry.  Rather than delete the old photometry (which would be bad if we ever decided we want to reproduce something), we would upload the replacement photometry for just those 5% of lightcurves with base processing version ``bpv_smp1a``.  We would then set ``pv_smp1`` to be associated with base processing versions ``(bpv_smp1a, bpv_smp1)``.  This is a priority-ordered list.  When pulling lightcurves from the database, the queries need to pull the photometry with base processing version ``bpv_smp1a`` where it exists, and ``bpv_smp1`` where there is no corresponding ``bpv_smp1a`` photometry.

As you can imagine, this leads to rather subtle and complicated database queries.  It's not a simple matter of pulling all the values from the ``diaforcedsource`` table for a given set of ``diaobjectid`` values and a given processing version.  Rather, the query will need to join to the table that tracks which base processing versions go with which processing versions, use the necesary subqueries to make sure photometry is not duplicated, and ensure that the highest priority base processing version is extracted for each point.  Because it's easy for users to look at the table schema and come up with "obvious" queries that do the wrong thing, and because the right queries are potentially error prone (and, even if you manage to do it right, hard to write efficiently), we avoid having users make direct SQL queriers to the database.  Rather, we provide web APIs where the user need only specify the processing version, and the complicated business of sorting through base processing versions is handled behind the scenes for them.

Note that the base processing version of ``diaobject`` is a bit complicated.  To first order, you should just ignore the processing version of ``diaobject``.  If you select a base processing version of ``diasource`` or ``diaforcedsource``, those rows will link back to the *right* ``diaobject``, but that ``diaobject`` may well not have the same base processing version as the ``diasforcedsource``!  (Consider the example given above.  *All* of the objects in processing version ``pv_smp1`` will have base processing version ``bpv_smp1``, including those who have at least some photometry in ``bpv_smp1a``).
