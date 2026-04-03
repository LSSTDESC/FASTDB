.. _usage-docs:

============
Using FASTDB
============

.. contents::

This documentation is for people who want to *use* FASTDB.  There is a FASTDB server installed somewhere that you wish to connect to in order to pull date from or push data to.  Since FASTDB is currently under heavy development, there is no global production server.  As such, if you are working with an instance of FASTDB for your own development, probably Rob set that up for you and you already know where it is.  Alternatively, you might set up a local test environment (see :ref:`developers-docs`) to use to develop code on your own machine.

Access to FASTDB is designed to be entirely through a web API.  By design, the underlying PostgreSQL and MongoDB servers cannot be connected to directly.  (There are a variety of reasons for this; talk to Rob if you're interested.)

.. _the-fastdb-client:

The FASTDB Client
=================

While you can access the FASTDB web API using any standard way of accessing web APIs (e.g. the python ``requests`` module), there is a FASTDB client designed to make this a little bit easier.

Getting Set Up to Use the FASDTB Client
----------------------------------------

The FASDTB client is entirely contained in the file ``client/fastdb_client.py`` in the github checkout.  You can just refer to this directly in your checkout by adding something to your `PYTHONPATH`, or you can copy it somewhere.  (**Warning**: if you copy it somewhere, then be aware that eventually stuff might break as your copied version falls out of date!)

The `fastdb_client.py` requires some python modules that are always installed in various environments.  The specific packages required that may not be included in base python installs are:

  * ``requests`` (though this is very often included in python installs)
  * ``pycryptodome``

Both of these are easily installable in virtual environments with ``pip``.  It's possible if you're on a Linux machine (or if you're using something like Macports) that you will be able to find them in your system's packager manager.  (On Debian and close derivatives, the packages are ``python3-requests`` and ``python3-pycryptodome``.) ``pycryptodome`` includes libraries used for the user authentication to FASTDB, for more information see [Rob put in a link if you ever describe the internal details of the user authentication system].

On NERSC Perlmutter
********************

To run the FASTDB client on Perlmutter, you need to do the following *once*:

  * Get an account on FASTDB.  Talk to Rob; he will need your name, email address, and the username you want on FASTDB.

  * Go to https://desc-fastdb.lbl.gov and click on "Request Password Reset".  Follow the prompts.  Make sure to use a good password, that is not the same as a password you use anywhere else.  If you're still using the same password for more than one account, and indeed if you're not using a password manager like KeePassXC or LastPass, then you are at least a decade behind on what you really need to be doing with your passwords.
  
  * Create a file ``.fastdb.ini`` in your home directory on nersc.  Give it contents::

      [production]
      url = https://desc-fastdb.lbl.gov
      username = <your fastdb username>
      pass = <your fastdb password>

  * Run ``chmod go-rws ~/.fastdb.ini`` to make sure nobody else can read your password.

Having done that, thereafter in order to use FASTDB from Perlmutter, each time you start a session you need to:

  * Start the DESC-TD environment with::

      source /global/cfs/cdirs/lsst/groups/TD/setup_td.sh

  * Add the ``fastdb_client`` diretory to your python path with::

      export PYTHONPATH=/dvs_ro/cfs/cdirs/desc-td/SOFTWARE/fastdb_deployment/fastdb_client:$PYTHONPATH

  * Run python::

      python

  * In python, import ``fastdb_client`` and instantiate a connection object with::

      from fastdb_client import FASTDBClient
      fdb = FASTDBClient( "production" )

You can then use the python object ``fdb`` to communicate with the FASTDB server using what's documented here.

Using the Client
----------------

You can use the various "API endpoints" documented below in :ref:`web-api` with::

  result = fdb.post( endpoint, json=options )

where ``endpoint`` is a string starting with "/", and is documented below.  ``json=options`` is optional, but if it's there, ``options`` should be a dictionary holding additional configuration options, all of which are documented below.


You can find a jupyter notebook with documentation and examples at ROB UPDATE THIS AND PUT IN THE LOCATION.


Interactive Web Pages
======================

TODO


.. _web-api:

The Web API
===========

Top-Level Endpoints
-------------------

.. _webap-getprocvers:

``/getprocvers``
***************

Returns a list of known procesing versions and processing version aliases.  You get back a JSON-encoded dictionary with keys:

* ``status``: string, value ``ok``
* ``procvers`` : list of string; the processing version names.

Because ``procvers`` includes both aliases and processing version names, some of the elements of the list actually refer to the same thing.  (For instance, if ``default`` is in the list, it's almost certainly an alias for something else that is also in the list.)

.. _webap-procver:

``/procver``
***********

Hit this API endpoint with ``/procver/<procver>``, where ``<procver>`` is either the name or the UUID (as a string) of the processing version you want information about.  For example, on the production FASTDB, try::

  result = fdb.post( "/procver/realtime" )

You will get back a JSON dictionary with keys:

* ``status``: string, value ``ok``
* ``id``: string UUID, the UUID of the processing version
* ``description`` : string, the name of the processing version; this should be exactly the same thing as ``<procver>``
* ``aliases`` : list of string, aliases of this processing version
* ``base_procvers`` : a dictionary of lists of lists.  The dictionary keys are table names.  The dictionary values are lists; the list elements are each themselves 2-element lists, [base processing_version, priority], sorted from high to low.

You can pass either a processing version or a processing version alias in ``<procver>``.  If the name you pass is actually an alias, the ``description`` in the result is the name of the processing version itself, not the alias.  You will see the alias you passed in the ``aliases`` array.  (It turns out you could also pass the UUID of the processing version, but that will usually not be the most conveient for users.)

.. _webap-baseprocver:

``/baseprocver``
***************

Hit this API endpoint with ``/baseprocver/<procver>/<table>``, where ``<procver>`` is either the name or the UUID (as a string) of the base processing version you want information about; if this is a UUID, then you should omit the ``/<table>``, but if it's a string, then ``<table>`` should be one of ``diaobject``, ``diasource``, or ``diaforcedsource``.  You will get back a JSON dictionary with keys:

* ``status`` : string, value ``ok``
* ``id`` : string UUID, the UUID of the base processing version
* ``description`` : string, the name of the base processing version
* ``table`` : the table that this is a base processing version for
* ``procvers`` : list of string, names of processing versions that include this base processing version.  (Normally, I'd expect this to be at most a single-element list, but you never know.)


.. _webap-count:

``/count``
*********

Use this API endpoint to count how many objects, sources, or forced sources there are associated with a given processing version.  There are two calling methods:

* ``/count/<which>``
* ``/count/<which>/<procver>``

In both of these ``<which>`` is one of ``diaobject``, ``diasource``, or ``diaforcedsource``; it indicates the table whose rows you want to convert.  ``<procver>`` is the name or string UUID of the processing version you want to count rows for.  If you omit it, it will use ``default`` as the procesing version.  (Note: as of this writing, the ``default`` processing version is not actually defined in the production FASTDB; the only one defined is ``realtime``.)

You will get back a JSON dictionary with keys:

* ``status`` : string, value ``ok``
* ``table`` : string, name of the database table that was counted (one of ``diaobject``, ``diasource``, or ``diaforcedsource``).
* ``count`` : integer, the number of rows in that table corresponding to the specified processing version.

Note that ``count`` is not the total number of rows in the table, only the number of rows that you'd get if you asked for all objects in that table for a given processing version.

Because of the table joins necessary to handle processing versions, this can actually be a slow query.  An instance of FASTDB with ELAsTiCC2 loaded into it (4 million objects, 60 million sources, 900 million forced sources) took a minute or two to count the source table, and over 10 minutes to count the forced source table.  As of this writing the production FASTDB had 59 thousand diaobjects, 3 million diasources, and 8 million diaforcedsources; it took 14 seconds to return the count of diaforcedsources.

.. _webap-objectsearch:

``/objectsearch``
****************

WARNING : this query right now can be very slow, the web proxy may time out.  Rob will work on this.

MORE IMPORTANT WARNING : this endpoint is currently broken.

Find objects according to criteria.  Hit this API endpoint with either just ``/objectsearch`` or with ``/objectsearch/<procver>``.  In the latter case, ``<procver>`` is either the name or the UUID (as a string) of the processing version you want to search.  In the former case, it will search the ``default`` processing verson.

Search criteria are passed as a JSON-encoded dictionary in the body of the POST.  Keywords that may be included are:

* ``object_processing_version`` : Use this with great care, because it's complicated and confusing; probably you want to omit it.  However, if you know what you're doing, it's possible you'll make the search faster by including the right thing here.

* ``position_processing_Version`` : ...even more complciated and confusing than ``object_processing_version``.  Unless you know what you're doing, don't specify this option.

* ``fall_back_to_root_position`` : bool, default True; documentation TBD

* ``only_use_rootposition``: bool, default False; documentation TBD
  
* ``just_objids`` : bool.  If True, you don't get back lightcurves, you just get back object ids.  If this is True, and ``min_lastmag`` and ``max_lastmag`` are not specified, then the object search will be somewhat faster.

* ``noforced`` : bool.  Normally, you will get back the last forced photometry point for each object (see below).  If ``noforced`` is True, then you will not get that back.  This can make the search faster.  Ignored if either ``min_lastmag`` or ``max_lastmag`` are True.
  
* ``mjd_now`` : float.  Normally, the search will look through all photometry when trying to find objects that match your specified criteria.  If you pass a value here, it will only look at photometry taken at this MJD or earlier.  Use this for tests and simuilations when you want to pretend that the current date is different from the real current date.

* ``ra``, ``dec`` : floats.  The RA and Dec, in decimal degrees, for the center of a cone search.  If you pass these, both are required, and ``radius`` is also required.

* ``radius`` : float.  The radius of the cone search on arcseconds, centered on (``ra``, ``dec``).

* ``window_t0``, ``window_t1`` : floats.  Some search criteria use a window.  These two numbers are the beginning and end MJD of that window.

* ``min_window_numdetectons`` : int.  Only return objects that have at least this many detections within the window.

* ``mint_firstdetection`` : float.  The MJD of the first detection (i.e. there's a ``diasource``, not just forced photometry) of objects must be at least this.

* ``maxt_firstdetection`` : float.  The MJD of the first detection must be at most this.

* ``minmag_firstdetection`` : float.  The AB magnitude of the first detection must be at least this.  (Use this to only select objects that were dim when they were first found, if for some reason you want to do that.)

* ``maxmag_firstdetection`` : float.  The AB magnitude of the last detection must be at most this.  (Use this to only select objects that were bright when they were first found, if for some reason you want to do that.)

* ``mint_lastdetection`` : float.  The MJD of the last detection (i.e. there's a ``diasource``, not just forced photometry) of objects must be at least this.

* ``maxt_lastdetection`` : float.  The MJD of the last detection must be at most this.

* ``minmag_lastdetection`` : float.  The last detection must be no brighter than this.

* ``maxmag_lastdetection`` : float.  The last detection just be no dimmer than this.

* ``mint_maxdetection`` : float.  The brightest detection must be on or after this MJD.

* ``maxt_maxdetection`` : float.  The brightest detecton must be on or before this MJD.

* ``minmag_maxdetection`` : float.  The brightest detection must be no brighter than this.  This is often the one you will want to use to throw out too-bright objects.

* ``maxmag_maxdetection`` : float.  The brightest detection must be no dimmer than this.  This is often the one you will want to use to throw out too-dim objects.

* ``min_numdetections`` : int.  Objects must have at least many detections.  (I.e. diasources.  They may well, and probably do, have more forced photometry points than this.)
  
* ``mindt_firstlastdetection`` : float.  The time between the first and last *detections* must be at least this many days.

* ``maxdt_firstlastdetection`` : float.  The time between the first and last *detections* m ust be at most this many days.  Be careful with this.  If you're trying to find stuff whose lightcurve only lasts a week, and a cosmic ray hit the objects' host galaxy a year later, and somehow that cosmic ray didn't get properly filtered out, then the ``dt`` between the first and last detections will be a year.

* ``min_lastmag`` : The most recent photometric measurement (including both detections and forced photometry ) must be no brighter than this.

* ``max_lastmag`` : The most recent photometry measurement must be no dimmer than this.

* ``statbands`` : list of string.  Normally, all of the cuts based on detection dates, detection counts, magnitudes, etc., consider all bands equally.  If you only want to consider some bands, list those here.  For instance, if you're only interested in cutting on measurements of the g, r, and i bands, pass ``['g', 'r', 'i']`` here.  This parameter also affects what is inclued in the returned data; it will ignore any measurements of bands that aren't in this list.

You get back a dictionary-encoded table of data.  Each key of the dictionary is a column in the table, and each value is a list of values in that column.  The columns are as follows.  (Note first, last, max detections all implicilty include "within ``statbands``" if that parmeters was passed.)  "Detections" below are from the ``diasource`` able.  It's possible that the brightest point on the lightcurve isn't a "detection", because for whatever reason it didn't end up in the list of detections by LSST differential imaging.

* ``diaobjectid`` : Object ID
* ``ra`` : RA in decimal degrees
* ``dec`` : Dec in decimal degrees
* ``numdet`` : Number of detections
* ``numdetinwindow`` : Number of detections in [``window_t0``, ``window_t1``].  (Null if window not given.)
* ``firstdetmjd`` : MJD of first detection
* ``firstdetband`` : Band of first detection
* ``firstdetflux`` : flux (nJy) of first detection
* ``firstdetfluxerr`` : uncertainty on ``firstdetflux``
* ``lastdetmjd`` : MJD of last detection
* ``lastdetband`` : Band of last detection
* ``lastdetflux`` : flux (nJy) of last detection
* ``lastdetfluxerr`` : uncertainty on ``lastdetflux``
* ``maxdetmjd`` : MJD of brightest detection
* ``maxdetband`` : Band of brighest detection
* ``maxdetflux`` : flux (nJy) of brightest detection
* ``maxdetfluxerr`` : uncertainty on ``maxdetflux``
* ``lastforcedmjd`` : MJD of the latest forced-photometry measurement
* ``lastforcedband`` : band of the latest forced-photometry measurement
* ``lastforcedflux`` : flux (nJy) of the last forced-photometry measurement
* ``lastforcedfluxerr`` : uncertainty on ``lastforcedflux``

The ``...forced...`` columns will not be included if ``noforced`` is passed as True, and if neither ``min_lastmag`` or ``max_lastmag`` are given.  Note that it's possible that the latest detection will be *later* than the last forced-photometry measurement.  (This will often be true in the ``realtime`` processing version, as the most recent detections will not yet have corresponding forced-photometry yet performed.)



Lightcurve Endpoints
--------------------

.. _ltcv-getmanyltcvs:

``ltcv/getmanyltcvs``
*********************

Get the lightcurves of multiple objects.

Call this by hitting one of the two endpoints:

* ``/ltcv/getmanyltcvs``
* ``/ltcv/getmanyltcvs/<procver>``

where ``<procver>`` is the name or UUID of the processing version you want lightcurves from.  If not given, it will use ``default`` as the processing version.

You must pass a JSON-encoded dictionary as the POST data, which has one required key: ``objids``.  The value must be a list of object IDs.  These may be either integer ``diaobjectid`` or UUID (string) ``rootid`` values.  (You can't mix them, however; either pass all integers, or all uuids.)  **Warning**: There are subtleties around ``diaobjectid`` values and processing versions.  If you give a ``diaobjectid`` from one release, but then ask for the processing version of another release, you may well get nothing back even when you might have expected to get something.  Maybe.

In addition, there are several optional keys that control what's included:

* ``bands`` : a list of string.  The bands you want the lightcurves for.  If not give, you will get all bands.

* ``which`` : a string, one of "detections", "forced", or "patch".  If "detections", you will only get back ``diasource`` information (i.e. things that passed detection cuts on a difference image).  If "forced", you get back only forced photometry.  If "patch", you get back forced photometry where it's available, with detections filled in where forced photometry is not available (see below).  The default is ``patch``, which is often not what you want (but often is).

* ``mjd_now`` : float.  Normally, you will get back all relevant photometry.  For normal usage, that means photomtery from before the current time, because the future hasen't happened yet.  If you specify this value, you only get back photometry from this MJD or earlier.  Use this during tests and simulations.

* ``return_object_info`` : See below

* ``include_object_positions`` : TBD

* ``position_processing_version`` : TBD
  
* ``include_base_procver`` : TBD

* ``include_source_positions`` : TBD

* ``use_weighted_source_positions`` : TBD

* ``always_use_weighted_source_positions`` : TBD


What you get depends on whether you included ``return_object_info``.  If you included that with a value of 1, then the return will be a dictionary: ``{'ltcvs': ltcvs, 'objinfo': objinfo}``.  If you didn't include that, then you just get back ``ltcvs``..

``ltcvs`` is a list that's a little bit complicated.  Each row of ``ltcvs`` is a dictionary, and corresponds to the lightcurve for a single diaobject.  It has one key ``rootid`` that is the root diaobject id for the object; this is an internal FASTDB id that tries to deduplicate redundant diaobjectids that came from LSST.  The remaining keys can be thought of as column names, and the values are lists that can be thought of as the contents of those columns.  The columns include:

  * ``mjd`` : float, the MJD of this point on the lightcurve
  * ``diasourceid`` : int, the diaSourceId, or null if this was not a detection
  * [ ``diaforcedsourceid`` : bigint, the diaForcedSourceId, or null if FASTDB does not have forced photometry for this object at this epoch.  Only included you didn't pass ``which`` as ``detections``. ]
  * ``source_diaobjectid`` : int, the diaObjectId that was associated with this diaSource, or null if this was not a detection.  It *is* possible that this will be different for different rows for the *same* diaobject, because at least the alert stream, LSST sometimes identifies more than one ``diaobjectid`` for the same actual physical transient or variable.  What' smore, the ``diaobjectid`` associated with a given ``diasource`` can *change* in different alerts froM LSST.  So, treat this with care; within FASTDB, the ``rootid`` is what you want to use.  However, if you want to compare objects to things that are reported elsewhere, you will probably need to use the LSST ``diaobjectid`` values; in that case, use, at least, the full collection of ``diaobjectid`` values identified for a single object's lightcurve.
  * [ ``forced_diaobjectid`` : int, the diaObjectId that was associated with this diaForcedSource, or null if this was not a detection.  All the same caveats apply as for ``source_diaobjectid``.  This column is not included if ``which`` was ``detections``. ]
  * ``visit`` : int, the visit number (as defined by LSST) for this observation
  * ``band`` : the filter/band of the this point.
  * ``flux`` : float, the flux in nJy of this point on the lightcruve
  * ``fluxerr`` : float, the uncertainty on flux
  * ``isdet`` : int: 1 if this was detected (i.e. a diaSource exists), 0 if not
  * [ ``ispatch`` : see below ; only inclucded you passed ``which`` as ``patch`` (the default) ]
  * [ ``base_procver_s`` : TBD; only included if you set ``include_base_procver`` ]
  * [ ``base_procver_f`` : TBD; only included if you set ``includebase_procver`` and ``which`` wasn't ``detections`` ]
  * [ ``det_ra`` : the RA where this source was detected by LSST on the difference image.  Only incluced if you specified ``include_source_positions`` ]
  * [ ``det_dec`` : the Dec where this source was detected by LSST on the difference image.  Only incluced if you specified ``include_source_positions`` ]
  * [ ``det_ra_err`` : uncetainty on RA.  Only incluced if you specified ``include_source_positions`` ]
  * [ ``det_dec_err`` : uncertainty on Dec.  Only incluced if you specified ``include_source_positions`` ]
  * [ ``det_ra_dec_cov`` : covariance between ra and dec.  Only incluced if you specified ``include_source_positions`` ]

If you also get back ``objinfo``, then ROB DOCUMENT.

About the flux values you get back
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are two kinds of photometry that is stored for object lightcurves.  A ``diaSource`` stores *detections*.  LSST does image subtractions, and then scans the difference image for soruces that patch detection thresholds.  Anything found is a ``diaSource``.

A ``diaForcedSource`` stores *forced photometry*.  When objects are known, LSST goes back and does image subractions and measures the brightness at the know object positions, regardless of whether they would have been deteted or not when scanning that difference image.

If you set ``which`` to ``detections``, you only get back ``diaSource`` values.  The fluxes come from there, and forced sources are ignored.

If you set ``which`` to ``forced``, you only get back ``diaForcedSource`` values.  The fluxes come from there, and *mostly* diasources are ignored, except that the ``isdet`` column tells you if there was a ``diaSoruce`` at this visit for this root object.

If you set ``which`` to ``patch``... it's more complicated.  You might think that (a) mostly what you want is forced sources, because it includes nondetections, and because the position is consistent so the lightcurve fluxes are less biased (**warning** it's totally unclear, however, exactly what this means for forced source values that come in the alert stream!).  Howevever, forced photometry is performed by LSST at a delay, and we only find out about it if there is a later detection that triggers an alert.  So, FASTDB will have some diasources where it does not have any forced photometry, and, normally you would expect this to be the most recent points.  If you're planning follow-up, you want those most recent points.  In this case, use ``patch``.  You will get forced photometry, but if there are visits for the object where FASTDB has a detection but does not have forced photometry, it will "patch in" the photometry from the detection.

**TLDR short summary**: ``patch`` is what you want for knowing what we've got and planning follow-up.  If you're trying to do any kind of high precision analysis with the phtometry from the alert stream, you're doing it wrong.


.. _ltcv-getltcv:

``/ltcv/getltcv``
****************

Get the lightcurve of a single object.  Hit this with one of:

* ``/ltcv/getltcv/<objid>``
* ``/ltcv/getltcv/<procver>/<objid>``

``<objid>`` is either the integer ``diaobjectid`` or string (uuid) ``rootid`` of the object whose lightcurve you want.  **Warning**: there are subtleties around ``diaobjectid`` and processing versions.  Be careful!  It's often safer to specify root ids.

``<procver>`` is the processing version of the photometry to fetch.  If not given, it will assume ``default``.

You can optionally include a JSON-encoded dictionary as POST data with any of the keys ``bands``, ``which``, or ``mjd_now``.  See the docuemtnation on :ref:`ltcv-getmanyltcvs` for what these mean.

What you get back is the same as what youg et back from :ref:`ltcv-getmanyltcvs`, except that instead of ``ltcvs`` as a list of dictionaries, get get a single dictionary with the lightcurve for the one object.



``/ltcv/getrandomltcv``
**********************

* ``/ltcv/getrandomltcv``
* ``/ltcv/getrandomltcv/<procver>``

**WARNING**: This one is scary and ill-defined and probably doesn't even work right now.
  
Randomly choose an object from the given processing version (using "default" if one is not specified) and return its lightcurve.  Format of the return is the same as for ``ltcv/getltcv``.  You can optionally pass a JSON dictionary in the POST body with parameters from ``bands``, ``which``, and ``mjd_now``, just as in ``ltcv/getltcv``.


.. _ltcv-gethottransients:

``/ltcv/gethottransients``
*************************

Call this with one of:

  * ``/ltcv/gethottransients``
  * ``/ltcv/gethottransients/<procver>``

In the first case, it will use the ``realtime`` processing version, which is almost always what you want.

Additional options that you can included in the ``json=`` dictionary are:

* ``detected_since_mjd`` : Only return lightcurves of sources that have been detected by LSST at this MJD or later.

* ``detected_in_last_days`` : Only return lightcurves of sources that have been detected by LSST between this many days ago and now.  Do not include both this and ``detected_since_mjd``, because they are do different ways of asking the same question.  If you don't include either, it defaults to 30 (*I think*) for ``detected_in_last_days``.

* ``mjd_now`` : Normally, it includes all data until the current mjd.  If you're doing simulations, or if you want to (sort of) reconstruct what we knew earlier, pass this parameter with the MJD to pretend it is.  This will affect the time window that ``detected_in_last_days`` specifies, and what photometry is returned.

* ``position_processing_version`` : TBD, and you probably don't want to include this

* ``include_object_position`` : TBD; defaults to True, which is almost certainly what you want

* ``include_base_procver`` : TBD

* ``use_weighted_source_positions`` : TBD

* ``always_use_weighted_source_positiosn`` : TBD

* ``source_patch`` : TBD, defaults to True.  Just leave it at that, probably.

You will get back the same thing as the return from :ref:`ltcv-getmanyltcvs`, including both ``ltcvs`` and ``objinfo``.  It will only include the lightcurves for objects that had a detection in the time window you specified.


Spectrum Endpoints
------------------

**WARNING** : all of these are currently untested, not up to date, and probably broken.

``spectrum/askforspectrum``
***************************

This is the web API end point you use to register your desire for spectroscopic follow-up of a transient.  (Or, ideally, host, but the system is not yet designed to distinguish the two.)  You pass to it JSON-encoded POST data which is a dictionary with keys:

* ``requester`` : string.  Who wants this specrum?  In the case of RESSEPCT instances, this should indicate that it was RESSPECT, and which running algorithm / instance of RESSPECT is making the request.

* ``objectids`` : The *root* object ids of the objects whose spectra you want.  This is a list of uuids (or strings formatted from UUIDs); it is *not* a list of integers.  Do *not* use the ``diaobjectid`` field to fill this out!

* ``priorities`` : A list of integers whose length must be the same as the length of ``objectids``.  Priorities in the inclusive range 0 (low priority) to 5 (high priority) indicating how important this spectrum is to you.  These priorities are fuzzily defined, and may well be ignored by anybody going to get spectra.  However, they will have access to these numbers, in case they want to decide which objects to target first.

If all is well, you will get back a JSON dictionary with keys ``status`` (value ``ok``), ``message`` (value ``wanted spectra created``), and ``num`` (value is an integer with the number of wanted spectra rows inserted into the database).


``spectrum/spectrawanted``
**************************

This is the endpoint to query if you want to figure out which specific objects have had spectra requested.  You would use this if you've got access to a spectroscopic instrument, and you want to know what spectra are most useful to DESC.  This will *only* find spectra where somebody has requested it using ``spectrum/askforspectrum``; if what you're after is any active transient, then you want to use :ref:`ltcv/gethottransients <ltcv-gethottransients>` instead.

POST to the endpoint with dictionary in a JSON payload.  This may be an empty dictionary ``{}``; the following optional keys may be included:

* ``processing_version`` : string; the processing version to look at when finding photometry.  If not given, will assume ``realtime``.

* ``requested_since`` : string in the format ``YYYY-MM-DD`` or ``YYYY-MM-DD hh:mm:ss``; only find spectra that were requested since this time.  (This is so you can filter out old requests.)  You will usually want to specify this.  If you don't, it will give you anything that anybody has asked for ever.

* ``requester`` : string; if given, only get spectra requested by a specific requester.  If not given, get all spectra requested by everybody.
  
* ``not_claimed_in_last_days`` : int; only return spectra where nobody else has indicated a intention to take this spectrum.  Use this to coordinate between facilities, so that multiple facilities don't all get the same spectra.  This defaults to 7 if not specified.  If you don't want to consider whether anybody else has said they're going to take a spectrum, explicitly pass ``None`` for this value.

* ``no_spectra_in_last_days``: int; only return objects that have not had spectrum information reported in this many days.  This is also for coordination.  If you don't want to consider just what is planned, but what somebody actually claims to have observed, then use this.  If not given, it defaults to 7.  (This may be combined with ``not_claimed_in_last_days``.  It's entirely possible that people will report spectra that they have not claimed.)  To disable consideration of existing spectra, as with ``not_claimed_in_last_days`` set this parameter to ``None``.
  
* ``detected_since_mjd`` : float.  Only return objects that have been *detected* (i.e. found as a source in DIA scanning) by Rubin since this MJD.  Be aware that an object may not have been detected in the last few days simply because it's field hasn't been observed!  If not passed, then the server will use ``detected_in_last_days`` (below) instead.  Pass ``None`` to explicilty disable consideration of recent detections.

* ``detected_in_last_days``: float.  Only return objects that have been *detected* within this may previous days by LSST DIA.  Ignored if ``detected_since_mjd`` is specified.  If neither this nor ``detected_since_mjd`` is given, defaults to 14.

* ``lim_mag`` : float; a limiting magnitude; make sure that the last measurement or detection was at most this magnitude.

* ``lim_mag_band`` : str; one of u, g, r, i, z, or Y.  The band of ``lim_mag``.  If not given, will just look at the latest observation without regard to band.
  
* ``mjd_now`` : float; pretend that the current MJD is this date.  Normally, the server will use the current time, and normally this is what you want.  This parameter is here for testing purposes.  All database queries will cut off things that are later in time than this time.
  
You will get back a JSON-encoded list.  Each element of the list is a dictionary with keys:

* ``root_diaobject_id``: a (string) UUID, the root diaobject id of the object whose spectrum is wanted.
* ``diaobjectid`` : a (64-bit) integer, the diaobjectid of the object (see below)
* ``requester`` : a string, the name of the person or system who requested the spectrum
* ``priority`` : an integer in the range [0,5]: the priority of the spectrum.  Higher means higher priority.  This is defined fuzzily, so consider it advisory rather than rigorous; different requesters may use this differently.
* ``ra`` : RA in degrees of the object (from the ``diaobject`` table)
* ``dec`` : Dec in degrees of hte object (from the ``diaobject`` table )
* ``latest_source_mjd`` : the MJD of the latest *detection* of this object
* ``latest_source_band`` : the latest *detection* of this object
* ``latest_source_mag`` : the AB magnitude of the latest *detection*
* ``latest_forced_mjd`` : the MJD of the latest forced photometry available for this object
* ``latest_forced_band`` : the band of the latest forced photometry available for this object
* ``latest_forced_mag`` : the AB magnitude of the latest forced photometry available for this object.

Note that you may well get back multiple entries in the list for the same ``root_diaobject_id``.  This will happen if more than one requester has asked for spectra of the same object.

It's possible that different calls will get different ``diaobjectid`` for the same ``root_diaobject_id``.  We recommend (and sometimes require) using ``root_diaboject_id`` for communication with the Web API where possible.


``spectrum/planspectrum``
*************************

Use this to declare your intent to take a spectrum.  This is here so that multiple observatories can coordinate.  ``spectrum/spectrawanted`` (see above) is able to filter out things that have a planned spectrum.

POST to the api endpoint with a JSON payload that is a dict.  Required keys are:

* ``root_diaobject_id``: string UUID; the object ID of the object you're going to take a spectrum of.  These UUIDs are returned by ``ltcv/gethottransients``.

* ``facility``: string; the name of the telescope or facility where you will take the spectrm.

* ``plantime``: string ``YYYY-MM-DD`` or ``YYYY-MM-DD HH:MM:SS``; when you expect to actuallyobtain the spectrum.

You may also include one optional key:

* ``comment``: string, any notes bout your planned spectrum.

If all is well, you will get back a dictionary with a single key: ``{'status': 'ok'}``

``spectrum/removespectrumplan``
*******************************

Use this to remove a spectrum plan.  This isn't strictly necessary if you succesfully took a spectrum and reported the info with ``spectrum/reportspectruminfo`` (see below), but you may still use it.  The real use case is if you planned a spectrum, but for whatever reason (e.g. the night was cloudy), you didn't actually get that spectrum.  In that case, you probably want to remove your spectrum plan from FASTDB so that other people won't skip that object thinking you are going to do it.

POST to the api endpoint with a JSON payload that is a dict.  There are two required keywords:
* ``root_diaobject_id``: string UUID
* ``facility``: string
these must match exactly what you passed when you called ``spectrum/planspectrum``.  Any entry in the database matching these two things will be removed.

(Note: there's no authentication check on the specific facility.  Any authenticated user to FASTDB can remove any spectrum plan.  We're trusting that the people who have been given accounts on FASTDB are only going to remove spectrum plans that they themselves submitted, or that the otherwise know are legitimate to remove.)

If all is well, you will get back a dictionary with a two keys.  The value of ``status`` will be ``ok``, and the value of ``ndel`` will be the number of rows deleted from the database.

``spectrum/reportspectruminfo``
*******************************

When you've actually taken a spectrum, it will help us greatly if you tell us about it. This both lets us know that a spectrum has been taken, and gives us information about type and redshift. Eventually, we may have additional fields (something about S/N, something about type confidence, perhaps), and eventually we will have a way for uploading a 1d spectrum, but for now we're just asking for a redshift and a classid.

POST to the api endpoint with a JSON payload that is a dict, with keys:

* ``root_diaobject_id``: string UUID;  the id of the object, the same value that all the previous URLs have used

* ``facility``: string; the name of the facility. If you submitted a plan, this should match the facililty that you sent to ``spectrum/planspectrum``. (It's OK to report spectra that you didn't declare a plan for ahead of time!)

* ``mjd``: float; the mjd of when the spectrum was taken. (Beginning, middle, or end of exposure, doesn't matter.)

* ``z``: float;  the redshift of the supernova from the spectrum. Leave this blank ("" or None) if it cannot be determined.

* ``classid``: int — the type from the spectrum. Use the `ELAsTiCC/DESC taxonomy <https://github.com/LSSTDESC/elasticc/blob/main/taxonomy/taxonomy.ipynb>`_.
  

``spectrum/getknownspectruminfo``
**********************************

This is to get what spectrum information has been reported.

POST to the api endpoint a JSON-encoded dict.  All keys are optional; possibilities include:

* ``root_diaobject_ids`` :  str or list of str; if included only get the spectra for this object or these objects.  (Query multiple objects by passing a list.)  These are the same UUIDs that all the previous endpoints have used.

* ``facility``: str; if included, only get spectrum information from this facility.  Otherwise, include spectrum information from all facilities.

* ``mjd_min``: float; if included, only get information about spectra taken at this mjd or later.

* ``mjd_max``: float; if included, only get information about spectra taken at this mjd or earlier.

* ``classid``: float; if included, only get information about spectra tagged with this cass id.

* ``z_min``: float; if included, only get information about spectra at this redshift or higher.

* ``z_max``: float, if included, only get information about spectra at this redshift or lower.

* ``since``: str ``YYYY-MM-DD HH:MM:SS`` or ``YYYY-MM-DD``; if included, only get spectra that were reported on this data/time (UTC) or later.

If you include no keys, you'll get information about all spectra that the database knows about, which may be overwhelming. (The API may also time out.)

If all is well, the response you get back is a json-encoded list (which might be empty).  Each element of the list is a dictionary with keys:

* ``specinfo_id``: string UUID; you can safely ignore this

* ``root_diaobject_id``: string UUID; the same UUID you've been using all along

* ``facility``: string; the facility that reported the spectrumn

* ``inserted_at``: datatime; the time at which the spectrum was reported to the database
  
* ``mjd``: float, the MJD the spectrum was taken

* ``z``: float or None, the redshift from the spectrum.  If None, it means that the redshfit wasn't able to be determined from the spectrum.

* ``classid``: the reported class id.

Direct SQL Queries
------------------

**WARNING** : this API is currently broken and not working.

**Warning**: We strongly recommend *against* using custom-built SQL queries to the database.  The reason is that the table structure surrounding :ref:`processing-versions` is complicated enough that it's very easy to construct a query that will give you results that to casual inspecton look right but that are in fact wrong.  If you can't find a web API to do what you need to do, please talk to Rob.  If you *must* do direct SQL queries, make sure you really understand how processing versions work.

The FASDTB web interface includes a front-end for direct read-only SQL queries to the backend PostgreSQL database.  (Note that "read-only" means that you can't commit changes to the database.  You *can* use temporary tables with this interface, and that is often a very useful thing to do.)

TODO document this.  In the mean time, see the `examples FASDTB client Juypyter notebook <https://github.com/LSSTDESC/FASTDB/blob/main/examples/using_fastdb_client.ipynb>`_ for documentation on this interface.
