.. _developers-docs:
.. contents::

==========================
Information for Developers
==========================

This documentation is for people who want to install a test version of FASTDB on their local machine, edit the FASTDB code, or try to install FASTDB somewhere else.  (It is currently woefully incomplete for the last purpose.)

The FASDTB code can be checked out from https://github.com/LSSTDESC/FASTDB ; that is currently the only place to get the code.  (There are no plans to make it pip installable or anything like that.)


Submodules
==========

FASTDB uses at least one submodule. These are checked out in the ``extern`` subdirectory underneath the top-level of the checkout.  When first checking out the repository, things will not fully work unless you run::

  git submodule update --init

That command will check the appropriate commit of all needed submodules.

If later you pull a new revision, ``git status`` may show your submodule as modified, if somebody else has bumped the submodule to a newer verion.  In that case, just run::

  git submodule update

to get the current version of all submodules.

The ``pg_healpix`` submodule contains a PostgreSQL extension written in
C for HEALPix indexing.  After checking out the code you may build and
install the extension with ``make`` and ``make install`` (``pg_config``
must be in your ``PATH``).  Then load it in your database with::

  CREATE EXTENSION pg_healpix;


.. _installing-the-code:

Installing the Code
===================

(If you're reading this documentation for the first time, don't try to do what's in this section directly.  Rather, read on.  You will want to refer back to this section later.  First, though, you will probably want to do everything below about setting up a test environment.  That sections includes everything you need to install the test code.  This section is more general, and what you'd think about if you're trying to install a FASTDB instance somewhere else.)

The code (for the most part) is not designed to be run out of the ``src`` directory where it exists, though you may be able to get that to work.  Ideally, you should install the code first.  Exactly where you're installing it depends on what you're trying to do.  If you're just trying to get a local test environment going on your own machine, see :ref:`local-test-env`.

If you've edited a ``Makefile.am`` file in any directory, or the ``configure.ac`` file in the top-level directory, see :ref:`autoreconf-install` below.  Otherwise, to install the code, you can run just two commands::

  ./configure --with-installdir=[DIR] --with-smtp-server=[SERVER] --with-smpt-port=[PORT]
  make install

The first ``[DIR]`` is the directory where you want to install the code.  The SMTP server setup requires you to know what you're doing.  You can run::

  ./configure --help

as usual with GNU autotools to see what other options are available.  If you're making a production install of FASTDB somewhere, you will definitely want to do things like configure the database connection.

It's possible that after running the first command, you'll get errors about ``aclocal-1.16 is missing on your system`` or something similar.  There are two possibilites; one is that you do legimiately need to rebuild the autotools file, in which case see :ref:`autoreconf-install` below.  However, if you haven't touchedd the files ``aclocal.m4``, ``configure``, or, in any subdirectory, ``Makefile.in`` or ``Makefile.am``, then this error may be result of an unfortunate interaction between autotools and git; autotools (at least some versions) looks at timestamps, but git checkouts do not restore timestamps of files committed to the archive.  In this case, you can run::

  touch aclocal.m4 configure
  find . -name Makefile.am -exec touch \{\} \;
  find . -name Makefile.in -exec touch \{\} \;

and then retry the ``./configure`` command above.


.. _local-test-env:

Local Test Environment
=======================

Build and run the Docker services
----------------------------------

The file ``docker-compose.yaml`` in the top-level directory contains (almost) everything necessary to bring up a test FASTDB environment on your local machine.  You'll need to have some form of docker installed, with a new enough version of ``docker compose``.  Rob is able to get things to work with Docker 20.10.24 (run ``docker --version``) and docker compose 2.36.2 (run ``docker compose version``).  If you have older versions and something doesn't work, try upgrading.  You'll need to have the docker container runtime going; how that works depends on exactly which docker you install.  On a Linux, we rcommend `installing Docker Engline <https://docs.docker.com/engine/install/>`_.  On a Mac, you can also try that, but people have had success with `Docker Desktop <https://www.docker.com/products/docker-desktop>`_.

You can build all the docker images necessary to create a development/test environment by running the following in the top level directory of your git checkout::

  docker compose build

Once you've successfully built the docker environments, run::

  docker compose up -d webap
  docker compose up -d shell

(For those of you who know docker compose and are wondering why ``webap`` is not just a prerequisite for ``shell``, the reason is so one can get a debug environment up even when code errors prevent the web application from successfully starting.)

**NOTE**: sometimes some of the services seem to be failing to come up properly.  It's possible that this is happening because the checks in the docker compose file time out too fast.  You may be able to get it to work by just repeating the ``...docker compose up -d ...`` line; the second time around, it's possible everything will work.  If something doesn't work, look at the service that didn't come up, and try ``docker compose logs <service>`` to see if it sheds any light.

When you run these two commands, it will start a number of local servers (containers) on your machine, and will set up all the basic database tables.  You can run ``docker compose ps`` to see what containers are running.  Assuming you're running these commands on the same machine you're sitting at (i.e. you're running them on your laptop or desktop, not on a remote server you've connected to), and that everything worked, then after this you should be able to connect to the FASTDB web application with your browser by going to:

   http://localhost:8080

(You can change the port on your local machine from ``8080`` to something else by setting the ``WEBPORT`` environment variable before running ``docker compose``.)  This will give you the interactive web pages; however, the same URL can be used for API calls documented on :ref:`Using FASTDB <usage-docs>`.  Right after bringing it up, you won't be able to do much with it, because there are no FASTDB users configured.  See :ref:`creating-a-persistent-test-user` below.

The containers that get started by ``docker compose`` are, as of this writing:

  * A ``kafka`` zookeeper and a ``kafka`` server.  (TODO: use ``kraft`` so we don't need the zookeeper any more.)
  * A ``postgresql`` server
  * A ``mongodb`` server
  * A "query runner", which is a custom process that handles the "long query" interface
  * A web server that is the FASTDB front end
  * A shell server to which you can connect and run things.

You may notice that ``docker compose`` tells you that more than this was started.  There are some transitory servers, e.g. ``createdb``, that start, do their thing, and then stop.

Ideally, at this point you're done setting up your test/dev environment.  When you're finished with it, and want to clean up after yourself, just run, again in the top-level of your git checkout::

  docker compose down -v

(This must be run on the host system, *not* inside one of the containers.)  That command will remove all of the started servers, and wipe out all disk space allocated for databases and such.  (You will probably want to ``exit`` any shells you have running on containers before doing this.)

It's possible the shell server won't start, usually because the ``createdb`` step failed.  The first thing you should do is::

  docker compose logs createdb

to see if there's an obvious error message you know how to fix.  Failing that, you can run::

  docker compose up -d shell-nocreatedb

That will bring up a shell server you can connect to and work with that will have the Postgres and Mongo servers running, but which will (probably) not have the tables created on the Postgres server.  (It's also possible other steps will fail, in which more work may potentially be required.)

Please Don't Docker Push
------------------------

The `docker-compose.yaml` file will build docker images set up so that they can easily be pushed to Perlmutter's container image registrly.  Please do *not* run any docker push commands to push those images, unless you've tagged them differently and know what you're doing.  (If you really know what you're doing, you're always allowed to do *anything*.)


Working With the Test Installation
==================================

Assuming everything in the previous step worked, you can run, from the top level of the git checkout::

  docker compose exec -it shell /bin/bash

That will connect you to the shell container.  (You can tell you're inside the container because your prompt will start with "``I have no name!@``".)

If you want to run the tests in the ``tests`` subdirectory, you will first need to install the code to where it's expected; see :ref:`installing-for-tests`.  Once you're ready, inside the container go to the ``/code/tests`` directory and run various tests with ``pytest``.  If you just run ``pytest -v``, it will try to run all of them, but you can, as usual with pytest, give it just the file (or just the file and test) you want to run.


.. _installing-for-tests:

Installing for tests
--------------------

:ref:`installing-the-code` above describes the general procedure for installing the code.  If you want to install the code on your local test enviroment for use with the tests in the docker compose environment, then make sure you're inside the shell container, and run::

  cd /code
  ./configure --with-installdir=$PWD/install \
              --with-smtp-server=mailhog \
              --with-smtp-port=1025

If you get an error on the ``./configure`` line, it means one of two things.  Either you've edited the file ``aclocal.m4``, or you've edited the file ``Makefile.am`` in one of the subdirectories.  (Never edit any of the ``Makefile.in`` files, as these are all automatically generated.)  If you have edited one of these files, see :ref:`autoreconf-install` below.  If you haven't, then this is the result of autotools and git not agreeing about how file timestamps should be treated.  Try running::

  touch aclocal.m4 configure
  find . -name Makefile.am -exec touch \{\} \;
  find . -name Makefile.in -exec touch \{\} \;

and then redoing the ``./configure`` line.

Once your configure has worked, run::

  make install

.. _autoreconf-install:

If you've modified the base autotools files
-------------------------------------------

Usually, the ``./configure`` and ``make`` commands in the previous section are sufficient for installing the tests.  However, if you've modified ``configure.ac`` in the top level directory, or ``Makefile.am`` in any directory, then you need to rerun autotools to build all the derivative Makefiles.  This requires you to have things installed on your system which are *not* available inside the FASTDB docker container; specifically, you will need to have GNU Autotools installed.  On Linux, this is usually a simple matter of installing one or more packages.  (On Debian and close derivatives, the packages are probably called things like ``autoconf``, ``automake``, and ``autotools-dev``.)  On NERSC's Perlmutter, these should already be available to you by default.

Rebuilding all the derivative Makefiles is just a matter of running::

  autoreconf --install

before the ``./configure`` step described above.  Note, however, that ``autoreconf`` is *not* available inside the container.  You will need to run this on the host system, which must itself have autotools installed.


.. _unpacking-test-data:

Unpacking test data
-------------------

The tests will not yet run as-is.  Inside the ``tests`` subdirectory, you must run::

  tar xvf elasticc2_test_data.tar.bz2

in order create the expected test data on your local machine.  Note that ``bzip2`` is *not* installed inside the docker container, so you need to run this on your host machine.  You only need to do this once in your checkout; you do *not* have to do this every time you create a new set of docker containers.  (If the subdirectory ``tests/elasticc2_test_data`` has stuff in it, then you've probably already done this.)

Exiting the test environment
----------------------------

If you're inside the container, you can exit with ``exit`` (just like any other shell).  Once outside the container, assuming you're still in the ``tests`` subdirectory, you re-enter the (still-running) test container with another ``docker compose exec -it shell /bin/bash``.  If you want to tear down the test enviornment, run::

  docker compose down -v

This will completely tear down the environment.  All containers will be stopped, all volumes created for the environment (such as the backend storage for the test databases) will be wiped clean.  This is what you do if you want to make sure you're starting fresh.



Running the tests
-----------------

Once inside the container::

  cd /code/tests
  pytest -v

that will run all of the tests and tell you how they're doing.  As usually with ``pytest``, you can give filenames (and functions or classes/methods within those files) to just run some tests.

**WARNING**: it's possible the tests do not currently clean up after themselves (especially if some tests fail), so you may need to restart your environment after running tests before running them again.  If you hit ``CTRL-C`` while ``pytest`` is running, tests will almost certainly not have cleaned up after themselves.

What's more, right now, if you're running all of the tests, if an early test fails, it can cause a later test to fail, even though that later test wouldn't actually fail if the earlier tests had passed.  This is bad behvaior; if tests properly cleaned up after themselves (which they're supposed to do even if they fail), then the later tests shouldn't fail just because an earlier one does.  Until we get this behavior fixed, when looking at lots of tests at once, work on them in order, as the later tests might not "really" have failed.

You can always exit any shells running on containers, and tear down the whole environment with ``docker compose down -v``.  That will allow you to start up a new test environment (see :ref:`local-test-env`) and start over with empty databases.


Directly accessing the database
-------------------------------

If you want to directly access the database inside the test environment, inside the container run::

  psql -h postgres -U postgres fastdb

It will prompt you for a password, which is "fragile".  (This is a test environment local to your machine; never install a production environment with a password like that!)  You can now issue SQL commands, and do anything you might normally do with PostgreSQL using ``psql``.

TODO : instructions for accessing the mongo database.


.. _creating-a-persistent-test-user:


Setting yourself up to futz around with the web app
---------------------------------------------------

There will eventually be a better way to do this, as the current method is needlessly slow.  Right now, if you want to have a database with some stuff loaded into it for purposes of developing the web UI, what you can do is get yourself fully set up for tests, and then, inside the shell container, run::container, either run::

  cd /code/tests
  pytest -v --trace test_ltcv_object_search.py::test_object_search

or run::

  cd /code/tests
  pytest -v --trace services/test_sourceimporter.py::test_import_30days_60days

Both of these start tests with test fixtures that create a database user and load data into the database.  The ``--trace`` command tells pytest to stop at the begining of a test, after the fixture has run.  The shell where you run this will dump you into a ``(Pdb)`` prompt.  Just leave that shell sitting there.  At this point, you have a loaded database.  You can look at ``localhost:8080`` in your web browser to see the web ap, and log in with user ``test`` and password ``test_password``.

The ``test_object_search`` command takes about 10 seconds to run, and loads up the main postgres tables with the test data.  It does *not* load anyting into the mongo database.  The ``test_import_30days_60days`` command takes up to a minute to run, because what it's really doing is testing a whole bunch of different servers, an there are built in sleeps so that each step of the test can be sure that other servers have had time to do their stuff.  This one loads the full test data set into the "ppdb" tables, and runs a 90 simulated days of alerts through some test brokers.  When it's done, the sources from those 90 simulated days will be in the main postgrest ables, and the mongo database will be populated with  the test broker messages.  (The test brokers aren't doing anything real, but are just assigning random classifications for purposes of testing the plubming.)

When you're done futzing around with the web ap, go to the shell where you ran ``pytest ...`` and just press ``c`` and hit Enter at the ``(Pdb)`` prompt.  The test will compete, exit, and (ideally) clean up after itself.

If you edit the web ap software and what to see the changes, you need to do a couple of things to see the changes.  First, you need to re-install the code.  On a shell inside the container (a different one from the one where your ``(Pdb)`` prompt is sitting), do ``cd /code`` and ``make install``.  (If you've added files, not just edited them, there is more to do; ROB TODO document this.)   Second, you need to get a shell on the webap.  Outside any container, in the ``tests`` directory, run ``docker compose exec -it webap /bin/bash``.  On the shell inside the webap container, run::

  kill -HUP 1

If all is well, then your webserver is now running the new code; shift-reload it in your browser to see it.  If the webap shell immediately exits after this ``kill`` command, it means you broker the server-side software enough that it no longer runs.  Do ``docker compose logs webap`` to see the logs, and try to fix the errors.  Once you've fixed them, you will need to do ``docker compose down webap`` and ``docker compose up -d webap`` to get the webap running again.


Creating a persistent test user
-------------------------------

TODO


Loading persistent test data
----------------------------

TODO



Note for Rob: Installing on Perlmutter
======================================

rknop_dev environment
---------------------

(This is a note for Rob about running a test environment on NERSC Spin.)

The base installation directory is::

  /global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev

In that directory, make sure there are subdirectories ``install``, ``query_results``, and ``sessions``, in additon to the ``FASTDB`` checkout generated with::

  git clone git@github.com::LSSTDESC/FASTDB
  cd FASTDB
  git checkout <version>
  git submodule update --init

The ``.yaml`` files defining the Spin workloads are in ``admin/spin/rknop_dev`` in the git archive.  (Note that, unless I've screwed up (...which has happend...), the files ``secrets.yaml`` and ``webserver-cert.yaml`` will not be complete, because those are the kinds of things you don't want to commit to a public git archive.  Edit those files to put in the actual passwords and SSL key/certificates before using them, and **make sure to remove the secret stuff before   committing anything to git**.  If you screw up, you have to change **all** the secrets.)  To install the code to work with those ``.yaml`` files, run::

  cd /global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev/FASTDB
  touch aclocal.m4 configure
  find . -name Makefile.am -exec touch \{\} \;
  find . -name Makefile.in -exec touch \{\} \;
  ./configure \
    --with-installdir=/global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev/install \
    --with-smtp-server=smtp.lbl.gov \
    --with-smtp-port=25 \
    --with-email-from=raknop@lbl.gov
  make install

This is necessary because the docker image for the web ap does *not* have the fastdb code baked into it.  Rather, it bind mounds the ``install`` directory and uses the code there.  (This allows development without having to rebuild the docker image.)
