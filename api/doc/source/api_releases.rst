
.. toctree::
   :maxdepth: 2


API Releases
============

AGDC API v0.1.0 (2015-05-13 snapshot)
-------------------------------------

The source code is available on the ``api`` branch from the ``agdc-api-0.1.0-b20150513`` tag.

The packaged version is available as a loadable module on ``raijin`` at the NCI as ``agdc-api/0.1.0-b20150513`` in
both ``u46`` and ``el8``::

    $ module load agdc-api/0.1.0-b20150513

**Changes in this release...**

* Bug - `Issue 51 Time series retrieval error <https://github.com/GeoscienceAustralia/agdc/issues/51>`_

AGDC API v0.1.0 (2015-05-12 snapshot)
-------------------------------------

The source code is available on the ``api`` branch from the ``agdc-api-0.1.0-b20150512`` tag.

The packaged version is available as a loadable module on ``raijin`` at the NCI as ``agdc-api/0.1.0-b20150512`` in
both ``u46`` and ``el8``::

    $ module load agdc-api/0.1.0-b20150512

**Changes in this release...**

* Enhancement - `Issue 36 Add the ability to query the AGDC by area of interest polygons <https://github.com/GeoscienceAustralia/agdc/issues/36>`_
* Enhancement - `Issue 47 Implement Retrieve Area of Interest Time Series tool <https://github.com/GeoscienceAustralia/agdc/issues/47>`_

AGDC API v0.1.0 (2015-05-07 snapshot)
-------------------------------------

The source code is available on the ``api`` branch from the ``agdc-api-0.1.0-b20150507`` tag.

The packaged version is available as a loadable module on ``raijin`` at the NCI as ``agdc-api/0.1.0-b20150507`` in
both ``u46`` and ``el8``::

    $ module load agdc-api/0.1.0-b20150507

**Changes in this release...**

* Enhancement - `Issue 43 Enhance the Retrieve Dataset tool with the ability to apply a vector mask <https://github.com/GeoscienceAustralia/agdc/issues/43>`_
* Enhancement - `Issue 44 Enhance the Retrieve Dataset Stack tool with the ability to apply a vector mask <https://github.com/GeoscienceAustralia/agdc/issues/44>`_

AGDC API v0.1.0 (2015-05-05 snapshot)
-------------------------------------

The source code is available on the ``api`` branch from the ``agdc-api-0.1.0-b20150505`` tag.

The packaged version is available as a loadable module on ``raijin`` at the NCI as ``agdc-api/0.1.0-b20150505`` in
both ``u46`` and ``el8``::

    $ module load agdc-api/0.1.0-b20150505

**Changes in this release...**

* Enhancement - `Issue 29 Enhance Retrieve Dataset tool to support ENVI output format <https://github.com/GeoscienceAustralia/agdc/issues/29>`_
* Enhancement - `Issue 32 Enhance Retrieve Dataset Stack tool to support ENVI output format <https://github.com/GeoscienceAustralia/agdc/issues/32>`_
* Enhancement - `Issue 23 Implement Retrieve Dataset Stack Luigi workflow <https://github.com/GeoscienceAustralia/agdc/issues/23>`_
* Enhancement - `Issue 41 Enhance the Retrieve Dataset Stack Luigi workflow to support ENVI output format <https://github.com/GeoscienceAustralia/agdc/issues/41>`_

AGDC API v0.1.0 (2015-04-30 snapshot)
-------------------------------------

The source code is available on the ``api`` branch from the ``agdc-api-0.1.0-b20150430`` tag.

The packaged version is available as a loadable module on ``raijin`` at the NCI as ``agdc-api/0.1.0-b20150430`` in
both ``u46`` and ``el8``::

    $ module load agdc-api/0.1.0-b20150430

**Changes in this release...**

* Enhancement - `Issue 31 Implement Retrieve Dataset Stack tool <https://github.com/GeoscienceAustralia/agdc/issues/31>`_
* Enhancement - `Issue 40 Allow Retrieve Dataset Stack tool to operate over satellites with non-identical bands <https://github.com/GeoscienceAustralia/agdc/issues/40>`_

AGDC API v0.1.0 (2015-04-28 snapshot)
-------------------------------------

The source code is available on the ``api`` branch from the ``agdc-api-0.1.0-b20150428`` tag.

The packaged version is available as a loadable module on ``raijin`` at the NCI as ``agdc-api/0.1.0-b20150428`` in
both ``u46`` and ``el8``::

    $ module load agdc-api/0.1.0-b20150428

**Changes in this release...**

* Enhancement - `Issue 28 Implement Retrieve Dataset tool <https://github.com/GeoscienceAustralia/agdc/issues/28>`_

AGDC API v0.1.0 (2015-04-27 snapshot)
-------------------------------------

The source code is available on the ``api`` branch from the ``agdc-api-0.1.0-b20150427`` tag.

The packaged version is available as a loadable module on ``raijin`` at the NCI as ``agdc-api/0.1.0-b20150427`` in
both ``u46`` and ``el8``::

    $ module load agdc-api/0.1.0-b20150427

**Changes in this release...**

* Enhancement - `Issue 35 Implement query API <https://github.com/GeoscienceAustralia/agdc/issues/35>`_
* Enhancement - `Issue 37 Add the ability to query the AGDC and export the results to CSV file <https://github.com/GeoscienceAustralia/agdc/issues/37>`_
* Enhancement - `Issue 38 Add the ability to query the AGDC for WOFS datasets <https://github.com/GeoscienceAustralia/agdc/issues/38>`_
* Enhancement - `Issue 12 Add ability to query for "DSM" datasets <https://github.com/GeoscienceAustralia/agdc/issues/12>`_
* Enhancement - `Issue 21 Add ability to query for "missing" datasets <https://github.com/GeoscienceAustralia/agdc/issues/21>`_
* Enhancement - `Issue 27 Implement Retrieve Pixel Time Series tool <https://github.com/GeoscienceAustralia/agdc/issues/27>`_
* Bug         - `Issue 20 Satellite filter not being applied for WATER tiles in Retrieve Pixel Time Series tool <https://github.com/GeoscienceAustralia/agdc/issues/20>`_
* Enhancement - `Issue 24 Allow retrieve pixel drill tool to operate over satellites with non-identical band <https://github.com/GeoscienceAustralia/agdc/issues/24>`_

Packaging an AGDC API Release
-----------------------------

The AGDC API is packaged as a loadable module on ``raijin`` at the NCI.

The ``agdc-api`` module has the following dependencies:

* ``python/2.7.6``
* ``psycopg2``
* ``gdal``
* ``enum34``
* ``luigi-mpi``
* ``psutil``
* ``numpy``
* ``scipy``

The first step in building the AGDC API module is to obtain the source code from the `AGDC GitHub Repository <https://github.com/GeoscienceAustralia/agdc>`_.

Then do something like the following to build the package::

    $ cd source/agdc-api/api

    $ MY_MODULE_NAME=agdc-api
    $ MY_MODULE_VERSION=0.1.0-b20150430
    $ #MY_MODULE_DIR=$HOME/modules
    $ #MY_MODULE_DIR=/projects/u46/opt/modules
    $ #MY_MODULE_DIR=/projects/el8/opt/modules

    $ mkdir -p $MY_MODULE_DIR/$MY_MODULE_NAME/$MY_MODULE_VERSION/lib/python2.7/site-packages

    $ PYTHONPATH=$PYTHONPATH:$MY_MODULE_DIR/$MY_MODULE_NAME/$MY_MODULE_VERSION/lib/python2.7/site-packages python setup.py install --prefix $MY_MODULE_DIR/$MY_MODULE_NAME/$MY_MODULE_VERSION

The add a module file:

.. code-block:: text
    :caption: ``/projects/el8/opt/modules/modulefiles/agdc-api/0.1.0-b20150430``

    #%Module########################################################################
    ##
    ## agdc-api modulefile
    ##
    proc ModulesHelp { } {
            global version

            puts stderr "   This module loads the AGDC API module"
            puts stderr "   Version $version"
    }

    set version       0.1.0-b20150430
    set name          agdc-api
    set base          /projects/u46/opt/modules

    module-whatis   "Loads the AGDC API version $version"

    if { [ module-info mode load ] } {

        if { ! [is-loaded enum34] } {
            module load enum34
        }

        if { ! [is-loaded gdal] } {
            module load gdal
        }

        if { ! [is-loaded psycopg2] } {
            module load psycopg2
        }

        if { ! [is-loaded python/2.7.6] } {
            module load python/2.7.6
        }

        if { ! [is-loaded luigi-mpi] } {
            module load luigi-mpi
        }

        if { ! [is-loaded psutil] } {
            module load psutil
        }

        if { ! [is-loaded numpy] } {
            module load numpy
        }

        if { ! [is-loaded scipy] } {
            module load scipy
        }

        prepend-path        PYTHONPATH $base/$name/$version/lib/python2.7/site-packages
        prepend-path        PATH $base/$name/$version/bin
    }

    if { [ module-info mode remove ] } {
        remove-path PYTHONPATH $base/$name/$version/lib/python2.7/site-packages
        remove-path PATH $base/$name/$version/bin
        module unload enum34
        module unload gdal
        module unload psycopg2
        module unload python/2.7.6
        module unload luigi-mpi
        module unload psutil
        module unload numpy
        module unload scipy
    }

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

