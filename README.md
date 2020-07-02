HSC RC2 on Gen3 Middleware
==========================

The HSC RC2 dataset is a collection of three tracts (in the GAMA,
VVDS, and COSMOS fields) from the first public data release of the HSC
SSP survey, used for regular testing of the LSST Data Release
Production pipelines.

A full description of the dataset can be found on
[DM-11345](https://jira.lsstcorp.org/browse/DM-11345), and a description of
processing runs can be found
[here](https://confluence.lsstcorp.org/display/DM/Reprocessing+of+the+HSC+RC2+dataset).

This repository contains scripts and configuration for processing this
dataset with LSST's new "Generation 3" middleware:
[lsst.daf.butler.Butler](https://pipelines.lsst.io/py-api/lsst.daf.butler.Butler.html#lsst.daf.butler.Butler)
and
[lsst.pipe.base.PipelineTask](https://pipelines.lsst.io/py-api/lsst.pipe.base.PipelineTask.html#lsst.pipe.base.PipelineTask).


Creating a Data Repository
--------------------------

The `bootstrap.py` command can be used to create a new Gen3 data repository
on a system that already has a Gen2 data repository with the RC2 dataset.

The paths in that file are currently set to be appropriate for `lsst-devN@ncsa.illinois.edu`, and must be manually modified for systems with
different Gen2 repository paths.

The script has options (see `bootstrap.py --help`) to ingest only individual
tracts and/or individual bands, but note that some steps (master calibration ingest and skymap registry) ignore these options.  Master calibration ingest in particular will ingest the entirety of the Gen2 calibration repo it is pointed at, and can be quite slow.

Using the Data Repository
-------------------------

The `bootstrap.py` script produces a repo with the following
collections:

 - `HSC/calib`
 - `HSC/raw/all`
 - `skymaps`
 - `HSC/masks`
 - `refcats`

To improve preflight performance (by simplifying the queries involved), it
may be useful to explicitly state which collections should be used to obtain
instances of particular dataset types.  For example, pipelines that include ISR would pass
```
'flat:HSC/calib','dark:HSC/calib','bias:HSC/calib','camera:HSC/calib','bfKernel:HSC/calib','defects:HSC/calib','transmission_optics:HSC/calib','transmission_filter:HSC/calib','transmission_sensor:HSC/calib','transmission_atmosphere:HSC/calib','HSC/raw'
```
as the `-i` argument to `pipetask`.

Configuration files for concrete PipelineTasks are being added incrementally to the repository as they are tested, in the `config` directory.  These will automatically read config files from obs_subaru as needed, so they should be the only configs that need to be passed on the command-line when running `pipetask`.
