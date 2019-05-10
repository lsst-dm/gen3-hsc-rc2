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
