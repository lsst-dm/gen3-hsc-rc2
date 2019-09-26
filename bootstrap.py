#!/usr/bin/env python

import sqlite3
import shutil
import os

import lsst.log.utils
from lsst.obs.base.gen3 import BootstrapRepoTask, BootstrapRepoInputs
from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam
from lsst.daf.butler import Butler
from lsst.daf.persistence import Butler as Butler2
from lsst.daf.persistence.butlerExceptions import NoResults

VISITS = {
    0: {
        "r": [
            903334, 903336, 903338, 903342, 903344, 903346
        ],
        "i": [
            903986, 904014, 903990, 904010, 903988
        ],
    },
}

GEN2_RAW_ROOT = "/datasets/hsc/repo"
GEN2_CALIB_ROOT = "/datasets/hsc/repo/CALIB"
REFCAT_ROOT = "/datasets/refcats/htm/v1"
BRIGHT_OBJECT_MASK_ROOT = GEN2_RAW_ROOT


def computeFilesForVisits(visits):
    with lsst.log.utils.temporaryLogLevel("CameraMapper", lsst.log.Log.WARN):
        butler = Butler2(GEN2_RAW_ROOT)
        for visit in visits:
            for ccd in range(104):
                try:
                    f = butler.get("raw_filename", visit=visit, ccd=ccd)[0]
                except NoResults:
                    continue
                yield f


def configureLogging():
    lsst.log.configure_prop("""
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.out
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern=%-5p %d{yyyy-MM-ddTHH:mm:ss.SSSZ} %c - %m%n
""")


def makeInputs(tracts, filters):
    instrument = HyperSuprimeCam()
    raws = []
    for tract in tracts:
        for filter in filters:
            raws.extend(computeFilesForVisits(VISITS[tract][filter]))
    return BootstrapRepoInputs(instrument=instrument, raws=raws,
                               refCatRoot=REFCAT_ROOT,
                               brightObjectMaskRoot=BRIGHT_OBJECT_MASK_ROOT,
                               calibRoot=GEN2_CALIB_ROOT)


def makeTask(butler):
    instrument = HyperSuprimeCam()
    config = BootstrapRepoTask.ConfigClass()
    instrument.applyConfigOverrides(BootstrapRepoTask._DefaultName, config)
    config.raws.onError = "break"
    config.raws.transfer = "symlink"
    for sub in config.refCats.values():
        sub.transfer = "symlink"
    config.brightObjectMasks.transfer = "symlink"
    config.calibrations.transfer = "symlink"
    return BootstrapRepoTask(config=config, butler=butler)


def main(root, *, tracts, filters, create=False, clobber=False):
    configureLogging()
    inputs = makeInputs(tracts, filters)
    if create:
        if os.path.exists(root):
            if clobber:
                shutil.rmtree(root)
            else:
                raise ValueError("Repo exists and clobber=False.")
        Butler.makeRepo(root)
    butler = Butler(root, run="raw/hsc")
    task = makeTask(butler)
    task.run(inputs)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Bootstrap a Gen3 Butler data repository with HSC RC2 data."
    )
    parser.add_argument("root", type=str, metavar="PATH",
                        help="Gen3 repo root (or config file path.)")
    parser.add_argument("--create", action="store_true", default=False,
                        help="Create the repo before attempting to populating it.")
    parser.add_argument("--clobber", action="store_true", default=False,
                        help="Remove any existing repo if --create is passed (ignored otherwise).")
    parser.add_argument("--tract", type=int, action="append",
                        help=("Ingest raws from this tract (may be passed multiple times; "
                              "default is all known tracts)."))
    parser.add_argument("--filter", type=str, action="append", choices=("g", "r", "i", "z", "y"),
                        help=("Ingest raws from this filter (may be passed multiple times; "
                              "default is grizy)."))
    options = parser.parse_args()
    tracts = options.tract if options.tract else list(VISITS.keys())
    filters = options.filter if options.filter else list("grizy")
    main(options.root, tracts=tracts, filters=filters, create=options.create, clobber=options.clobber)
