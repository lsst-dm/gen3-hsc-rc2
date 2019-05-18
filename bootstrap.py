#!/usr/bin/env python

import sqlite3
import shutil
import os

import lsst.log.utils
from lsst.obs.base.gen3 import BootstrapRepoTask, BootstrapRepoInputs
from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam
from lsst.daf.butler import Butler
from lsst.daf.persistence import Butler as Butler2

VISITS = {
    9615: {
        "g": [
            26024, 26028, 26032, 26036, 26044, 26046, 26048, 26050, 26058,
            26060, 26062, 26070, 26072, 26074, 26080, 26084, 26094
        ],
        "r": [
            23864, 23868, 23872, 23876, 23884, 23886, 23888, 23890, 23898,
            23900, 23902, 23910, 23912, 23914, 23920, 23924, 28976
        ],
        "i": [
            1258, 1262, 1270, 1274, 1278, 1280, 1282, 1286, 1288, 1290, 1294,
            1300, 1302, 1306, 1308, 1310, 1314, 1316, 1324, 1326, 1330, 24494,
            24504, 24522, 24536, 24538
        ],
        "z": [
            23212, 23216, 23224, 23226, 23228, 23232, 23234, 23242, 23250,
            23256, 23258, 27090, 27094, 27106, 27108, 27116, 27118, 27120,
            27126, 27128, 27130, 27134, 27136, 27146, 27148, 27156
        ],
        "y": [
            380, 384, 388, 404, 408, 424, 426, 436, 440, 442, 446, 452, 456,
            458, 462, 464, 468, 470, 472, 474, 478, 27032, 27034, 27042,
            27066, 27068
        ],
    },
    9697: {
        "g": [
            6320, 34338, 34342, 34362, 34366, 34382, 34384, 34400, 34402,
            34412, 34414, 34422, 34424, 34448, 34450, 34464, 34468, 34478,
            34480, 34482, 34484, 34486
        ],
        "r": [
            7138, 34640, 34644, 34648, 34652, 34664, 34670, 34672, 34674,
            34676, 34686, 34688, 34690, 34698, 34706, 34708, 34712, 34714,
            34734, 34758, 34760, 34772
        ],
        "i": [
            35870, 35890, 35892, 35906, 35936, 35950, 35974, 36114, 36118,
            36140, 36144, 36148, 36158, 36160, 36170, 36172, 36180, 36182,
            36190, 36192, 36202, 36204, 36212, 36214, 36216, 36218, 36234,
            36236, 36238, 36240, 36258, 36260, 36262
        ],
        "z": [
            36404, 36408, 36412, 36416, 36424, 36426, 36428, 36430, 36432,
            36434, 36438, 36442, 36444, 36446, 36448, 36456, 36458, 36460,
            36466, 36474, 36476, 36480, 36488, 36490, 36492, 36494, 36498,
            36504, 36506, 36508, 38938, 38944, 38950
        ],
        "y": [
            34874, 34942, 34944, 34946, 36726, 36730, 36738, 36750, 36754,
            36756, 36758, 36762, 36768, 36772, 36774, 36776, 36778, 36788,
            36790, 36792, 36794, 36800, 36802, 36808, 36810, 36812, 36818,
            36820, 36828, 36830, 36834, 36836, 36838
        ],
    },
    9813: {
        "g": [
            11690, 11692, 11694, 11696, 11698, 11700, 11702, 11704, 11706,
            11708, 11710, 11712, 29324, 29326, 29336, 29340, 29350
        ],
        "r": [
            1202, 1204, 1206, 1208, 1210, 1212, 1214, 1216, 1218, 1220, 23692,
            23694, 23704, 23706, 23716, 23718
        ],
        "i": [
            1228, 1230, 1232, 1238, 1240, 1242, 1244, 1246, 1248, 19658,
            19660, 19662, 19680, 19682, 19684, 19694, 19696, 19698, 19708,
            19710, 19712, 30482, 30484, 30486, 30488, 30490, 30492, 30494,
            30496, 30498, 30500, 30502, 30504
        ],
        "z": [
            1166, 1168, 1170, 1172, 1174, 1176, 1178, 1180, 1182, 1184, 1186,
            1188, 1190, 1192, 1194, 17900, 17902, 17904, 17906, 17908, 17926,
            17928, 17930, 17932, 17934, 17944, 17946, 17948, 17950, 17952,
            17962
        ],
        "y": [
            318, 322, 324, 326, 328, 330, 332, 344, 346, 348, 350, 352, 354,
            356, 358, 360, 362, 1868, 1870, 1872, 1874, 1876, 1880, 1882,
            11718, 11720, 11722, 11724, 11726, 11728, 11730, 11732, 11734,
            11736, 11738, 11740, 22602, 22604, 22606, 22608, 22626, 22628,
            22630, 22632, 22642, 22644, 22646, 22648, 22658, 22660, 22662,
            22664
        ],
    }
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
                yield butler.get("raw_filename", visit=visit, ccd=ccd)[0]


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
