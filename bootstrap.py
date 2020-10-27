#!/usr/bin/env python

from __future__ import annotations

import shutil
import os
from typing import List

import lsst.log.utils
from lsst.obs.base.gen2to3 import ConvertRepoTask, Rerun
from lsst.obs.base import Instrument
from lsst.obs.subaru import HyperSuprimeCam
from lsst.daf.butler import Butler, DatasetType
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.pipe.tasks.makeSkyMap import MakeSkyMapTask

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

# Specifications for known groups of reruns to be converted.  Keys are just
# names to be used to make command-line argument parsing easier.  Convention
# here is to make that the same as the chainName for one of the Reruns, which
# should be the one that has all others as parents and hence includes all
# outputs.
RERUNS = {
    "RC2/w_2020_19": [
        Rerun(
            path="rerun/RC/w_2020_19/DM-24822-sfm",
            runName="RC2/w_2020_19/DM-24822/sfm",
            chainName=None,
            parents=[]
        ),
        Rerun(
            path="rerun/RC/w_2020_19/DM-24822",
            runName="RC2/w_2020_19/DM-24822/remainder",
            chainName="RC2/w_2020_19",
            parents=["RC2/w_2020_19/DM-24822/sfm", "HSC/calib"],
        )
    ],
    "RC2/w_2020_22": [
        Rerun(
            path="rerun/RC/w_2020_22/DM-25176-sfm",
            runName="RC2/w_2020_22/DM-25176/sfm",
            chainName=None,
            parents=[]
        ),
        Rerun(
            path="rerun/RC/w_2020_22/DM-25176",
            runName="RC2/w_2020_22/DM-25176/remainder",
            chainName="RC2/w_2020_22",
            parents=["RC2/w_2020_22/DM-25176/sfm", "HSC/calib"],
        )
    ],
    "RC2/v20_0_0_rc1": [
        Rerun(
            path="rerun/RC/v20_0_0_rc1/DM-25349-sfm",
            runName="RC2/v20_0_0_rc1/DM-25349/sfm",
            chainName=None,
            parents=[]
        ),
        Rerun(
            path="rerun/RC/v20_0_0_rc1/DM-25349",
            runName="RC2/v20_0_0_rc1/DM-25349/remainder",
            chainName="RC2/v20_0_0_rc1",
            parents=["RC2/v20_0_0_rc1/DM-25349/sfm", "HSC/calib"],
        )
    ],
    "RC2/w_2020_26": [
        Rerun(
            path="rerun/RC/w_2020_26/DM-25714-sfm",
            runName="RC2/w_2020_26/DM-25714/sfm",
            chainName=None,
            parents=[]
        ),
        Rerun(
            path="rerun/RC/w_2020_26/DM-25714",
            runName="RC2/w_2020_26/DM-25714/remainder",
            chainName="RC2/w_2020_26",
            parents=["RC2/w_2020_26/DM-25714/sfm", "HSC/calib"],
        )
    ],
    "RC2/w_2020_30": [
        Rerun(
            path="rerun/RC/w_2020_30/DM-26105-sfm",
            runName="RC2/w_2020_30/DM-26105/sfm",
            chainName=None,
            parents=[]
        ),
        Rerun(
            path="rerun/RC/w_2020_30/DM-26105",
            runName="RC2/w_2020_30/DM-26105/remainder",
            chainName="RC2/w_2020_30",
            parents=["RC2/w_2020_30/DM-26105/sfm", "HSC/calib"],
        )
    ],
    "RC2/w_2020_34": [
        Rerun(
            path="rerun/RC/w_2020_34/DM-26441-sfm",
            runName="RC2/w_2020_34/DM-26441/sfm",
            chainName=None,
            parents=[]
        ),
        Rerun(
            path="rerun/RC/w_2020_34/DM-26441",
            runName="RC2/w_2020_34/DM-26441/remainder",
            chainName="RC2/w_2020_34",
            parents=["RC2/w_2020_34/DM-26441/sfm", "HSC/calib"],
        )
    ],
    "RC2/w_2020_36": [
        Rerun(
            path="rerun/RC/w_2020_36/DM-26637-sfm",
            runName="RC2/w_2020_36/DM-26637/sfm",
            chainName=None,
            parents=[]
        ),
        Rerun(
            path="rerun/RC/w_2020_36/DM-26637",
            runName="RC2/w_2020_36/DM-26637/remainder",
            chainName="RC2/w_2020_36",
            parents=["RC2/w_2020_36/DM-26637/sfm", "HSC/calib"],
        )
    ],

}


GEN2_RAW_ROOT = "/datasets/hsc/repo"


def configureLogging(level):
    lsst.log.configure_prop("""
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.out
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern=%-5p %d{yyyy-MM-ddTHH:mm:ss.SSSZ} %c - %m%n
""")
    log = lsst.log.Log.getLogger("convertRepo")
    log.setLevel(level)


def makeVisitList(tracts: List[int], filters: List[str]):
    visits = []
    for tract in tracts:
        for filter in filters:
            visits.extend(VISITS[tract][filter])
    return visits


def makeTask(butler: Butler, *, continue_: bool = False, reruns: List[Rerun]):
    instrument = HyperSuprimeCam()
    config = ConvertRepoTask.ConfigClass()
    instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
    config.relatedOnly = True
    config.transfer = "symlink"
    if not reruns:
        # No reruns, so just include datasets we want from the root and calib
        # repos (default is all datasets).
        config.datasetIncludePatterns = ["brightObjectMask", "flat", "bias", "dark", "fringe", "sky",
                                         "ref_cat", "raw"]
    config.datasetIgnorePatterns.append("*_camera")
    config.datasetIgnorePatterns.append("yBackground")
    config.datasetIgnorePatterns.append("fgcmLookUpTable")
    config.fileIgnorePatterns.extend(["*.log", "*.png", "rerun*"])
    config.doRegisterInstrument = not continue_
    config.doWriteCuratedCalibrations = not continue_
    return ConvertRepoTask(config=config, butler3=butler, instrument=instrument)


def putSkyMap(butler: Butler, instrument: Instrument):
    datasetType = DatasetType(name="deepCoadd_skyMap", dimensions=["skymap"], storageClass="SkyMap",
                              universe=butler.registry.dimensions)
    butler.registry.registerDatasetType(datasetType)
    run = "skymaps"
    butler.registry.registerRun(run)
    skyMapConfig = MakeSkyMapTask.ConfigClass()
    instrument.applyConfigOverrides(MakeSkyMapTask._DefaultName, skyMapConfig)
    skyMap = skyMapConfig.skyMap.apply()
    butler.put(skyMap, datasetType, skymap="hsc_rings_v1", run=run)


def run(root: str, *, tracts: List[int], filters: List[str],
        create: bool = False, clobber: bool = False, continue_: bool = False,
        reruns: List[Rerun]):
    if create:
        if continue_:
            raise ValueError("Cannot continue if create is True.")
        if os.path.exists(root):
            if clobber:
                shutil.rmtree(root)
            else:
                raise ValueError("Repo exists and --clobber=False.")
        Butler.makeRepo(root)
    if reruns and set(filters) != set("grizy"):
        raise ValueError("All filters must be included if reruns are converted.")
    butler = Butler(root, run="HSC/raw/all")
    task = makeTask(butler, continue_=continue_, reruns=reruns)
    task.run(
        root=GEN2_RAW_ROOT,
        reruns=reruns,
        calibs=({"CALIB": "HSC/calib"} if not continue_ else None),
        visits=makeVisitList(tracts, filters)
    )
    if not continue_:
        task.log.info("Ingesting y-band stray light data.")
        task.instrument.ingestStrayLightData(Butler(root, run="HSC/calib"),
                                             directory=os.path.join(GEN2_RAW_ROOT, "CALIB", "STRAY_LIGHT"),
                                             transfer="symlink")
        task.log.info("Writing deepCoadd_skyMap to root repo.")
        try:
            putSkyMap(butler, task.instrument)
        except ConflictingDefinitionError:
            # Presumably this skymap was converted because we found a Gen2 one;
            # that's fine.
            pass


def main():
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
    parser.add_argument("--continue", dest="continue_", action="store_true", default=False,
                        help="Ingest more tracts and/or filters into a repo already containing calibs.")
    parser.add_argument("--tract", type=int, action="append",
                        help=("Ingest raws from this tract (may be passed multiple times; "
                              "default is all known tracts)."))
    parser.add_argument("--filter", type=str, action="append", choices=("g", "r", "i", "z", "y"),
                        help=("Ingest raws from this filter (may be passed multiple times; "
                              "default is grizy)."))
    parser.add_argument("--reruns", type=str, action="append", choices=tuple(RERUNS.keys()),
                        help=("Convert output products from this predefined set of Gen2 reruns.  "
                              "Not compatible with --filter.  May be passed multiple times."))
    parser.add_argument("-v", "--verbose", action="store_const", dest="verbose",
                        default=lsst.log.Log.INFO, const=lsst.log.Log.DEBUG,
                        help="Set the log level to DEBUG.")
    options = parser.parse_args()
    tracts = options.tract if options.tract else list(VISITS.keys())
    filters = options.filter if options.filter else list("grizy")
    reruns = []
    if options.reruns is not None:
        for key in options.reruns:
            reruns.extend(RERUNS[key])
    configureLogging(options.verbose)
    run(options.root, tracts=tracts, filters=filters, create=options.create, clobber=options.clobber,
        continue_=options.continue_, reruns=reruns)


if __name__ == "__main__":
    main()
