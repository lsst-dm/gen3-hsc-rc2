#!/bin/bash

pipetask -i 'calibs/hsc/default','raw/hsc','refcats/ps1_pv3','skymaps','masks/hsc' \
-p lsst.meas.base -p lsst.ip.isr -p lsst.pipe.tasks \
"$@" \
-t isrTask.IsrTask:isr -C isr:config/isr.py \
-t characterizeImage.CharacterizeImageTask:cit -C cit:config/charImage.py \
-t calibrate.CalibrateTask:ct -C ct:config/calibrate.py \
-t makeCoaddTempExp.MakeWarpTask:mwt -C mwt:config/makeWarp.py \
-t assembleCoadd.CompareWarpAssembleCoaddTask:cwact -C cwact:config/compareWarpAssembleCoadd.py \
-t multiBand.DetectCoaddSourcesTask:dcst -C dcst:config/detectCoaddSources.py \
-t mergeDetections.MergeDetectionsTask:mdt -C mdt:config/mergeCoaddDetections.py \
-t deblendCoaddSourcesPipeline.DeblendCoaddSourcesSingleTask:dcsst -C dcsst:config/deblendCoaddSourcesSingle.py \
-t multiBand.MeasureMergedCoaddSourcesTask:mmcst -C mmcst:config/measureCoaddSources.py \
-t mergeMeasurements.MergeMeasurementsTask:mmt -C mmt:config/mergeCoaddMeasurements.py \
-t forcedPhotCcd.ForcedPhotCcdTask:fpccdt -C fpccdt:config/forcedPhotCcd.py \
-t forcedPhotCoadd.ForcedPhotCoaddTask:fpct -C fpct:config/forcedPhotCoadd.py
