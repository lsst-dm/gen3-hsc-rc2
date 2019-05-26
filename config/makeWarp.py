
from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam

# makeWarp is a Gen3-only task, but the obs_subaru config overrides for
# makeCoaddTempExp currently apply cleanly to it; this may not be guaranteed
# going forward, but it's a way to avoid duplication for now.
HyperSuprimeCam().applyConfigOverrides("makeCoaddTempExp", config)

# Awaiting jointcal-as-PipelineTask.
config.doApplyUberCal = False

# Awaiting skyCorrection-as-PipelineTask.
config.doApplySkyCorr = False

# Expected null outputs shouldn't invalidate the QuantumGraph.
config.doWriteEmptyWarps = True
