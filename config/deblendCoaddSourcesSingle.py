from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam
HyperSuprimeCam().applyConfigOverrides("deblendCoaddSourcesSingle", config)

config.singleBandDeblend.propagateAllPeaks=True
