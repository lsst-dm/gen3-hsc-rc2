from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam
HyperSuprimeCam().applyConfigOverrides("mergeCoaddDetections", config)

# Gen3 priority list needs to be abstract filters, not physical filters.
config.priorityList = ["i", "r", "z", "g", "y"]
