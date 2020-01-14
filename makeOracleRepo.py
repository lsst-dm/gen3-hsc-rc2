#!/usr/bin/env python

import sys

from lsst.daf.butler import Butler, Config

config = Config()
config[".registry.db"] = "oracle+cx_oracle://@gen3_cred_jbosch_1"

root = sys.argv[1]
Butler.makeRepo(root, config=config)
