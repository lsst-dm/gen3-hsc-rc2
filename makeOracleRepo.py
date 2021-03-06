#!/usr/bin/env python

import sys
import getpass

from lsst.daf.butler import Butler, Config

config = Config()
config[".registry.db"] = f"oracle+cx_oracle://@gen3_cred_{getpass.getuser()}_1"

root = sys.argv[1]
Butler.makeRepo(root, config=config)
