#!/usr/bin/env python

import sys
import getpass

from lsst.daf.butler import Butler, Config

username = getpass.getuser()

config = Config()
config[".registry.db"] = f"postgresql://{username}@lsst-pg-prod1.ncsa.illinois.edu:5432/lsstdb1"
config[".registry.namespace"] = username

root = sys.argv[1]
Butler.makeRepo(root, config=config)
