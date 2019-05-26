#!/usr/bin/env python

from lsst.daf.butler import Butler, Config

config = Config()
config[".registry.cls"] = "lsst.daf.butler.registries.oracleRegistry.OracleRegistry"
config[".registry.db"] = "oracle+cx_oracle://@gen3_cred_jbosch_1"

root = "/project/jbosch/gen3/RC2/repo-oracle"
Butler.makeRepo(root, config=config)
