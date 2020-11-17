#!/usr/bin/env python

import getpass

import sqlalchemy


def main():
    username = getpass.getuser()
    uri = f"postgresql://{username}@lsst-pg-prod1.ncsa.illinois.edu:5432/lsstdb1"
    engine = sqlalchemy.create_engine(uri)
    tables_sql = sqlalchemy.sql.text("SELECT tablename FROM pg_tables WHERE schemaname=:namespace")
    tables = [row["tablename"] for row in engine.execute(tables_sql, namespace=username)]
    for table in tables:
        engine.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')


if __name__ == "__main__":
    main()
