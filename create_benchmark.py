import os
import dbutil
import subprocess

def main():
    dbutil.TESTSERVER.create("expected", "/g/data1/v10/test_resources/mph547/expected/landsat_tiler/Test_landsat_tiler", "tiler_testing.sql")
    dbutil.TESTSERVER.create("output", "/g/data1/v10/test_resources/mph547/output/landsat_tiler/Test_landsat_tiler", "tiler_testing.sql")
if __name__ == '__main__':
    main()
