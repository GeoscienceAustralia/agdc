import os
import dbutil
import subprocess

def main():
    #dbutil.TESTSERVER.drop("expected")
    #dbutil.TESTSERVER.drop("output")
    dbutil.TESTSERVER.drop("test_tiler_505477438")
    dbutil.TESTSERVER.drop("test_tiler_612406082")
if __name__ == '__main__':
    main()
