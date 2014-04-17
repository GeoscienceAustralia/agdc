import os
import dbutil
import subprocess

def main():
    dbutil.TESTSERVER.drop("benchmark")
if __name__ == '__main__':
    main()
