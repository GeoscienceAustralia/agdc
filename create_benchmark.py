import os
import dbutil
import subprocess

def main():
    print "About to create benchmark database"
    dbutil.TESTSERVER.create("benchmark", "/g/data/v10/test_resources/mph547/output/dbupdater/TestDBUpdater", "onescene.sql") 
    print "Finished creating benchmark database"
if __name__ == '__main__':
    print "Abount to call main()"
    main()
    print "Finsished calling main"
