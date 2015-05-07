#!/usr/bin/env python

#===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

"""dbcleanup.py - Script to clean up the test database server.

This script attempts to drop all the temporary test databases on
the test database server. These may be left behind if the test that
creates them is unable to remove them after running for some reason.

It uses the dbutil TESTSERVER object, which can drop databases being
pooled by pgbouncer.

Note that running this script may cause tests currently running to fail
(by dropping their databases out from under them).
"""

import re
import dbutil

#
# Temporary test database pattern
#
# This is the regular expression used to identify a test database.
#
# The current pattern looks for a name containing 'test' and ending
# in an underscore followed by a 9 digit number.
#

TESTDB_PATTERN = r".*test.*_\d{9}$"

#
# Main program
#

db_list = dbutil.TESTSERVER.dblist()

test_db_list = [db for db in db_list if re.match(TESTDB_PATTERN, db)]

print "Dropping temporary test databases:"

if test_db_list:
    for db_name in db_list:
        if re.match(TESTDB_PATTERN, db_name):
            print "    %s" % db_name
            dbutil.TESTSERVER.drop(db_name)

else:
    print "    nothing to do."
