from __future__ import absolute_import
#!/usr/bin/env python

#===============================================================================
# Copyright 2015 Geoscience Australia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

from . import dbutil

def main():
    #dbutil.TESTSERVER.drop("expected")
    #dbutil.TESTSERVER.drop("output")
    dbutil.TESTSERVER.drop("test_tiler_505477438")
    dbutil.TESTSERVER.drop("test_tiler_612406082")
if __name__ == '__main__':
    main()
