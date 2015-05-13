#!/usr/bin/env python

# ===============================================================================
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
# ===============================================================================


from datetime import datetime
from datacube.api import Satellite


def run():

    launch_date = {
        Satellite.LS5: datetime(1984, 3, 1),
        Satellite.LS7: datetime(1999, 4, 15),
        Satellite.LS8: datetime(2013, 2, 11)
    }

    repeat_interval = {
        Satellite.LS5: 16,
        Satellite.LS7: 16,
        Satellite.LS8: 16
    }

    # This is relative to path 120 row 65 which is 110/-7

    australian_overpass_date = {
        Satellite.LS5: datetime(1984, 3, 1),
        Satellite.LS7: datetime(1999, 4, 15),
        Satellite.LS8: datetime(2013, 5, 23)
    }

    calculate_first_overpass_date(australian_overpass_date[Satellite.LS8],
                                  launch_date[Satellite.LS8],
                                  repeat_interval[Satellite.LS8])

    # first_australian_overpass_dates = {
    #     Satellite.LS5: datetime(1984, 3, 1),
    #     Satellite.LS7: datetime(1999, 4, 15),
    #     Satellite.LS8: datetime(2013, 2, 11)
    # }


def calculate_first_overpass_date(overpass_dt, launch_dt, repeat_interval):
    pass


if __name__ == "__main__":
    run()