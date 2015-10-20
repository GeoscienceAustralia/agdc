#!/usr/bin/env python

# ===============================================================================
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
# ===============================================================================


__author__ = "Simon Oldfield"


import abc
import datacube.api.workflow as workflow
import logging


_log = logging.getLogger()


class Workflow(workflow.Workflow):

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        workflow.Workflow.__init__(self, name)

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        workflow.Workflow.setup_arguments(self)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        workflow.Workflow.process_arguments(self, args)

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        workflow.Workflow.log_arguments(self)

    def create_summary_tasks(self):

        raise Exception("Abstract method should be overridden")


class SummaryTask(workflow.SummaryTask):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def create_cell_tasks(self, x, y):

        raise Exception("Abstract method should be overridden")


class CellTask(workflow.CellTask):

    __metaclass__ = abc.ABCMeta

