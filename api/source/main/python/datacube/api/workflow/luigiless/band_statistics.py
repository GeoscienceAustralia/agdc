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
from datacube.api import Satellite, PqaMask, DatasetType

__author__ = "Simon Oldfield"


import click
import logging


_log = logging.getLogger()


@click.group()
def cli():
    pass


@cli.command()
@click.option("--x", required=True, nargs=2, type=(click.IntRange(110, 155), click.IntRange(110, 155)), show_default=True, metavar="min max", help="110 - 155")
@click.option("--y", required=True, nargs=2, type=(click.IntRange(-45, -10), click.IntRange(-45, -10)), show_default=True, metavar="min max", help="-45 - -10")
@click.option("--satellite", multiple=True, type=click.Choice([s.name for s in Satellite]), default=[Satellite.LS5.name, Satellite.LS7.name], show_default=True)
@click.option("--acq", nargs=2, type=(str, str), default=("1985", "2014"), show_default=True, metavar="min max", help="YYYY[[-MM]-DD]")
@click.option("--epoch", nargs=2, type=(int, int), default=(5, 6), show_default=True, metavar="increment duration")
@click.option("--dataset-type", type=click.Choice([s.name for s in DatasetType]), default=DatasetType.ARG25.name, show_default=True)
@click.option("--output-directory", required=True, type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True, readable=True, resolve_path=True), show_default=True)
@click.option("--mask-pqa-apply/--no-mask-pqa-apply", default=True, show_default=True, help="Apply PQA mask")
@click.option("--mask-pqa-mask", multiple=True, type=click.Choice([s.name for s in PqaMask]), default=[PqaMask.PQ_MASK_SATURATION.name, PqaMask.PQ_MASK_CONTIGUITY.name, PqaMask.PQ_MASK_CLOUD.name], show_default=True)
def submit_jobs(x, y, satellite, acq, epoch, dataset_type, output_directory, mask_pqa_apply, mask_pqa_mask):
    _log.info("""
    x = {x_min:03d} to {x_max:03d},
    y = {y_min:04d} to {y_max:04d}
    satellite = {satellite}
    acq = {acq_min} to {acq_max}
    dataset = {dataset}
    epoch = {epoch_increment} (increment) / {epoch_duration} (duration)
    output directory = {output_directory}
    PQA mask = {pqa}
    """.format(x_min=x[0], x_max=x[1], y_min=y[0], y_max=y[1], satellite=" ".join(satellite), acq_min=acq[0], acq_max=acq[1],
               epoch_increment=epoch[0], epoch_duration=epoch[1], dataset=dataset_type,
               output_directory=output_directory, pqa=mask_pqa_apply and " ".join(mask_pqa_mask) or ""))
    # click.echo("Submit jobs x=%d to %d y = %d to %d output" % x, y)


@cli.command()
def generate_statistics_chunk():
    click.echo("Doing chunk")


@cli.command()
def generate_statistics_output():
    click.echo("Doing chunk")


# cli.add_command(submit_jobs)
# cli.add_command(generate_statistics_chunk)
# cli.add_command(generate_statistics_output)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
    cli()