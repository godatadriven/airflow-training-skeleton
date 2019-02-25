from __future__ import absolute_import

import argparse
import logging
import sys

import apache_beam as beam
from apache_beam.examples.cookbook.coders import JsonCoder
from apache_beam.io import ReadFromText, BigQueryDisposition
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        help="Input file to process.",
    )
    parser.add_argument(
        "--table",
        dest="table",
        help="Destination BigQuery table",
    )
    parser.add_argument(
        "--dataset",
        dest="dataset",
        help="Destination BigQuery dataset",
    )
    parser.add_argument(
        "--project",
        dest="project",
        help="project_id",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:
        (
                p
                | "ReadFromGCS" >> ReadFromText(known_args.input, coder=JsonCoder())
                | WriteToBigQuery(
            known_args.table,
            dataset=known_args.dataset,
            project=known_args.project,
            schema="city:string, "
                   "county:string, "
                   "district:string, "
                   "duration:string, "
                   "locality:string, "
                   "newly_built:boolean, "
                   "paon:string, "
                   "postcode:string, "
                   "ppd_category_type:string, "
                   "price:numeric, "
                   "property_type:string, "
                   "record_status:string, "
                   "saon:string, "
                   "street:string, "
                   "transaction:string, "
                   "transfer_date:numeric",
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run(argv=sys.argv)
