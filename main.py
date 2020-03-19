import argparse
import json
import time
import apache_beam as beam

from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

from Models.utils.ChangeData import ChangeData
from Models.utils.Snooze import Snooze
from Models.utils.SplitWords import SplitWords
from Models.firestore.Firestore import WriteToFS, UpdateToFS, ReadFromFS
from Models.bq.FormatForBQ import FormatForBQ


def get_pipeline_options(pipeline_args):
    '''
    Generates the pipeline options object
    '''
    # Define the pipeline options
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    # Returns the option object
    return options


def _parse_user_args(argv):
  '''
  Parse the user known and unknown arguments from CLI
  '''
  # Define parser
  parser = argparse.ArgumentParser()

  # Define external arguments
  parser.add_argument("--topic", required=True, help="PubSub topics")
  parser.add_argument("--schema-path", default="/etc/relesaar/keys/messaging.json", help="Schema")

  # Parse arguments
  return parser.parse_known_args(argv)

def run(argv = None):
  known_args, pipeline_args = _parse_user_args(argv)

  options = get_pipeline_options(pipeline_args)

  # Load schema
  schema = '{"fields": ' + open(known_args.schema_path, "r").read() + '}'

  schema = parse_table_schema_from_json(schema)


  with beam.Pipeline(options=options) as p:
    # Get message from pubsub and split it by identifier
    formated_messages = (
      p
      | "Read from PubSub" >> beam.io.ReadFromPubSub(known_args.topic)
      | "Windowing" >> beam.WindowInto(window.FixedWindows(30))
      | "Decoder" >> beam.Map(lambda e: e.decode())
      | "Split into List" >> beam.ParDo(SplitWords(","))
    )

    # Pipeline split:
      # 1. Write to FS
      # 2. Snooze for 10 sec, and change data locally

    # Write to FS
    writer_messages = (
      formated_messages
      | "Write to FS" >> beam.ParDo(WriteToFS())
      | "Get FS keys" >> beam.Map(lambda val: (val["uniqe_id"], val))
    )

    # Snooze for 10 sec, and change data locally
    do_something_that_takes_time = (
      formated_messages
      | "Snooze For 10 Seconds" >> beam.ParDo(Snooze())
      | "Add Data" >> beam.ParDo(ChangeData("changed!"))
      | "Get Update keys" >> beam.Map(lambda val: (val["uniqe_id"], val))
    )

    # Pipeline group by id and update data in FS after changed locally
    results = (
      (writer_messages, do_something_that_takes_time)
      | "Group by key" >> beam.CoGroupByKey()
      | "Update FS" >> beam.ParDo(UpdateToFS())
    )

    # Write updated data to Big Query
    (
      results
      | "Read Document From FS" >> beam.ParDo(ReadFromFS())
      | "Format For BQ" >> beam.ParDo(FormatForBQ())
      | "Write to BigQuery" >> beam.io.WriteToBigQuery(
        "saar.messaging",
        schema=schema
      )
    )

if __name__ == "__main__":
    run()