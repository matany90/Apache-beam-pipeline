import apache_beam as beam
from uuid import uuid4

class SplitWords(beam.DoFn):
  def __init__(self, split_by = ","):
    super().__init__()
    self.split_by = split_by

  def process(self, text):
    for word in text.split(self.split_by):
      yield {
        "first_field": word,
        "uniqe_id": uuid4().hex
      }
