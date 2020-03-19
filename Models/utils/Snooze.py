import apache_beam as beam
import time

class Snooze(beam.DoFn):
  def __init__(self):
    super().__init__()

  def process(self, element):
    time.sleep(10)
    yield element
