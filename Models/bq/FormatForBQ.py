import apache_beam as beam


class FormatForBQ(beam.DoFn):
  def __init__(self):
    super().__init__()

  def process(self, element):
    yield {
      "first_field": element["first_field"]
    }
