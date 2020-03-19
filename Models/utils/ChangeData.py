import apache_beam as beam


class ChangeData(beam.DoFn):
  def __init__(self, data):
    self.data = data
    super().__init__()

  def process(self, element):
    yield {
      **element,
      "first_field": self.data
    }
