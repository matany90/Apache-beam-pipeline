import firebase_admin
import apache_beam as beam
import uuid

from firebase_admin import firestore
from firebase_admin import credentials


# get credentials
cred = credentials.ApplicationDefault()

# init app
firebase_admin.initialize_app(cred, { "projectId": "rele-saar" })

class WriteToFS(beam.DoFn):
  def __init__(self):
    super().__init__()

  def start_bundle(self):
    self.db = firestore.client()

  def process(self, element):
    col_path = "test_collection"

    # write message
    write_result, doc_ref = self.db.collection(col_path).add(element)

    yield {
      **element,
      "doc_id": doc_ref.id
    }


class UpdateToFS(beam.DoFn):
    '''
    Update the message with the NLP data.
    '''

    def __init__(self):
        '''
        Init the DoFn class
        '''
        super().__init__()

    def start_bundle(self):
        '''
        Init DB client
        '''
        self.db = firestore.client()

    def process(self, element):
        '''
        Runs the update process
        '''
        # get data from elements
        _, values = element
        write_results, message = values
        doc_id = write_results[0]["doc_id"]
        updated_value = message[0]["first_field"]
        doc_path = f"test_collection/{doc_id}"

        self.db.document(doc_path).update({
            "first_field": updated_value
        })

        yield {
          "doc_id": doc_id
        }


class ReadFromFS(beam.DoFn):
    '''
    Update the message with the NLP data.
    '''

    def __init__(self):
        '''
        Init the DoFn class
        '''
        super().__init__()

    def start_bundle(self):
        '''
        Init DB client
        '''
        self.db = firestore.client()

    def process(self, element):
        '''
        Runs the update process
        '''
        # get data from elements
        doc_id = element["doc_id"]

        doc_path = f"test_collection/{doc_id}"
        res = self.db.document(doc_path).get()

        yield res.to_dict()

