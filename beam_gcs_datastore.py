import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions 

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--json_input")

class ConvertToJson(beam.DoFn):
    def process(self, element):
        import json
        return json.loads("[" + element + "]")

class CreateHbaseRow(beam.DoFn): 
  import logging
  
  def __init__(self, project_id):
      self.project_id = project_id


  def start_bundle(self):
      from google.cloud import datastore
      self.client = datastore.Client()
      

  def process(self, element):
    from google.cloud import datastore 
    try:
        key = self.client.key('customer', element['customerNumber'])
        entity = datastore.Entity(key=key)
        entity.update(element)  
        self.client.put(entity) 
    except:   
        logging.error("Failed with input: ", str(element))
        raise
        
options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.staging_location = 'gs://beamgcdeveloper/staging'
google_cloud_options.temp_location = 'gs://beamgcdeveloper/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

json_input = options.view_as(MyOptions).json_input
project_id =  google_cloud_options.project

p = beam.Pipeline(options=options)

lines_text  = p | "ReadJsonFromGCS" >> beam.io.ReadFromText(json_input)
lines_json = lines_text | "ConvertToJson" >> beam.ParDo(ConvertToJson())  
lines_json | "CreateHbaseRowsFromJson" >> beam.ParDo(CreateHbaseRow(project_id))   
p.run()