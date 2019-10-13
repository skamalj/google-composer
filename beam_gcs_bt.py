import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions 

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--avro_input")
        parser.add_argument("--json_input")

class ConvertToJson(beam.DoFn):
    def process(self, element):
        import json
        return json.loads("[" + element + "]")

class CreateHbaseRow(beam.DoFn): 
  import logging
  
  def __init__(self, project_id, instance_id, table_id):
      self.project_id = project_id
      self.instance_id = instance_id
      self.table_id = table_id

  def start_bundle(self):
    from google.cloud import bigtable
    from google.cloud.bigtable import  column_family
    try:
        self.client = bigtable.Client(project=self.project_id, admin=True) 
        self.instance = self.client.instance(self.instance_id) 
        self.table = self.instance.table(self.table_id)
        max_versions_rule = column_family.MaxVersionsGCRule(2)
        column_family_id = 'cf1'
        column_families = {column_family_id: max_versions_rule}
        if not self.table.exists():
            self.table.create(column_families=column_families)
        else:
            logging.info("Table {} already exists.".format(self.table_id))
    except:
        logging.error("Failed to start bundle")
        raise       

  def process(self, element):
    from datetime import datetime  
    try:
        table = self.table
        rows = []  
        row = table.row(row_key=str(element['customerNumber']).encode())  
        for colname, val in element.items():
            if val: 
                if isinstance(val, unicode):
                    logging.info("Creating row for: ", colname, val)
                    valenc = val.encode("utf-8")
                else:
                    valenc = bytes(val)    
                row.set_cell('cf1',colname.encode("utf-8"),valenc, datetime.now())
        rows.append(row)    
        table.mutate_rows(rows)   
    except:   
        logging.error("Failed with input: ", str(element))
        raise
        
options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'yourprojectid'
google_cloud_options.staging_location = 'gs://beamgcdeveloper/staging'
google_cloud_options.temp_location = 'gs://beamgcdeveloper/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

avro_input = options.view_as(MyOptions).avro_input
json_input = options.view_as(MyOptions).json_input

p = beam.Pipeline(options=options)

lines_avro  = p | "ReadAvroFromGCS" >> beam.io.avroio.ReadFromAvro(avro_input)
lines_text  = p | "ReadJsonFromGCS" >> beam.io.ReadFromText(json_input)
lines_avro | "CreateHbaseRowsFromAvro" >> beam.ParDo(CreateHbaseRow(google_cloud_options.project,
                                                                'mybigtable','customer'))
lines_json = lines_text | "ConvertToJson" >> beam.ParDo(ConvertToJson())  
lines_json | "CreateHbaseRowsFromJson" >> beam.ParDo(CreateHbaseRow(google_cloud_options.project,
                                                                'mybigtable','customerfromjson'))   
p.run()