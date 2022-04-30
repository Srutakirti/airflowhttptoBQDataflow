
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam


#(gcpairflowvenv) C:\DataScience\GCPairflow\gcpairflowvenv\Scripts>

##python -m beamtemplate --region us-central1 --runner DataflowRunner --project proven-mystery-337004 --temp_location gs://srutakirti_dataflow/tmp --staging_location gs://srutakirti_dataflow/tmp  --template_location gs://srutakirti_dataflow/templates/third_template


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments
        parser.add_value_provider_argument(
            '--input_file', 
            help='Path of the file to read from')
        parser.add_value_provider_argument(
            '--date',
            help='Date to add as an attribute')



pipeline_options = PipelineOptions()

args = pipeline_options.view_as(MyOptions)

def add_datetime(elem,val):
    return elem.split(" ") + [val.get()]

def create_dict(elem):
    d={}
    d["domain_code"]=elem[0]
    d["page_title"]=elem[1]
    d["view_count"]=elem[2]
    d["response_size"]=elem[3]
    d["day_date"]=elem[4]
    return d

table_spec="third-index-347203:airflowlearns.wikipageviews"

def run():
    with beam.Pipeline(options=pipeline_options) as p:
        output = (
            p
            | "read_from_file" >> beam.io.ReadFromText(args.input_file)
            | "add datetime" >> beam.Map(add_datetime,args.date)
            | "dict for BQ" >> beam.Map(create_dict)
            | "writing to BQ" >> beam.io.WriteToBigQuery(table_spec,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                                                        custom_gcs_temp_location="gs://srutakirti_dataflow/tmp/beam_tmp_BQ")
            
        )

if __name__ == "__main__":
    run()

#gs://srutakirtisparktests/sample_csv/sample.csv
#gs://srutakirtisparktests/sample_csv/sample_test.csv


