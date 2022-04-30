from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import argparse

parser=argparse.ArgumentParser()

parser.add_argument(
    '--input_file',
    help='The file path for the input text to process.')

parser.add_argument(
    '--output_file',
    help='The file path for output to be dumped.')

known_args,pipeline_args=parser.parse_known_args()

beam_options = PipelineOptions(
    pipeline_args)

pipeline_options = PipelineOptions(pipeline_args)

def run():
    with beam.Pipeline(options=pipeline_options) as p:
        output = (
            p
            | "read_from_file" >> beam.io.ReadFromText(known_args.input_file)
            | "write to file" >> beam.io.WriteToText(known_args.output_file,file_name_suffix="tsv")
            
        )

if __name__ == "__main__":
    run()

#gs://srutakirtisparktests/sample_csv/sample.csv
#gs://srutakirtisparktests/sample_csv/sample_test.csv
