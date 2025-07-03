import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import sys, argparse
import utils
from itertools import combinations

HIRE_COUNT_NAME = 'hire_count'
STATION_DISTANCE_NAME = 'station_distance'

def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_location', required=True, help='GCS folder to write output')
    args, _ = parser.parse_known_args()
    OUTPUT_FOLDER = args.output_location
    
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as pipeline:
        stations = (
            pipeline 
            | "LoadCycleStations" >> beam.io.ReadFromBigQuery(
                query="""
                    SELECT id, longitude, latitude 
                    FROM `bigquery-public-data.london_bicycles.cycle_stations`
                    LIMIT 100
                """,
                use_standard_sql=True
            )
            | "CreateStationId" >> beam.Map(lambda row: (row['id'], row))
        )
        
        station_distance_permutation = (
            stations
            | 'GatherToSingleList' >> beam.combiners.ToList()
            | "CreateStationCombination" >> beam.FlatMap(
                lambda ids_list: combinations(ids_list, 2)
            )
            | "CalculateStationPermutationDistance" >> beam.ParDo(utils.ComputeStationDistanceFn())
        )
        
        hire_count = (
            pipeline 
            | "LoadCycleHire" >> beam.io.ReadFromBigQuery(
                query="""
                    SELECT rental_id, start_station_id, end_station_id 
                    FROM `bigquery-public-data.london_bicycles.cycle_hire`
                    LIMIT 10000
                """,
                use_standard_sql=True
            )
            | "FilterHireNullId" >> beam.Filter(lambda row: row['start_station_id'] is not None and row['end_station_id'] is not None)
            | "CreateHireCountId" >> beam.Map(lambda row: ((row['start_station_id'], row['end_station_id']), row))
            | "CountCycleHire" >> beam.combiners.Count.PerKey()
        )
        
        easy_test_result = (
            hire_count
            | 'Top100Easy' >> beam.transforms.combiners.Top.Of(100, key=lambda x: x[1])
            | 'FlattenEasyTop100' >> beam.FlatMap(lambda elements: elements)
            | 'FormatEasyResult' >> beam.Map(lambda row: f"{row[0][0]},{row[0][1]},{row[1]}")
            | 'WriteEasyTestOutput' >> beam.io.WriteToText(
                OUTPUT_FOLDER+'easy_test',
                file_name_suffix=".txt",
                num_shards=1
            )
        )

        hard_test_result = (
            {
                HIRE_COUNT_NAME: hire_count, 
                STATION_DISTANCE_NAME:station_distance_permutation
            }
            | "HireCountJoinStationDistance" >> beam.CoGroupByKey()
            | "CalculateTotalDistanceBetweenStations" >> beam.ParDo(
                utils.ComputeTotalDistanceBetweenStationsFn(
                    HIRE_COUNT_NAME, STATION_DISTANCE_NAME
                    )
            )
            | 'Top100Hard' >> beam.transforms.combiners.Top.Of(100, key=lambda x: x[3])
            | 'FlattenHardTop100' >> beam.FlatMap(lambda elements: elements)
            | 'FormatHardResult' >> beam.Map(lambda row: f"{row[0]},{row[1]},{row[2]},{row[3]}")
            | 'WriteHardTestOutput' >> beam.io.WriteToText(
                OUTPUT_FOLDER+'hard_test',
                file_name_suffix=".txt",
                num_shards=1
            )
                
        )

if __name__ == '__main__':
    main(sys.argv)