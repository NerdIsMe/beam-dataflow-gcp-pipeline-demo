import apache_beam as beam
from haversine import haversine

class ComputeStationDistanceFn(beam.DoFn):
    def process(self, element):
        station1_kv, station2_kv = element
        station1 = station1_kv[1]
        station2 = station2_kv[1]
        
        dist = haversine(
                (station1['longitude'], station1['latitude']),
                (station2['longitude'], station2['latitude'])
            )

        yield ((station1['id'], station2['id']), dist)
        yield ((station2['id'], station1['id']), dist)

class ComputeTotalDistanceBetweenStationsFn(beam.DoFn):
    def __init__(self, hire_count_name, station_distance_name):
        self.hire_count_name = hire_count_name
        self.station_distance_name = station_distance_name
        
    def process(self, element):
        key, values = element
        hire_counts = values.get(self.hire_count_name, [])
        distance = values.get(self.station_distance_name, [])
        
        if not hire_counts or not distance:
            return
        
        hire_count = hire_counts[0]          
        station_distance = distance[0]      
        total_distance_between_stations = hire_count * station_distance
        
        yield (*key, hire_count, total_distance_between_stations)
   