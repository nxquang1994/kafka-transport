"""Defines trends calculations for stations"""
import logging
from datetime import timedelta

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("org.chicago.cta.stations.table.v1.stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.v1.stations.transformed", 
                      partitions=1,
                      key_type=str,
                      value_type=TransformedStation)
# TODO: Define a Faust Table
table = app.Table(
   name="transform_station_hopping",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
).hopping(
    size=timedelta(seconds=30),
    step=timedelta(seconds=5),
    expires=timedelta(minutes=50),
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(out_topic)
async def station(stations):
    async for s in stations:
        curr_key = f"{s.station_id}__{s.stop_id}"
        transformed_station = TransformedStation()
        transformed_station.station_id = s.station_id
        transformed_station.station_name = s.station_name
        transformed_station.order = s.order
        if s.red == True:
            transformed_station.line = "red"
        elif s.blue == True:
            transformed_station.line = "blue"
        elif s.green == True:
            transformed_station.line = "green"
        else:
            transformed_station.line = ""
        table[curr_key] = transformed_station



if __name__ == "__main__":
    app.main()
