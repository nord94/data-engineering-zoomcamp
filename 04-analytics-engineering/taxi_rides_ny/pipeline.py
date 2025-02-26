import dlt
import requests
import gzip
import csv
from typing import Iterator, Dict


@dlt.resource
def fhv_trips_single() -> Iterator[Dict[str, str]]:
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz"
    print(f"Downloading data from {url}...")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    with gzip.open(resp.raw, 'rt') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

@dlt.source
def fhv_source_single():
    return fhv_trips_single()

pipeline = dlt.pipeline(
    pipeline_name="fhv_nyc_data",
    destination="postgres",
    dataset_name="fhv_nyc_tlc"
)

pipeline.run(fhv_source_single())
