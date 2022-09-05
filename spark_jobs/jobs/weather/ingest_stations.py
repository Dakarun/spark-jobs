STATION_URL = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
DOWNLOAD_PATH = "/tmp/ghncd-stations.txt"

JOB_CONFIG = {
    "name": "IngestNOAAStationFile",
    "config": {},
    "jars": [],
}


if __name__ == "__main__":
