import sys

import pyspark.sql.functions as f

from pyspark.sql.types import StructType, StructField, StringType, LongType

from spark_jobs.util.spark import get_spark_session
from spark_jobs.jobs.weather.util import download_file

BASE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/{}.csv.gz"
DOWNLOAD_PATH = "/tmp/annual_weather_{}.csv.gz"

JOB_CONFIG = {
    "name": "IngestNOAAAnnualFile",
    "config": {
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    },
    "jars": [],
}


def string_to_bool(value: str) -> bool:
    if isinstance(value, str):
        if value.lower() == 'true':
            return True
        elif value.lower() == "false":
            return False
        else:
            raise Exception(f"Value must be either True/true or False/false. Got {value}")
    else:
        raise Exception("Passed in value was not a string")


if __name__ == "__main__":
    job_year = sys.argv[1]
    file_path = DOWNLOAD_PATH.format(job_year)

    try:
        get_file = string_to_bool(sys.argv[2])
    except IndexError as e:
        get_file = True

    if get_file:
        print("Downloading file")
        # Download file
        download_url = BASE_URL.format(job_year)
        download_file(download_url, file_path, 128**16)

    spark = get_spark_session(JOB_CONFIG)
    noaa_schema = StructType([
        StructField("station_id", StringType(), False),
        StructField("date", StringType(), False),
        StructField("element_type", StringType(), False),
        StructField("value", LongType(), False),
        StructField("measurement_flag", StringType(), True),
        StructField("quality_flag", StringType(), True),
        StructField("source_flag", StringType(), True),
        StructField("observation_time", StringType(), True)
    ])

    print("Reading data")
    print(spark.conf.get("spark.hadoop.fs.s3a.aws.credentials.provider"))
    ddf = spark.sql("CREATE DATABASE IF NOT EXISTS weather LOCATION 's3a://databrewing-structured-data/weather/'")
    ddf.show()
    print(ddf.count())
    df = spark.read.schema(noaa_schema).csv(file_path)
    df.withColumn("year", f.lit(job_year)) \
        .write \
        .partitionBy("year") \
        .mode("overwrite") \
        .saveAsTable("weather.yearly_station_raw")
    print("Completed write")
    import time
    time.sleep(120)
