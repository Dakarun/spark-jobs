from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

BASE_JARS = [
    "org.apache.hadoop:hadoop-aws:3.2.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.290"
]

BASE_CONFIG = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    "spark.sql.adaptive.enabled": "true",
    "hive.metastore.uris": "thrift://localhost:9083",
    "spark.sql.hive.metastore.version": "3.1.0",
    "spark.sql.hive.metastore.jars": "path",  # If you have it try to download it from maven/run hive 3.1.0's jars it'll fail since it's not compatible with java 11. Instead path will look at an empty value
}


def get_spark_session(config: dict = {}):
    if "jars" in config.keys():
        jars = config["jars"] + BASE_JARS
    else:
        jars = BASE_JARS

    if "config" in config.keys():
        spark_config = {
            **BASE_CONFIG,
            **config["config"]
        }
    else:
        spark_config = BASE_CONFIG

    conf = SparkConf()
    conf.setAll([(k, v) for k, v in spark_config.items()])

    spark = SparkSession \
        .builder \
        .appName(config.get("name", "DefaultSparkAppName")) \
        .config(conf=conf) \
        .config("spark.jars.packages", ",".join(jars)) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark
