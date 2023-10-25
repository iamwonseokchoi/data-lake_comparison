# %%
import os
import toml
import logging
from pyspark.sql import SparkSession

def create_spark_session(
    app: str = "PysparkWithDeltaIceberg", 
    warehouse_location: str = "spark-warehouse",
    deltadb: str = "deltadb",
    icebergdb: str = "icebergdb"
    ) -> SparkSession:
    """
    Creates a spark session using local client with delta lake & iceberg for Pyspark 3.4.1.
    :param app: Name of the spark application (default: PysparkWithDeltaIceberg)
    :param warehouse_location: Location of the spark warehouse (default: spark-warehouse)
    :param deltadb: Name of the delta database (default: deltadb)
    :param icebergdb: Name of the iceberg database (default: icebergdb)
    :return: SparkSession
    """
    # Set environment variables
    config = toml.load("../config.toml")
    java_home = config["JAVA_HOME"]["JAVA_HOME"]
    spark_home = config["SPARK_HOME"]["SPARK_HOME"]
    os.environ["JAVA_HOME"] = java_home
    os.environ["SPARK_HOME"] = spark_home
    os.environ["PATH"] = f'{os.environ["JAVA_HOME"]}/bin:{os.environ["PATH"]}'
    os.environ["PATH"] = f'{os.environ["PATH"]}:{os.environ["SPARK_HOME"]}/bin'
    
    # Create spark session
    spark_jars = "io.delta:delta-core_2.12:2.4.0,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0"
    spark_extensions = "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app)
        .config("spark.jars.packages", spark_jars)
        .config("spark.sql.extensions", spark_extensions)
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config(f"spark.sql.catalog.{icebergdb}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{icebergdb}.type", "hadoop")
        .config(f"spark.sql.catalog.{icebergdb}.warehouse", warehouse_location + "/" + icebergdb)
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
        .getOrCreate()
    )
    
    # Loggers Setup
    spark.sparkContext.setLogLevel("ERROR")
    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.warn(f"Pyspark script logger initialized from {__name__}")
    
    # Create delta database
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{deltadb}")
    except Exception as e:
        logging.error=f"Error while creating delta database `{deltadb}`:", str(e)
    
    return spark