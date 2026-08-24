from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="cdc_demo.bronze.employees_bronze",
    comment="Bronze layer: Raw employee data ingested from UC Volume using Auto Loader"
)
def employees_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("sep", ",")
        .load("/Volumes/cdc_demo/bronze/employees")
    )