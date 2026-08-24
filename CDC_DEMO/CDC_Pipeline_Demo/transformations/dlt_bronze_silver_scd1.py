from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

# Step 1: Create target streaming table for SCD Type 1
dp.create_streaming_table(
    name="cdc_demo.silver.employees_silver_scd1",
    comment="Silver layer: Employee data with SCD Type 1 (latest values only)"
)

# Step 2: Apply CDC flow from bronze to silver
dp.create_auto_cdc_flow(
    target="cdc_demo.silver.employees_silver_scd1",
    source="cdc_demo.bronze.employees_bronze",
    keys=["id"],
    sequence_by="sequence_num",
    stored_as_scd_type=1,
    apply_as_deletes=expr("op = 'DELETE'")
)