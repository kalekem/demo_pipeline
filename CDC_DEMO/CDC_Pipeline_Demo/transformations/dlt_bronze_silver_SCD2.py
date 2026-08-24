from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

# Step 1: Create target streaming table for SCD Type 2
dp.create_streaming_table(
    name="cdc_demo.silver.employees_silver_scd2",
    comment="Silver layer: Employee data with SCD Type 2 (full history tracking with __START_AT and __END_AT)"
)

# Step 2: Apply CDC flow from bronze to silver with SCD Type 2
dp.create_auto_cdc_flow(
    target="cdc_demo.silver.employees_silver_scd2",
    source="cdc_demo.bronze.employees_bronze",
    keys=["id"],
    sequence_by="sequence_num",
    stored_as_scd_type=2,  # Track full history
    apply_as_deletes=expr("op = 'DELETE'")
)