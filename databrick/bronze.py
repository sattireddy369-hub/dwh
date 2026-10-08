Bronze Layer — Customer Source Ingestion
Ingests the two raw customer CSVs from the source volume into the Bronze layer as-is (no business transformations):
customer_historical.csv → datamodel.bronze.customers_history
customer_incremental.csv → datamodel.bronze.customers_increment
Ingestion uses Auto Loader (cloudFiles) with a one-shot availableNow trigger, 
so each run picks up only new files since the last run (incremental file discovery, 
idempotent re-runs). All source columns land as strings, plus _rescued_data for malformed records
and _ingest_ts for lineage. The two sources are separated with pathGlobFilter.

Note: on this serverless compute CREATE STREAMING TABLE is not supported — PySpark Auto Loader 
with trigger(availableNow=True) is the working pattern. Checkpoints must live in a UC volume, 
here under /Volumes/datamodel/source/vol/checkpoints/.



# Source, target, and checkpoint configuration
INPUT_PATH = "/Volumes/datamodel/source/vol/input/"
CHECKPOINT_ROOT = "/Volumes/datamodel/source/vol/checkpoints"

BRONZE_SOURCES = {
    "customers_history": {
        "table": "datamodel.bronze.customers_history",
        "glob": "customer_historical*",
    },
    "customers_increment": {
        "table": "datamodel.bronze.customers_increment",
        "glob": "customer_incremental*",
    },
}

print("Input path:", INPUT_PATH)




#Ingest raw CSVs with Auto Loader
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

# Raw landing schema: every source column as STRING — no parsing or transformation in Bronze
schema = (
    StructType()
    .add("customer_id", StringType())
    .add("first_name", StringType())
    .add("email", StringType())
    .add("city", StringType())
    .add("registration_date", StringType())
    .add("last_updated_ts", StringType())
)

for name, cfg in BRONZE_SOURCES.items():
    print(f"Ingesting '{cfg['glob']}' -> {cfg['table']}")
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_ROOT}/{name}/schema")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        # Pick up only the files belonging to this source
        .option("pathGlobFilter", cfg["glob"])
        .schema(schema)
        .load(INPUT_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{name}/_ckpt")
        .trigger(availableNow=True)
        .toTable(cfg["table"])
    )
    stream.awaitTermination()
    print(f"  done: {cfg['table']}")




#Verify Bronze tables:
for name, cfg in BRONZE_SOURCES.items():
    df = spark.table(cfg["table"])
    rescued = df.filter(F.col("_rescued_data").isNotNull()).count()
    print(f"{cfg['table']}: {df.count()} rows, {rescued} rescued (malformed) records")
    display(df.limit(5))
