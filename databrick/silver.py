Silver Layer — Clean, Standardize, and Merge Customers
Turns the raw Bronze tables into a trusted Silver customer table (datamodel.silver.customers, SCD Type 1):

Read both Bronze sources (history + increment) as one union, tagging each row's origin
Clean & standardize: trim/initcap names & city, lowercase emails, parse registration_date (dd-MMM-yyyy) and last_updated_ts into proper types
Data quality checks: rows failing any rule go to datamodel.silver.customers_quarantine with the failure reasons (audit trail)
Deduplicate: keep only the latest version of each customer_id by last_updated_ts
Incremental processing: classify records as new, updated, or unchanged vs the Silver target
MERGE the result into datamodel.silver.customers (upsert on customer_id) and log run statistics to datamodel.silver.load_audit (monitoring)


# Read Bronze sources (history + increment)

from pyspark.sql import functions as F
from pyspark.sql import Window

BRONZE_HISTORY = "datamodel.bronze.customers_history"
BRONZE_INCREMENT = "datamodel.bronze.customers_increment"
SILVER_TARGET = "datamodel.silver.customers"
SILVER_QUARANTINE = "datamodel.silver.customers_quarantine"
SILVER_AUDIT = "datamodel.silver.load_audit"

history_df = spark.table(BRONZE_HISTORY).withColumn("_source", F.lit("history"))
increment_df = spark.table(BRONZE_INCREMENT).withColumn("_source", F.lit("increment"))

raw_df = history_df.unionByName(increment_df)
print(f"Bronze rows loaded: history={history_df.count()}, increment={increment_df.count()}, union={raw_df.count()}")



#Clean and standardize
# Standardize text casing, trim whitespace, and parse date/timestamp strings into proper types
cleaned_df = (
    raw_df
    .select(
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.initcap(F.trim(F.col("first_name"))).alias("first_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.initcap(F.trim(F.col("city"))).alias("city"),
        F.to_date(F.trim(F.col("registration_date")), "dd-MMM-yyyy").alias("registration_date"),
        F.to_timestamp(F.trim(F.col("last_updated_ts"))).alias("last_updated_ts"),
        F.col("_source"),
    )
)
display(cleaned_df.limit(10))


#Data quality checks + quarantine
# Data quality rules — each row must pass ALL of them to reach Silver
DQ_RULES = {
    "customer_id_not_null": F.col("customer_id").isNotNull(),
    "customer_id_format": F.col("customer_id").rlike(r"^C\d+$"),
    "email_valid": F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
    "registration_date_parsed": F.col("registration_date").isNotNull(),
    "last_updated_ts_parsed": F.col("last_updated_ts").isNotNull(),
}

failure_exprs = [F.when(~cond, F.lit(rule)) for rule, cond in DQ_RULES.items()]
checked_df = cleaned_df.withColumn("dq_failures", F.array_compact(F.array(*failure_exprs)))

valid_df = checked_df.filter(F.size("dq_failures") == 0).drop("dq_failures")
quarantine_df = (
    checked_df.filter(F.size("dq_failures") > 0)
    .withColumn("quarantined_ts", F.current_timestamp())
)

n_quarantined = quarantine_df.count()
if n_quarantined > 0:
    quarantine_df.write.mode("append").saveAsTable(SILVER_QUARANTINE)

print(f"Data quality: valid={valid_df.count()}, quarantined={n_quarantined}")
if n_quarantined > 0:
    display(quarantine_df)



# Deduplicate: keep only the latest version of each customer (across history + increment)
deduped_df = (
    valid_df
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.col("last_updated_ts").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_source")
)
print(f"Deduplicated: {deduped_df.count()} distinct customers")


#Incremental detection + MERGE into Silver + audit log
# Ensure the Silver target table exists (first run)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TARGET} (
        customer_id STRING,
        first_name STRING,
        email STRING,
        city STRING,
        registration_date DATE,
        last_updated_ts TIMESTAMP,
        insert_timestamp TIMESTAMP,
        update_timestamp TIMESTAMP
    )
""")

deduped_df.createOrReplaceTempView("src")

# Incremental processing: classify each source record vs the Silver target
match_condition = """
    t.first_name <=> s.first_name
    AND t.email <=> s.email
    AND t.city <=> s.city
    AND t.registration_date <=> s.registration_date
    AND t.last_updated_ts <=> s.last_updated_ts
"""

audit = spark.sql(f"""
    SELECT
        COUNT(CASE WHEN t.customer_id IS NULL THEN 1 END)                            AS new_customers,
        COUNT(CASE WHEN t.customer_id IS NOT NULL AND NOT ({match_condition}) THEN 1 END) AS updated_customers,
        COUNT(CASE WHEN t.customer_id IS NOT NULL AND ({match_condition}) THEN 1 END)    AS unchanged_customers
    FROM src s
    LEFT JOIN {SILVER_TARGET} t ON s.customer_id = t.customer_id
""").collect()[0]
print(f"Incremental classification: new={audit['new_customers']}, updated={audit['updated_customers']}, unchanged={audit['unchanged_customers']}")

# MERGE the historical + incremental data into the trusted Silver table (SCD Type 1 upsert)
spark.sql(f"""
    MERGE INTO {SILVER_TARGET} AS t
    USING src AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED AND NOT ({match_condition}) THEN UPDATE SET
        t.first_name = s.first_name,
        t.email = s.email,
        t.city = s.city,
        t.registration_date = s.registration_date,
        t.last_updated_ts = s.last_updated_ts,
        t.update_timestamp = current_timestamp()
    WHEN NOT MATCHED THEN INSERT
        (customer_id, first_name, email, city, registration_date, last_updated_ts, insert_timestamp)
        VALUES
        (s.customer_id, s.first_name, s.email, s.city, s.registration_date, s.last_updated_ts, current_timestamp())
""")

# Log run statistics for monitoring
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_AUDIT} (
        run_ts TIMESTAMP,
        bronze_rows INT,
        valid_rows INT,
        quarantined_rows INT,
        silver_rows_after_merge INT,
        new_customers INT,
        updated_customers INT,
        unchanged_customers INT
    )
""")
spark.sql(f"""
    INSERT INTO {SILVER_AUDIT}
    SELECT
        current_timestamp(),
        {raw_df.count()},
        {valid_df.count()},
        {n_quarantined},
        (SELECT COUNT(*) FROM {SILVER_TARGET}),
        {audit['new_customers']},
        {audit['updated_customers']},
        {audit['unchanged_customers']}
""")

#Verify Silver table and audit
print(f"{SILVER_TARGET} contents:")
display(spark.table(SILVER_TARGET).orderBy("customer_id"))

print("Latest load audit entry:")
display(spark.table(SILVER_AUDIT).orderBy(F.desc("run_ts")).limit(1))
