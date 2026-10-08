Gold Layer — Customer Dimensional Model
Applies the business logic on top of the trusted Silver customer table and builds the dimensional model for reporting and analytics:

datamodel.gold.dim_customer — customer dimension with a surrogate key (customer_key, MD5 of customer_id), standardized attributes, and derived business attributes (email domain, registration year/month, customer tenure in days)
datamodel.gold.fact_customer_registration — one row per customer registration event, joined to the dimension via customer_key, with a smart registration_date_key (yyyyMMdd int) for date-based analysis
Both tables are fully rebuilt each run (overwrite — appropriate for small dimensions) and carry table comments so they are self-describing in Unity Catalog.

# Read Silver customers
from pyspark.sql import functions as F

SILVER_CUSTOMERS = "datamodel.silver.customers"
DIM_CUSTOMER = "datamodel.gold.dim_customer"
FACT_REGISTRATION = "datamodel.gold.fact_customer_registration"

silver_df = spark.table(SILVER_CUSTOMERS)
print(f"Silver customers loaded: {silver_df.count()}")

#Build dim_customer (with business logic)
# Business logic: surrogate key, email domain, registration year, customer tenure
dim_df = (
    silver_df
    .withColumn("customer_key", F.md5("customer_id"))
    .withColumn("email_domain", F.split("email", "@").getItem(1))
    .withColumn("registration_year", F.year("registration_date"))
    .withColumn("tenure_days", F.datediff(F.current_date(), F.col("registration_date")))
    .select(
        "customer_key",
        "customer_id",
        "first_name",
        "email",
        "email_domain",
        "city",
        "registration_date",
        "registration_year",
        "tenure_days",
        "last_updated_ts",
        "insert_timestamp",
        "update_timestamp",
    )
)

(dim_df.write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(DIM_CUSTOMER))

spark.sql(f"""COMMENT ON TABLE {DIM_CUSTOMER} IS
    'Customer dimension built from datamodel.silver.customers (SCD1). Surrogate key customer_key = MD5(customer_id). Rebuilt on each pipeline run.'""")
print(f"{DIM_CUSTOMER}: {dim_df.count()} rows written")
display(dim_df.orderBy("customer_id"))

#Build fact_customer_registration
# Fact table: one registration event per customer, keyed to the dimension
fact_df = (
    silver_df
    .join(dim_df.select("customer_id", "customer_key"), "customer_id", "left")
    .select(
        "customer_key",
        F.date_format("registration_date", "yyyyMMdd").cast("int").alias("registration_date_key"),
        F.year("registration_date").alias("registration_year"),
        F.month("registration_date").alias("registration_month"),
        F.lit(1).alias("registration_count"),
    )
)

(fact_df.write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(FACT_REGISTRATION))

spark.sql(f"""COMMENT ON TABLE {FACT_REGISTRATION} IS
    'Customer registration fact: one row per customer, grain = customer. Joins to dim_customer on customer_key. Rebuilt on each pipeline run.'""")
print(f"{FACT_REGISTRATION}: {fact_df.count()} rows written")
display(fact_df.orderBy("registration_date_key"))

#Verify Gold model integrity
# Verification: dimension/fact join integrity and a sample analytics query
check = spark.sql(f"""
    SELECT
        d.customer_id,
        d.first_name,
        d.email_domain,
        d.city,
        d.registration_year,
        d.tenure_days,
        f.registration_date_key
    FROM {DIM_CUSTOMER} d
    JOIN {FACT_REGISTRATION} f ON d.customer_key = f.customer_key
    ORDER BY d.customer_id
""")
display(check)

orphan_facts = spark.sql(f"""
    SELECT COUNT(*) AS orphan_facts
    FROM {FACT_REGISTRATION} f
    LEFT JOIN {DIM_CUSTOMER} d ON d.customer_key = f.customer_key
    WHERE d.customer_key IS NULL
""").collect()[0]["orphan_facts"]
print(f"Referential integrity: {orphan_facts} orphan fact rows (expected 0)")

# Sample analytics: registrations per year
spark.sql(f"""
    SELECT registration_year, SUM(registration_count) AS registrations
    FROM {FACT_REGISTRATION}
    GROUP BY registration_year
    ORDER BY registration_year
""").show()





