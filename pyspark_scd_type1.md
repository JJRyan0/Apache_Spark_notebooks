# PySpark - Slowly Changing Dimension (SCD) Type 1


Existing Data: Contains customer information such as customer_id, name, address, and last_updated.

New Data: Contains updates to the customer information, with changes in name, address, or new customers.

Task: Implement Slowly Changing Dimension (SCD) Type 1 in PySpark. Update the existing records with new information when applicable. If no changes are present, keep the existing data.

```python

existing_data = [
 (1, "John Doe", "123 Elm St", "2023-01-01"),
 (2, "Jane Smith", "456 Oak St", "2023-02-01"),
 (3, "Jim Brown", "789 Pine St", "2023-03-01")
]
new_data = [
 (1, "John Doe", "123 Elm St", "2023-04-01"), # No change
 (2, "Jane Doe", "456 Oak St", "2023-04-01"), # Name changed
 (4, "Lucy Green", "101 Maple St", "2023-04-01") # New customer
]
```


```python

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("SCD Type 1").getOrCreate()

# Sample data
existing_data = [
    (1, "John Doe", "123 Elm St", "2023-01-01"),
    (2, "Jane Smith", "456 Oak St", "2023-02-01"),
    (3, "Jim Brown", "789 Pine St", "2023-03-01")
]

new_data = [
    (1, "John Doe", "123 Elm St", "2023-04-01"),   # No change
    (2, "Jane Doe", "456 Oak St", "2023-04-01"),   # Name changed
    (4, "Lucy Green", "101 Maple St", "2023-04-01")# New customer
]

# Define schemas
columns = ["customer_id", "name", "address", "last_updated"]

# Create DataFrames
df_existing = spark.createDataFrame(existing_data, columns)
df_new = spark.createDataFrame(new_data, columns)

# 1. Join existing and new on customer_id
joined_df = df_existing.alias("e").join(
    df_new.alias("n"), on="customer_id", how="outer"
)

# 2. Determine updates or inserts
scd1_df = joined_df.select(
    col("customer_id"),
    # Use COALESCE to get latest values (from new if changed or new row)
    col("n.name").alias("new_name"),
    col("n.address").alias("new_address"),
    col("n.last_updated").alias("new_last_updated"),
    col("e.name").alias("old_name"),
    col("e.address").alias("old_address"),
    col("e.last_updated").alias("old_last_updated")
).withColumn(
    "final_name", 
    col("new_name")
).withColumn(
    "final_address", 
    col("new_address")
).withColumn(
    "final_last_updated", 
    col("new_last_updated")
)

# 3. Filter for changed or new records
updated_df = scd1_df.filter(
    (col("old_name").isNull()) | 
    (col("new_name") != col("old_name")) | 
    (col("new_address") != col("old_address"))
).select(
    col("customer_id"),
    col("final_name").alias("name"),
    col("final_address").alias("address"),
    col("final_last_updated").alias("last_updated")
)

# 4. Get unchanged records
unchanged_df = df_existing.join(updated_df, on="customer_id", how="left_anti")

# 5. Union updated + unchanged to get final SCD Type 1 output
final_df = updated_df.union(unchanged_df)

final_df.show(truncate=False)

```

# SCD Type 1 in Pandas (Update existing records, insert new, ignore unchanged)

```python

import pandas as pd

# Existing data
existing_data = [
    (1, "John Doe", "123 Elm St", "2023-01-01"),
    (2, "Jane Smith", "456 Oak St", "2023-02-01"),
    (3, "Jim Brown", "789 Pine St", "2023-03-01")
]

# New data
new_data = [
    (1, "John Doe", "123 Elm St", "2023-04-01"),   # No change
    (2, "Jane Doe", "456 Oak St", "2023-04-01"),   # Name changed
    (4, "Lucy Green", "101 Maple St", "2023-04-01")# New customer
]

columns = ["customer_id", "name", "address", "last_updated"]

# Create DataFrames
df_existing = pd.DataFrame(existing_data, columns=columns)
df_new = pd.DataFrame(new_data, columns=columns)

# Merge on customer_id to detect updates
merged = pd.merge(df_existing, df_new, on="customer_id", how="outer", suffixes=('_old', '_new'))

# Identify updated or new rows (SCD Type 1 logic)
changes = merged[
    (merged['name_old'].isna()) |  # new customer
    (merged['name_old'] != merged['name_new']) | 
    (merged['address_old'] != merged['address_new'])
]

# Create updated rows from new data
updated_df = changes[['customer_id', 'name_new', 'address_new', 'last_updated_new']]
updated_df.columns = columns  # Rename to match original schema

# Keep unchanged records
unchanged = df_existing[~df_existing['customer_id'].isin(updated_df['customer_id'])]

# Final SCD Type 1 result
final_df = pd.concat([unchanged, updated_df]).sort_values(by='customer_id').reset_index(drop=True)

print(final_df)

```
