from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType

catalog = "zomato"
source_path = "/Volumes/zomato/landing/raw"
bronze_schema = "bronze"

# Raw dimension CSVs carry a leading unnamed index column and, for users.csv,
# headers with spaces ("Marital Status", "Monthly Income", ...) that Delta
# rejects as column names. Passing an explicit schema + header="true" skips
# the header row as data without using it for name-based column matching,
# so Auto Loader never sees those invalid header strings.
restaurants_schema = StructType([
    StructField("_idx", StringType()),
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("city", StringType()),
    StructField("rating", StringType()),
    StructField("rating_count", StringType()),
    StructField("cost", StringType()),
    StructField("cuisine", StringType()),
    StructField("lic_no", StringType()),
    StructField("link", StringType()),
    StructField("address", StringType()),
    StructField("menu", StringType()),
])

users_schema = StructType([
    StructField("_idx", StringType()),
    StructField("user_id", StringType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("password", StringType()),
    StructField("age", StringType()),
    StructField("gender", StringType()),
    StructField("marital_status", StringType()),
    StructField("occupation", StringType()),
    StructField("monthly_income", StringType()),
    StructField("education", StringType()),
    StructField("family_size", StringType()),
])

food_schema = StructType([
    StructField("_idx", StringType()),
    StructField("f_id", StringType()),
    StructField("item", StringType()),
    StructField("veg_or_non_veg", StringType()),
])

menu_schema = StructType([
    StructField("_idx", StringType()),
    StructField("menu_id", StringType()),
    StructField("r_id", StringType()),
    StructField("f_id", StringType()),
    StructField("cuisine", StringType()),
    StructField("price", StringType()),
])


@dp.table(
    name=f"{catalog}.{bronze_schema}.restaurants",
    comment="Restaurant dimension loaded via Auto Loader"
)
def restaurants():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(restaurants_schema)
        .load(f"{source_path}/restaurants")
    )


@dp.table(
    name=f"{catalog}.{bronze_schema}.users",
    comment="User dimension loaded via Auto Loader"
)
def users():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(users_schema)
        .load(f"{source_path}/users")
    )


@dp.table(
    name=f"{catalog}.{bronze_schema}.food",
    comment="Food dimension loaded via Auto Loader"
)
def food():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(food_schema)
        .load(f"{source_path}/food")
    )


@dp.table(
    name=f"{catalog}.{bronze_schema}.menu",
    comment="Menu dimension loaded via Auto Loader"
)
def menu():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(menu_schema)
        .load(f"{source_path}/menu")
    )


@dp.table(
    name=f"{catalog}.{bronze_schema}.orders",
    comment="Orders fact loaded via Auto Loader"
)
def orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(f"{source_path}/orders")
    )


@dp.table(
    name=f"{catalog}.{bronze_schema}.order_items",
    comment="Order items fact loaded via Auto Loader"
)
def order_items():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(f"{source_path}/order_items")
    )


@dp.table(
    name=f"{catalog}.{bronze_schema}.reviews",
    comment="Reviews fact loaded via Auto Loader"
)
def reviews():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(f"{source_path}/reviews")
    )