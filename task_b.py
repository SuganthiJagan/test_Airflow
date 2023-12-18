# Map users to distribution centres
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, regexp_extract, split, size, slice, concat_ws, concat, col, lit
from delta.tables import *

FILE_PATH = "s3://data-de-sanitized-dev-prepared-20200122052101862000000005/test/"
delta_name = "somefile"

def initialize_spark_session():
    return SparkSession.builder.getOrCreate()

def read_data(spark, file_loc):
    df_orders_file = "orders.csv"
    df_orders = spark.read.option('header', 'true').option('inferSchema','true').format("csv").load(file_loc)
    deltaTable = DeltaTable.forPath(spark, "s3://data-de-sanitized-dev-prepared-20200122052101862000000005/test/test_orders.delta")
    dist_mapped_customers = deltaTable.toDF()
    transform_data(spark, df_orders,dist_mapped_customers)

def transform_data(spark, df_orders, dist_mapped_customers):
    dist_mapped_customers.createOrReplaceTempView("users_mapped")
    df_orders.createOrReplaceTempView("orders")
    df_cust_returns = spark.sql("""select u.id as user_id, u.first_name, u.last_name, u.age, u.country, u.centre, u.dist_id, o.order_id, o.status, o.returned_at, o.num_of_item  from users_mapped as u 
    LEFT JOIN orders as o
    ON u.id = o.user_id
    where o.status = 'Returned' ORDER BY u.id
    """)

    write_data(df_cust_returns)

def write_data(df_cust_returns):
    df_cust_returns.write.format("delta").save(FILE_PATH+delta_name)

def main():
    spark = initialize_spark_session()
    read_data(spark,FILE_PATH)
    



    