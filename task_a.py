# Map users to distribution centres
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import expr, regexp_extract, split, size, slice, concat_ws, concat, col, lit

DATASET_NAME = "users_near_distcentre.delta"
FILE_PATH = "s3://data-de-sanitized-dev-prepared-20200122052101862000000005/test/"

def initialize_spark_session():
    return SparkSession.builder.getOrCreate()
     

def read_data(spark, file_loc):
    users_file="users.csv"
    dist_centre_file="some.csv"
    df_dist_centre = (
                spark.read.option('header', 'true')
                      .option('inferSchema','true')
                      .format("csv")
                      .load(file_loc+dist_centre_file)
    )
    df_users = spark.read.option('header', 'true').option('inferSchema','true').format("csv").load(file_loc+users_file)

    transform_data(df_dist_centre, df_users)

def transform_data(df_dist_centre, df_users):
    df_dist_centre=df_dist_centre.na.replace(['Port Authority of New York/New Jersey NY/NJ'], ['New York NY'], 'name')
    df_dist_centre = df_dist_centre.withColumn("state", split(df_dist_centre.name, " "))
    df_dist_centre=df_dist_centre.select('id',concat_ws(" ", slice(df_dist_centre.state, 1, (size(df_dist_centre.state)-1))),'latitude','longitude')
    df_dist_centre=df_dist_centre.withColumnRenamed('concat_ws( , slice(state, 1, (size(state) - 1)))', 'centre')
    df_dist_centre=df_dist_centre.withColumnRenamed('id', 'dist_id')
    df_users=df_users.drop('email','gender','street_address','postal_code','city','latitude','longitude', 'traffic_source','created_at')
    df_users_near_distcentre=df_users.join(df_dist_centre, df_users['state'] == df_dist_centre['centre'], 'full')
    #Write data to delta file
    write_data(df_users_near_distcentre)

def write_data(df_users_near_distcentre):

    df_users_near_distcentre.write.format("delta").save(FILE_PATH+DATASET_NAME)

def main():
    spark = initialize_spark_session()
    read_data(spark, FILE_PATH)
    

    