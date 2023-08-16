import pyspark
#Sudip Preparation
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


if __name__ == "__main__":
    #initialize SparkSession
    spark = SparkSession.builder.appName("Pyspark-sparkSQL-comparision").getOrCreate()
    df = spark.read.format("parquet").option("header","true").load("file:///D:/datasets/retail_db/orders_parquet/741ca897-c70e-4633-b352-5dc3414c5680.parquet")
    df.show(2)
    filter_df = df.filter(df.order_customer_id > 200).show()
    agg_df = df.groupBy(df.order_id).agg(sum(df.order_customer_id)).orderBy((df.order_id).desc()).show()
    # converting the above code into Pyspark sql
    df.createOrReplaceTempView("spark_df")
    sql1 = spark.sql("select order_id, sum('order_customer_id') as total from spark_df group by order_id order by order_id desc").show(2)
    #comparision of select syntax
    select_df = df.select(F.col("order_status")).distinct().show()
    # use this syntax
    select_df1 = df.select(df.order_status).distinct().show()
    # converting this into sql
    select_sql = spark.sql("select distinct order_status from spark_df").show()

    # comparision of order by
    orderby_df = df.orderBy((df.order_customer_id).desc()).show(2)

    # Now wrote the code in sql
    orderby_sql_df = spark.sql("select * from spark_df order by order_customer_id desc limit 2")


