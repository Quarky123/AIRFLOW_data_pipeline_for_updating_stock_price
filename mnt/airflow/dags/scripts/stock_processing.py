from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("stock processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file forex_rates.json from the HDFS
df = spark.read.csv('hdfs://namenode:9000/user/portfolio/stock.csv')

# Drop the duplicated rows based on the base and last_update columns
forex_rates = df

# Export the dataframe into the Hive table forex_rates
forex_rates.write.mode("append").insertInto("forex_rates")





# from os.path import expanduser, join, abspath

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json
# from pyspark.sql.types import StructType



# warehouse_location = abspath('spark-warehouse')

# # # Initialize Spark Session
# # spark = SparkSession \
# #     .builder \
# #     .appName("stock processing") \
# #     .config("spark.sql.warehouse.dir", warehouse_location) \
# #     .enableHiveSupport() \
# #     .getOrCreate()

# # # Read the file forex_rates.json from the HDFS
# # # df = spark.read.csv('hdfs://namenode:9000/user/portfolio/stock.csv')
# # df = spark.read.csv('hdfs://user/portfolio/stock.csv')


# # # Drop the duplicated rows based on the base and last_update columns
# # stocks_portfolio = df.select('date', 'close', 'volume', 'momentum_rsi') 



# # # Export the dataframe into the Hive table forex_rates
# # stocks_portfolio.write.mode("append").insertInto("stocks_portfolio")

# # Initialize Spark Session

# spark = SparkSession.builder\
# 	.master("local").appName("hdfs_test").getOrCreate()

# # booksSchema = StructType() \
# #                     	.add("id", "integer")\
# #                     	.add("book_title", "string")\
# #                     	.add("publish_or_not", "string")\
# #                     	.add("technology", "string")

# df=spark.read.csv("hdfs://namenode:9000/user/portfolio/stock.csv")
# df.show(5)

# # from here on idk how

# # stock_infor.write.mode("append").insertInto("stock_portfolio")
# # df.write.saveAsTable("default.table1")
# # df.write.insertInto("stock_portfolio",overwrite=True)