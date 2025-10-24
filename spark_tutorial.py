# Word Count (text file). RDD-style(Basic)--------------------------------------------------------------
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("WordCountRDD").getOrCreate()
# sc = spark.sparkContext
# # read text file
# lines_rdd = sc.textFile("Bigmarket.csv")  # or "hdfs:///user/..."
#
# # basic word count
# counts = (lines_rdd
#           .flatMap(lambda line: line.split())            # split into words
#           .map(lambda w: w.lower().strip(".,!?:;\"'()[]"))# normalize
#           .filter(lambda w: len(w) > 0)
#           .map(lambda w: (w, 1))
#           .reduceByKey(lambda a, b: a + b)
#           .sortBy(lambda kv: kv[1], ascending=False)     # sort by frequency
#          )
#
# # collect or save
# for word, cnt in counts.take(20):
#     print(word, cnt)
#
# spark.stop()
#------------------------------------------------------------------------------------------------------------

# # Word Count (text file). DataFrame-style (Spark SQL)-------------------------------------------------------------
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode, split, lower, trim, regexp_replace, col, desc
#
# spark = SparkSession.builder.appName("WordCountDF").getOrCreate()
#
# df = spark.read.text("Bigmarket.csv")   # single column "value"
#
# words = (df
#          .select(explode(split(col("value"), "\\s+")).alias("word"))
#          .select(lower(trim(regexp_replace(col("word"), r"[^\w']", ""))).alias("word"))
#          .filter(col("word") != "")
#         )
#
# word_counts = words.groupBy("word").count().orderBy(desc("count"))
# word_counts.show(20, truncate=False)
#
# spark.stop()
#--------------------------------------------------------------------------------------------------

# # Read a CSV and 10 different transformations----------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum as spark_sum, countDistinct, lit, split, explode, to_date, year
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("CSVTransforms").getOrCreate()

df = (spark.read
      .option("header", True)
      .option("inferSchema", True)   # in prod, specify schema explicitly
      .option("sep", ",")
      .csv("Bigmarket.csv"))

df.printSchema()
df.show(5, truncate=False)
