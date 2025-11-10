# from pyspark.sql import SparkSession




# from pyspark.sql.functions import lit, rank
#
# spark = SparkSession.builder.appName("wordCount").getOrCreate()
#
# sc = spark.sparkContext
#
# my_file = sc.textFile("Bigmarket.csv")
#
# words = my_file.flatMap(lambda line: line.split())
#
# word_pairs = words.map(lambda word: (word.lower(),1))
#
# word_counts = word_pairs.reduceByKey(lambda a, b: a +b)
#
# sorted_counts = word_counts.sortBy(lambda x: x[1], ascending=False)
#
# sorted_counts.saveAsTextFile("sorted_output.csv")
#
# for word, count in sorted_counts.take(10):
#     print(f"{word}: {count}")
#
# spark.stop()
#
#
# df = spark.read.csv("bigmarket.csv")
#
# df_with_new = df.withColumn("status", lit("active"))
#
# df = df.withColumn("rank", rank().over(df_with_new))