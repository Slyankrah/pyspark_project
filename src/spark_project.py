from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import sys

# run_date = sys.argv[1] if len(sys.argv)>1 else None
# if not run_date:
#     raise SystemExit("Usage: etl_job.py <load_date>")

spark = (SparkSession.builder.
         appName("raw_loans_to_curated").
         enableHiveSupport().
         getOrCreate())

spark.sql("use sylvester_db")

raw = spark.table("sylvester_db.sylvester_loans_ext")

df = (raw.withColumn("id", col("loan_id").cast("long"))
         .withColumn("balance", col("balance").cast("double"))
         .withColumn("start_date", to_date(col("start_date"), "yyyy-MM-dd"))
     )

print(df.show())


# w = Window.partitionBy("id").orderBy(col("event_time").desc())
# df = df.withColumn("rn", row_number().over(w)).filter(col("rn")==1).drop("rn")

# # write
# (df.drop("rn")
#    .write
#    .mode("append")
#    .partitionBy("load_date")
#    .format("parquet")
#    .option("compression","snappy")
#    .saveAsTable("curated_db.curated_table")
# )
#
# spark.stop()

