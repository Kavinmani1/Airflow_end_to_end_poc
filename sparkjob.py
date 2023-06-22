from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofmonth,month,date_format,year
from pyspark.sql.functions import col
from pyspark.sql.functions import max
from pyspark.sql.functions import corr
from pyspark.sql.functions import avg

spark = SparkSession.builder \
        .appName("timber_capstone") \
        .master("local[1]") \
        .getOrCreate()
rawdf = spark.read.csv("/root/airflow/input_files/timberland_stock.csv",header = True)
rawdf.show()
rawdf.printSchema()
df = rawdf.withColumn("day_of_week", date_format("Date", "EEEE"))
df.show()
df_timber = df.withColumnRenamed("Adj Close","Adj_price")
df1 = df_timber.withColumn("close", col("close").cast("float"))
df1 = df1.withColumn("Adj_price", col("Adj_price").cast("float"))
df1 = df1.withColumn("Low", col("Low").cast("float"))
df1 = df1.withColumn("High", col("High").cast("float"))
df1 = df1.withColumn("Open", col("Open").cast("float"))
df1 = df1.withColumn("Volume", col("Volume").cast("float"))
df1.printSchema()
df1.createOrReplaceTempView("tim")
max_high_row = df1.groupBy("Date").agg(max("High").alias("max_high")).orderBy("max_high", ascending=False).first()
peak_high_date = max_high_row["Date"]
peak_high_price = max_high_row["max_high"]
print("Date with the peak high price:", peak_high_date)
print("Peak high price:", peak_high_price)
mean_close_stock_value = spark.sql("select avg(close) AS Mean_close_Value from tim").show()
min_max_volume = spark.sql("select min(volume),max(volume) from  tim").show()
count_of_days = spark.sql("select count(close) as count_of_close_days  from tim where close<60").show()
count_high_greater_than_80 = df1.filter(col("high") > 80).count()
total_rows = df1.count()
percentage_high_greater_than_80 = (count_high_greater_than_80 / total_rows) * 100
print("Percentage of time High > $80:", percentage_high_greater_than_80)
correlation_value = df1.select(corr(col("High"), col("Volume"))).collect()[0][0]
print("Pearson correlation:", correlation_value)
df = df1.withColumn("year", year("date"))
df.show()
max_high_per_year = df.groupBy("year").agg(max("high").alias("max_high")).show()
df2 = df.withColumn("Month", month("date"))
avg_close_per_month = df2.groupBy("year", "month").agg(avg("close").alias("avg_close")).orderBy("year", "month")
avg_close_per_month.show()
df1.write.csv('/root/airflow/outputfiles/df1.csv', mode='overwrite', header=True)
max_high_row..write.csv('/root/airflow/outputfiles/max_high_row.csv', mode='overwrite', header=True)
mean_close_stock_value..write.csv('/root/airflow/outputfiles/mean_close_stock_value.csv', mode='overwrite', header=True)
min_max_volume.write.csv('/root/airflow/outputfiles/min_max_volume.csv', mode='overwrite', header=True)
max_high_per_year.write.csv('/root/airflow/outputfiles/max_high_per_year.csv', mode='overwrite', header=True)
avg_close_per_month.write.csv('/root/airflow/outputfiles/avg_close_per_month.csv', mode='overwrite', header=True)
spark.stop()