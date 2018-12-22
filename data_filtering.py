from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as func
from pyspark.sql import types as T
import matplotlib.pyplot as plt

sc = SparkContext(master="local", appName="spark_test")
sqlContext = SQLContext(sc)

path_to_file = "file:///D:/weatherHistory.csv"
path_to_output_file = "file:///D:/weatherSummary.csv"

real_csv_file_structure = sc.textFile(path_to_file)
header = real_csv_file_structure.first()
real_csv_file_structure = real_csv_file_structure.filter(lambda row: row != header)

prepared_rdd = real_csv_file_structure.map(lambda line: (line.split(',')[0].split(" ")[0],
                                                         line.split(',')[1],
                                                         line.split(',')[2],
                                                         line.split(',')[3],
                                                         line.split(',')[4],
                                                         line.split(',')[5],
                                                         line.split(',')[6],
                                                         line.split(',')[7],
                                                         line.split(',')[8],
                                                         line.split(',')[10],
                                                         line.split(',')[11]))

# Temperature in Celceus, Wind_Speed in Km/h, Wind Bearing in Degrees, Pressure in Millibars
weather_rdd = prepared_rdd.map(lambda x: Row(Formated_Date=x[0], Summary=x[1], Precip_Type=x[2], Temp=float(x[3]),
                                             Apparent_Temp=float(x[4]), Humidity=float(x[5]),Wind_Speed=float(x[6]),
                                             Wind_Bearing=float(x[7]), Visibility=float(x[8]), Pressure=float(x[9]),
                                             Daily_Summary=x[10]))
weather_schema = sqlContext.createDataFrame(weather_rdd)


# prepared_map.foreach(print)
# print(real_csv_file_structure.take(2))
# print(prepared_map.take(2))
# print(weather_schema)
# print(weather_schema.printSchema())
# print(weather_schema.head(3))
# print(weather_schema.show(2,truncate= True))
# print(weather_schema.count())
# print(len(weather_schema.columns))
# print(weather_schema.columns)
# print(weather_schema.describe().show())
# print(weather_schema.describe('Temp').show())
# print(weather_schema.select('Formated_Date','Temp').show(5))
# print(weather_schema.select('Formated_Date').distinct().count())
# print(weather_schema.groupby('Formated_Date').agg({'Temp': 'mean'}).show())
# print(weather_schema.groupby('Formated_Date').count().show())

# mean = weather_schema.groupBy("Formated_Date").agg({"Temp":"mean", "Humidity":"mean"}).show()
def get_mode_of_array(x):
    from collections import Counter
    most = Counter(x).most_common(1)[0][0]
    return most


udf_to_find_mode = func.udf(get_mode_of_array, T.StringType())

analyzed_df = weather_schema.groupBy("Formated_Date") \
    .agg( \
    udf_to_find_mode(func.collect_list('Summary')).alias('Summary'),

    func.max('Temp').alias('Max Temp'),
    func.min('Temp').alias('Min Temp'),
    func.mean('Temp').alias('Avg Temp'),

    func.max('Apparent_Temp').alias('Max Apparent_Temp'),
    func.min('Apparent_Temp').alias('Min Apparent_Temp'),
    func.mean('Apparent_Temp').alias('Avg Apparent_Temp'),

    func.max('Humidity').alias('Max Humidity'),
    func.min('Humidity').alias('Min Humidity'),
    func.mean('Humidity').alias('Avg Humidity'),

    func.max('Wind_Speed').alias('Max Wind_Speed'),
    func.min('Wind_Speed').alias('Min Wind_Speed'),
    func.mean('Wind_Speed').alias('Avg Wind_Speed'),

    func.max('Wind_Bearing').alias('Max Wind_Bearing'),
    func.min('Wind_Bearing').alias('Min Wind_Bearing'),
    func.mean('Wind_Bearing').alias('Avg Wind_Bearing'),

    func.max('Visibility').alias('Max Visibility'),
    func.min('Visibility').alias('Min Visibility'),
    func.mean('Visibility').alias('Avg Visibility'),

    func.max('Pressure').alias('Max Pressure'),
    func.min('Pressure').alias('Min Pressure'),
    func.mean('Pressure').alias('Avg Pressure'),

    udf_to_find_mode(func.collect_list('Daily_Summary')).alias('Daily_Summary')
)

analyzed_df.show()
# print(analyzed_df.count())

# analyzed_df \
#     .coalesce(1) \
#     .write \
#     .option("header", "true") \
#     .mode("overwrite")\
#     .csv(path_to_output_file)

sc.stop()
