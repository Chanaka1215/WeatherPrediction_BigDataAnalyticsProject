
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, FloatType,DateType
import numpy as np

from pyspark.sql import types as T
from pandas import DataFrame

import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import matplotlib as mpl
import numpy as np
import seaborn as sns


from pyspark.mllib.stat import Statistics
import pandas as pd

sc = SparkContext(master="local", appName="spark_test")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName('spark_test').getOrCreate()
path_to_file = "file:///D:/weatherHistory.csv"
path_to_output_file = "file:///D:/weatherSummary.csv"

real_csv_file_structure = sc.textFile(path_to_file)
header = real_csv_file_structure.first()
real_csv_file_structure = real_csv_file_structure.filter(lambda row: row != header)

# schema = StructType([
#     StructField("Formated_Date", IntegerType()),
#     StructField("Summary", StringType()),
#     StructField("Precip_Type", StringType()),
#     StructField("Temperature (C)", FloatType()),
#     StructField("Apparent_Temp", FloatType()),
#     StructField("Humidity", FloatType()),
#     StructField("Wind_Speed", FloatType()),
#     StructField("Wind_Bearing", FloatType()),
#     StructField("Visibility", FloatType()),
#     StructField("Pressure", FloatType()),
#     StructField("Daily_Summary", StringType())
# ])
#
# weatherDF = spark.read.csv('D:/weatherHistory.csv', header=True, schema=schema)
#
# weatherDF.show(10)

prepared_rdd = real_csv_file_structure.map(lambda line: (line.split(',')[0].split("-")[1],
                                                         line.split(',')[3],
                                                         line.split(',')[4],
                                                         line.split(',')[5],
                                                         line.split(',')[6],
                                                         line.split(',')[7],
                                                         line.split(',')[8],
                                                         line.split(',')[10]))

# Temperature in Celceus, Wind_Speed in Km/h, Wind Bearing in Degrees, Pressure in Millibars
weather_rdd = prepared_rdd.map(lambda x: Row(Formated_Date=x[0], Temp=float(x[1]),
                                             Apparent_Temp=float(x[2]), Humidity=float(x[3]),Wind_Speed=float(x[4]),
                                             Wind_Bearing=float(x[5]), Visibility=float(x[6]), Pressure=float(x[7])))
weather_schema = sqlContext.createDataFrame(weather_rdd)

weather_schema.show(10)

df = weather_schema
col_names = df.columns
features = df.rdd.map(lambda row: row[0:])
corr_mat=Statistics.corr(features, method="spearman")
corr_df = pd.DataFrame(corr_mat)
corr_df.index, corr_df.columns = col_names, col_names

print(corr_df.to_string())

f, ax = plt.subplots(figsize=(10, 6))
hm = sns.heatmap(round(corr_df,3), annot=True, ax=ax, cmap="coolwarm",fmt='.2f',linewidths=.05)
f.subplots_adjust(top=0.93)
t= f.suptitle('Weather Attributes Correlation Heatmap', fontsize=14)
plt.show()
sc.stop()
