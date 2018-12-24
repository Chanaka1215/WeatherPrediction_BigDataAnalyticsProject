from sklearn.preprocessing import LabelEncoder
from feature_selector import FeatureSelector
from pyspark import SparkContext
from pyspark.sql import SQLContext
from data_filter import DataFilter
from data_visualizer import DataVisualizer
from machine_learning import MachineLearning
import matplotlib.pyplot as plt
import pandas as pd



path_to_csv_file = "file:///C:/Users/Chanaka/OneDrive/python/WeatherPrediction_BigDataAnalyticsProject/weatherHistory.csv"
path_to_output_csv = "file:///C:/Users/Chanaka/OneDrive/python/WeatherPrediction_BigDataAnalyticsProject/weather_by_day.csv"

# Create spark context and sql context
print("[INFO] Initializing spark context")
spark_context = SparkContext(master="local[*]", appName="spark_test")
sql_context = SQLContext(spark_context)
print("[INFO] Initializing spark context:: done")

# Read the CSV file preprocess and  group by day basis
print("[INFO] Red CSV file, Group by day, load into data frame and write back to CSV output file")
day_weather_df = DataFilter.group_by_day(spark_context, sql_context, path_to_csv_file)
day_weather_df.show()
df = day_weather_df.select("Avg_Temp","Avg_Apparent_Temp","Avg_Humidity", "Avg_Wind_Speed","Avg_Wind_Bearing","Avg_Visibility","Avg_Pressure","Summary" )
df.show()
df.printSchema()
DataFilter.write_to_csv(df, path_to_output_csv)
print("[INFO] Red CSV file, Group by day, load into data frame:: done")


# Read the dataset and group by year
print("[INFO] Red CSV file, Group by year, load into data frame and write back to CSV output file")
year_df = DataFilter.group_by_year(spark_context,sql_context, path_to_csv_file)
year_df.show()
final = year_df.orderBy('Formated_Date', ascending=True)
final.show()
print("[INFO] Red CSV file, Group by year, load into data frame:: done")


# Visualization
print("[INFO] Visualize data")
DataVisualizer.draw_average_visibility_graph(final)
DataVisualizer.draw_average_pressure_graph(final)
DataVisualizer.draw_min_max_tempearature_graph(final)
DataVisualizer.draw_min_max_wind_speed_graph(final)
DataVisualizer.draw_min_max_humidity_graph(final)
print("[INFO] Visualize data:: done")


# Load preprocessed csv file into memory
# This csv file is exactly same as preprocessed csv file in the above steps
# For Fast reading we kept a copy of it. so this part can be run independently
print("[INFO] Start feature selection")
df = pd.read_csv("C:/Users/Chanaka/OneDrive/python/WeatherPrediction_BigDataAnalyticsProject/csv_day_wise.csv")
print(df.head())

# Check class imbalance and drow the class frequecncy table
fig = plt.figure(figsize=(8, 6))
df.groupby('Summary').Summary.count().plot.bar(ylim=0)
plt.show()
print(df.groupby("Summary").Summary.count())

# Feature selection
data = df
train_labels = data['Summary']
train = data.drop(columns=['Summary'])
fs = FeatureSelector(data=train, labels=train_labels)

# Find featuers with missing values
fs.identify_missing(missing_threshold=0.8)
fs.missing_stats.head(5)

# Find features with single unique value
fs.identify_single_unique()
fs.unique_stats.sample(5)

# Find features with correlation
fs.identify_collinear(correlation_threshold=0.7)
correlated_features = fs.ops['collinear']
print(fs.record_collinear.head())

# Based on above test prapare new dataset for prediction
new_data = df.drop(columns=['Avg_Apparent_Temp'])

lb_make = LabelEncoder()
new_data["Summary_label"] = lb_make.fit_transform(new_data["Summary"])
print(new_data.head())
# data_for_analyze = new_data.select(["Avg_Temp","Avg_Humidity","Avg_Wind_Speed","Avg_Visibility","Avg_Pressure","Summary_label"])

# X and Y value separation
x = new_data.iloc[:, 0:6].values
y = new_data.iloc[:, 7].values

MachineLearning.classify(x,y)


# indexed_df.show()
# print(indexed_df.select('Summary','SummaryIndex').groupBy('Summary').show())
# print(indexed_df.select('SummaryIndex').distinct().show())

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

spark_context.stop()



