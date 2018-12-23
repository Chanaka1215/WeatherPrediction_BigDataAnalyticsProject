from pyspark.sql import Row
import pyspark.sql.functions as func
from pyspark.sql import types as T


class DataFilter():

    @staticmethod
    def group_by_day(spark_context, sql_context,  path_to_file):
        real_csv_file_structure = spark_context.textFile(path_to_file)

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
        weather_rdd = prepared_rdd.map(
            lambda x: Row(Formated_Date=x[0], Summary=x[1], Precip_Type=x[2], Temp=float(x[3]),
                          Apparent_Temp=float(x[4]), Humidity=float(x[5]), Wind_Speed=float(x[6]),
                          Wind_Bearing=float(x[7]), Visibility=float(x[8]), Pressure=float(x[9])))
        weather_df = sql_context.createDataFrame(weather_rdd)

        # mean = weather_schema.groupBy("Formated_Date").agg({"Temp":"mean", "Humidity":"mean"}).show()
        udf_to_find_mode = func.udf(DataFilter.get_mode_of_array, T.StringType())
        udf_to_find_mean_angle = func.udf(DataFilter.get_mean_angle, T.IntegerType())

        analyzed_df = weather_df.groupBy("Formated_Date") \
            .agg( \

            func.max('Temp').alias('Max_Temp'),
            func.min('Temp').alias('Min_Temp'),
            func.mean('Temp').alias('Avg_Temp'),

            func.max('Apparent_Temp').alias('Max_Apparent_Temp'),
            func.min('Apparent_Temp').alias('Min_Apparent_Temp'),
            func.mean('Apparent_Temp').alias('Avg_Apparent_Temp'),

            func.max('Humidity').alias('Max_Humidity'),
            func.min('Humidity').alias('Min_Humidity'),
            func.mean('Humidity').alias('Avg_Humidity'),

            func.max('Wind_Speed').alias('Max_Wind_Speed'),
            func.min('Wind_Speed').alias('Min_Wind_Speed'),
            func.mean('Wind_Speed').alias('Avg_Wind_Speed'),

            func.max('Wind_Bearing').alias('Max_Wind_Bearing'),
            func.min('Wind_Bearing').alias('Min_Wind_Bearing'),
            udf_to_find_mean_angle(func.collect_list('Wind_Bearing')).alias('Avg_Wind_Bearing'),

            func.max('Visibility').alias('Max_Visibility'),
            func.min('Visibility').alias('Min_Visibility'),
            func.mean('Visibility').alias('Avg_Visibility'),

            func.max('Pressure').alias('Max_Pressure'),
            func.min('Pressure').alias('Min_Pressure'),
            func.mean('Pressure').alias('Avg_Pressure'),

            udf_to_find_mode(func.collect_list('Summary')).alias('Summary')

        )

        analyzed_df.orderBy(analyzed_df.Formated_Date.desc())
        return analyzed_df





    @staticmethod
    def group_by_year(spark_context, sql_context,  path_to_file):
        real_csv_file_structure = spark_context.textFile(path_to_file)

        header = real_csv_file_structure.first()
        real_csv_file_structure = real_csv_file_structure.filter(lambda row: row != header)

        prepared_rdd = real_csv_file_structure.map(lambda line: (line.split(',')[0].split(" ")[0].split('-')[0],
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
        weather_rdd = prepared_rdd.map(
            lambda x: Row(Formated_Date=x[0], Summary=x[1], Precip_Type=x[2], Temp=float(x[3]),
                          Apparent_Temp=float(x[4]), Humidity=float(x[5]), Wind_Speed=float(x[6]),
                          Wind_Bearing=float(x[7]), Visibility=float(x[8]), Pressure=float(x[9])))
        weather_df = sql_context.createDataFrame(weather_rdd)
        # mean = weather_schema.groupBy("Formated_Date").agg({"Temp":"mean", "Humidity":"mean"}).show()
        udf_to_find_mode = func.udf(DataFilter.get_mode_of_array, T.StringType())
        udf_to_find_mean_angle = func.udf(DataFilter.get_mean_angle, T.FloatType())

        analyzed_df = weather_df.groupBy("Formated_Date") \
            .agg( \

            func.max('Temp').alias('Max_Temp'),
            func.min('Temp').alias('Min_Temp'),
            func.mean('Temp').alias('Avg Temp'),

            func.max('Apparent_Temp').alias('Max_Apparent_Temp'),
            func.min('Apparent_Temp').alias('Min_Apparent_Temp'),
            func.mean('Apparent_Temp').alias('Avg_Apparent_Temp'),

            func.max('Humidity').alias('Max_Humidity'),
            func.min('Humidity').alias('Min_Humidity'),
            func.mean('Humidity').alias('Avg_Humidity'),

            func.max('Wind_Speed').alias('Max_Wind_Speed'),
            func.min('Wind_Speed').alias('Min_Wind_Speed'),
            func.mean('Wind_Speed').alias('Avg_Wind_Speed'),

            func.max('Wind_Bearing').alias('Max_Wind_Bearing'),
            func.min('Wind_Bearing').alias('Min_Wind_Bearing'),
            udf_to_find_mean_angle(func.collect_list('Wind_Bearing')).alias('Avg_Wind_Bearing'),

            func.max('Visibility').alias('Max_Visibility'),
            func.min('Visibility').alias('Min_Visibility'),
            func.mean('Visibility').alias('Avg_Visibility'),

            func.max('Pressure').alias('Max_Pressure'),
            func.min('Pressure').alias('Min_Pressure'),
            func.mean('Pressure').alias('Avg_Pressure'),

            udf_to_find_mode(func.collect_list('Summary')).alias('Summary')

        )

        analyzed_df.orderBy(analyzed_df.Formated_Date.desc())
        return analyzed_df

    @staticmethod
    def write_to_csv(data_frame, path_to_output_file):
        data_frame \
            .coalesce(1) \
            .write \
            .option("header", "true") \
            .mode("overwrite")\
            .csv(path_to_output_file)

    @staticmethod
    def get_mode_of_array(x):
        from collections import Counter
        most = Counter(x).most_common(1)[0][0]
        return most

    @staticmethod
    def get_mean_angle(deg):
        from cmath import rect, phase
        from math import radians, degrees
        mean_angle = round(degrees(phase(sum(rect(1, radians(d)) for d in deg) / len(deg))))
        return abs(mean_angle)
