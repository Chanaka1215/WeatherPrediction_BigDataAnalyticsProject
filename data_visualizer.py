import matplotlib.pyplot as plt
import numpy as np


class DataVisualizer():

    @staticmethod
    def draw_min_max_tempearature_graph(df):
        years = [(i.Formated_Date) for i in df.select('Formated_Date').collect()]
        max_temp_list = [float(i.Max_Apparent_Temp) for i in df.select('Max_Apparent_Temp').collect()]
        min_temp_list = [float(i.Min_Apparent_Temp) for i in df.select('Min_Apparent_Temp').collect()]

        n_groups = len(years)
        fig, ax = plt.subplots()
        index = np.arange(n_groups)
        bar_width = 0.35
        opacity = 0.8

        rects1 = plt.bar(index, max_temp_list, bar_width, alpha=opacity, color='b', label='Max Temperature')

        rects2 = plt.bar(index, min_temp_list, bar_width, alpha=opacity, color='g', label='Min Temperature')

        plt.xlabel('Year')
        plt.ylabel('Temperature')
        plt.title('Maximum & Minimum Apparent Temperature')
        plt.xticks(index + bar_width, years)
        plt.legend()

        plt.tight_layout()
        plt.show()


        @staticmethod
        def draw_min_max_humidity_graph(df):
            years = [(i.Formated_Date) for i in df.select('Formated_Date').collect()]
            max_list = [float(i.Max_Humidity) for i in df.select('Max_Humidity').collect()]
            min_list = [float(i.Min_Humidity) for i in df.select('Min_Humidity').collect()]

            n_groups = len(years)
            fig, ax = plt.subplots()
            index = np.arange(n_groups)
            bar_width = 0.35
            opacity = 0.8

            rects1 = plt.bar(index, max_list, bar_width,
                             alpha=opacity,
                             color='b',
                             label='Max Humidity')

            rects2 = plt.bar(index, min_list, bar_width,
                             alpha=opacity,
                             color='g',
                             label='Min Humidity')

            plt.xlabel('Year')
            plt.ylabel('Humidity')
            plt.title('Maximum & Minimum Humidity')
            plt.xticks(index + bar_width, years)
            plt.legend()

            plt.tight_layout()
            plt.show()

    @staticmethod
    def draw_min_max_wind_speed_graph(df):
        years = [(i.Formated_Date) for i in df.select('Formated_Date').collect()]
        max_list = [float(i.Max_Wind_Speed) for i in df.select('Max_Wind_Speed').collect()]
        min_list = [float(i.Min_Wind_Speed) for i in df.select('Min_Wind_Speed').collect()]

        n_groups = len(years)
        fig, ax = plt.subplots()
        index = np.arange(n_groups)
        bar_width = 0.35
        opacity = 0.8

        rects1 = plt.bar(index, max_list, bar_width,
                         alpha=opacity,
                         color='b',
                         label='Max Wind Speed')

        rects2 = plt.bar(index, min_list, bar_width,
                         alpha=opacity,
                         color='g',
                         label='Min Wind Speed')

        plt.xlabel('Year')
        plt.ylabel('Wind Speed')
        plt.title('Maximum & Minimum Wind Speed')
        plt.xticks(index + bar_width, years)
        plt.legend()

        plt.tight_layout()
        plt.show()

    @staticmethod
    def draw_average_visibility_graph(df):
        years = [(i.Formated_Date) for i in df.select('Formated_Date').collect()]
        max_list = [float(i.Avg_Visibility) for i in df.select('Avg_Visibility').collect()]

        n_groups = len(years)
        fig, ax = plt.subplots()
        index = np.arange(n_groups)
        bar_width = 0.35
        opacity = 0.8

        rects1 = plt.bar(index, max_list, bar_width,
                         alpha=opacity,
                         color='b',
                         label='Average Visibility')



        plt.xlabel('Year')
        plt.ylabel('Average Visibility')
        plt.title('Average Visibility (2006 - 2016)')
        plt.xticks(index + bar_width, years)
        plt.legend()

        plt.tight_layout()
        plt.show()

    @staticmethod
    def draw_average_pressure_graph(df):
        years = [(i.Formated_Date) for i in df.select('Formated_Date').collect()]
        max_list = [float(i.Avg_Pressure) for i in df.select('Avg_Pressure').collect()]

        n_groups = len(years)
        fig, ax = plt.subplots()
        index = np.arange(n_groups)
        bar_width = 0.35
        opacity = 0.8

        rects1 = plt.bar(index, max_list, bar_width,
                         alpha=opacity,
                         color='b',
                         label='Average Pressure')

        plt.xlabel('Year')
        plt.ylabel('Average Pressure')
        plt.title('Average Pressure (2006 - 2016)')
        plt.xticks(index + bar_width, years)
        plt.legend()

        plt.tight_layout()
        plt.show()
