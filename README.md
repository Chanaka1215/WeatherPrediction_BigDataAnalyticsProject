# WeatherPrediction - BigData Analytics Project
This is the mini project related with the course module IN4410 that uses the Apache Spark.
###Contributors
* Fernando S.D.N.C
* Karunarathne L.A.H
* Hasantha E.A.C

### Pre-Requisites
* Apache Spark 2.2.0
* Python 3.6
* PyCharm IDE

### How to setup the envirnment
This project was carried out on windows 10 pc. To run this project, first of all you
need to setup the Apache Spark 2.2.0 and python 3.6 on windows 10.
* Note* latest Apache Spark versions >=2.4.0 may not work since pySpark module not support for windows Environment
* Note* We experienced some erroneous behaviour when using python version >=3.7
  
To setup the environment, please watch this blog site 
https://hernandezpaul.wordpress.com/2016/01/24/apache-spark-installation-on-windows-10/

To integrate into pyCharm IDEA, read this blog site
https://kaizen.itversity.com/setup-spark-development-environment-pycharm-and-python/


### Project Structure  
        =>WeatherPrediction_BigDataAnalyticsProject
           => .idea
           => __pycache__
           => venv
           => weather_by_day.csv    * The output of the DataTransformation stage
           => data_filter.py        * Data Transformation
           => data_visualizer.py    * Visialization
           => feature_selector.py   * Feature selection
           => machine_learning.py   * Model training 
           => main.py               * Main program
           => README.md             
           => weatherHistory.csv    *This is the original dataset used in this project





## How to run the project
* Before running the the project, set the path_to_csv_file variable that maches with your project location
. Then
    * run the 'main.py' file