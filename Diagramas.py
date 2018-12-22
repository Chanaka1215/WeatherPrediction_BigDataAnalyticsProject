
import matplotlib.pyplot as plt
import csv

import pandas as pd
from mpl_toolkits.mplot3d import Axes3D
import matplotlib as mpl
import numpy as np
import seaborn as sns

x = []
y = []

with open('D:\mycsv.csv','r') as csvfile:
    plots = csv.reader(csvfile, delimiter=',')
    line_count = 0
    for row in plots:
        if line_count != 0:
            x.append(int(row[1]))
            y.append(float(row[3]))

        line_count+=1

plt.plot(x,y, label='Loaded from file!')
plt.xlabel('x')
plt.ylabel('y')
plt.title('Annual average tempreture')
plt.legend()
plt.show()


with open('D:\mycsv.csv','r') as csvfile:
    plots = csv.reader(csvfile, delimiter=',')
    line_count = 0
    for row in plots:
        if line_count != 0:
            x.append(int(row[1]))
            y.append(float(row[3]))

        line_count+=1

plt.plot(x,y, label='Loaded from file!')
plt.xlabel('x')
plt.ylabel('y')
plt.title('Annual average tempreture')
plt.legend()
plt.show()