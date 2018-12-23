import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn import datasets
from sklearn.preprocessing import StandardScaler
from data_filter import DataFilter
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from feature_selector import FeatureSelector
from sklearn.preprocessing import LabelEncoder
import tabulate
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score,recall_score, confusion_matrix, classification_report,accuracy_score, f1_score


class LogisticRegression():

    @staticmethod
    def classify(X, Y):
        X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.3, random_state=5)
        print(len(X_train))
        print("================================")
        print(len(X_test))

        sc = StandardScaler()
        X_train = sc.fit_transform(X_train)
        X_test = sc.transform(X_test)

        classifierRF = RandomForestClassifier(n_estimators=100, criterion='entropy', random_state=0)
        classifierRF.fit(X_train, y_train)

        # Fitting K-NN to the Training set
        classifierKN = KNeighborsClassifier(n_neighbors=7, metric='minkowski', p=2)
        classifierKN.fit(X_train, y_train)

        # Fitting Naive Bayes to the Training set
        classifierNB = GaussianNB()
        classifierNB.fit(X_train, y_train)

        # Predicting the Test set results of rf
        y_predRF = classifierRF.predict(X_test)
        print(confusion_matrix(y_test, y_predRF))
        print(accuracy_score(y_test, y_predRF))
        print("=====================>")

        # Predicting the Test set results of knn
        y_predKN = classifierKN.predict(X_test)
        cmKN = confusion_matrix(y_test, y_predKN)
        print(accuracy_score(y_test, y_predKN))
        print(confusion_matrix(y_test, y_predKN))
        print("=====================>")

        # Predicting the Test set results of nb
        y_predNB = classifierNB.predict(X_test)
        cmNB = confusion_matrix(y_test, y_predNB)
        print(accuracy_score(y_test, y_predNB))
        print(confusion_matrix(y_test, y_predNB))
        print("=====================>")












