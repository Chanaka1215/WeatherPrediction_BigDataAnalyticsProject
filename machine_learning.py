from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import StandardScaler


class MachineLearning():
    @staticmethod
    def classify(X, Y):
        # Train test split
        X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.3, random_state=5)
        print("[INFO]Size of the train dataset :: " + str(len(X_train)))
        print("[INFO]Size of the test dataset :: " + str(len(X_test)))

        # Standardize the dataset
        sc = StandardScaler()
        X_train = sc.fit_transform(X_train)
        X_test = sc.transform(X_test)

        # Fitting Training data into Random Forest classifier
        print("[INFO] Fit data with Random Forest Classifier")
        classifierRF = RandomForestClassifier(n_estimators=100, criterion='entropy', random_state=0)
        classifierRF.fit(X_train, y_train)
        print("[INFO] Fit data with Random Forest Classifier:: done")

        # Fitting K-NN to the Training set
        print("[INFO] Fit data with kNN Classifier")
        classifierKN = KNeighborsClassifier(n_neighbors=7, metric='minkowski', p=2)
        classifierKN.fit(X_train, y_train)
        print("[INFO] Fit data with kNN Classifier:: done")

        # Fitting Naive Bayes to the Training set
        print("[INFO] Fit data with Naive Bayes Classifier")
        classifierNB = GaussianNB()
        classifierNB.fit(X_train, y_train)
        print("[INFO] Fit data with Naive Bayes Classifier:: done")

        # Predicting the Test set results of rf
        print("Prediction using Random Forest")
        y_predRF = classifierRF.predict(X_test)
        print(confusion_matrix(y_test, y_predRF))
        print(accuracy_score(y_test, y_predRF))
        print("Prediction using Random Forest:: done")

        # Predicting the Test set results of knn
        print("Prediction using k-NN")
        y_predKN = classifierKN.predict(X_test)
        cmKN = confusion_matrix(y_test, y_predKN)
        print(accuracy_score(y_test, y_predKN))
        print(confusion_matrix(y_test, y_predKN))
        print("Prediction using k-NN:: done")

        # Predicting the Test set results of nb
        print("Prediction using Naive Bayes")
        y_predNB = classifierNB.predict(X_test)
        cmNB = confusion_matrix(y_test, y_predNB)
        print(accuracy_score(y_test, y_predNB))
        print(confusion_matrix(y_test, y_predNB))
        print("Prediction using Naive Bayes :: done")












