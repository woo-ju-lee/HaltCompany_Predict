import tabpfn_client
from tabpfn_client import TabPFNClassifier, TabPFNRegressor, set_access_token
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, log_loss
import pandas as pd
import numpy as np

API_TOKEN = "MY_API"
tabpfn_client.set_access_token(API_TOKEN)

X_train = r.bsmote_train
y_train = r.train_halt
X_test = r.x_test
y_test = r.test_halt

y_train = np.asarray(y_train).ravel()
y_test = np.asarray(y_test).ravel()

model = TabPFNClassifier(n_estimators=8, balance_probabilities=True, softmax_temperature=0.8)

model.fit(X_train, y_train)

predictions = model.predict(X_test)

print("Accuracy:", accuracy_score(y_test, predictions))

probs = model.predict_proba(X_test)
print("Log-Loss:", log_loss(y_test, probs))
