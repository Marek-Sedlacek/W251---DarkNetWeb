# General libraries.
import re
import numpy as np
import pandas as pd

# SK-learn libraries for learning.
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import BernoulliNB
from sklearn.naive_bayes import MultinomialNB

# SK-learn libraries for evaluation.
from sklearn.metrics import confusion_matrix
from sklearn import metrics
from sklearn.metrics import classification_report

# SK-learn libraries for feature extraction from text.
from sklearn.feature_extraction.text import *



if __name__ == "__main__":

	###BUILD CATEGORY MODEL FIRST###
	
	#Load cleaned data
	data = pd.read_csv("cleaned_output.csv")	
	data2 = data[pd.isnull(data["Cat2"])==False]	#Remove rows with missing labels from train
	data2 = data2[pd.isnull(data["title"])==False] 	#Remove rows with missing titles from train

	#Group into Top 10 Categories or Other
	categories = ["Stimulants", "Ecstasy", "Cannabis", "Psychadelics", "Relaxants", "Opioids","Sildenafil Citrate","Dissociatives","Analgesics","Steroids/PEDs"]
	data2["Cat2"] = [x if x in categories else "Other" for x in data2["Cat2"]]

	
	features = data2["title"].as_matrix() #Set features to title text
	labels = data2["Cat2"].as_matrix() #Set labels as category

	#Split the data into train and development
	num_test = len(data2)
	dev_features, dev_labels = features[:num_test/4], labels[:num_test/4]
	train_features, train_labels = features[num_test/4:], labels[num_test/4:]
	
	#Initialize Count Vectorizer on training data
	vectorizer = CountVectorizer()
	x=vectorizer.fit_transform(train_features)
	
	#Build Multinomial Naive Bayes model
	Mult = MultinomialNB(alpha=0.000001)
	Mult.fit(vectorizer.transform(train_features), train_labels) 
	
	#Make predicitions on dev dataset and print accuracy
	preds = Mult.predict(vectorizer.transform(dev_features))
	print "F1 score is %f" %(metrics.f1_score(dev_labels,preds,average="weighted"))
	
	#Make predictions on entire dataset. Some rows will not have a category to compare accuracy	
	data_pred = data["title"].fillna("").as_matrix()
	data["Pred_Cat"] = Mult.predict(vectorizer.transform(data_pred))
	
	
	
	###BUILD SUBCATEGORY MODEL SECOND###
	
	#Load cleaned data	
	data2 = data[pd.isnull(data["SubCat2"])==False]	#Remove rows with missing labels from train
	data2 = data2[pd.isnull(data["title"])==False] 	#Remove rows with missing titles from train

	#Group into Top 10 Categories or Other
	subcategories = ["MDMA", "Weed", "Cocaine", "LSD", "Benzos", "NBOMe","Speed","Amphetamine","Heroin","2C","Xanax","DMT",
                 "Stimulants","Drug paraphernalia","Methylone","Testosterone","Valium","Tramadol","Shrooms"]
	data2["SubCat2"] = [x if x in subcategories else "Other" for x in data2["SubCat2"]]
	
	features = data2["title"].as_matrix() #Set features to title text
	labels = data2["Cat2"].as_matrix() #Set labels as subcategory

	#Split the data into train and development
	num_test = len(data2)
	dev_features, dev_labels = features[:num_test/4], labels[:num_test/4]
	train_features, train_labels = features[num_test/4:], labels[num_test/4:]
	
	#Initialize Count Vectorizer on training data
	vectorizer = CountVectorizer()
	x=vectorizer.fit_transform(train_features)
	
	#Build Multinomial Naive Bayes model
	Mult_Sub = MultinomialNB(alpha=0.000001)
	Mult_Sub.fit(vectorizer.transform(train_features), train_labels) 
	
	#Make predicitions on dev dataset and print accuracy
	preds = Mult_Sub.predict(vectorizer.transform(dev_features))
	print "F1 score is %f" %(metrics.f1_score(dev_labels,preds,average="weighted"))
	
	#Make predictions on entire dataset. Some rows will not have a category to compare accuracy	
	data_pred = data["title"].fillna("").as_matrix()
	data["Pred_SubCat"] = Mult.predict(vectorizer.transform(data_pred))
	
	#Output csv
	data.to_csv("modeled_subcategories.csv",",",index=False)
	
	
	
	
	
	
	
	
	
