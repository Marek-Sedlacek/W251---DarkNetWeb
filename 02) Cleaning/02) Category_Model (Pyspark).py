from __future__ import print_function
# General libraries.
import string
import numpy as np
import pandas as pd
from sklearn import preprocessing

#NLTK Libraries
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
#nltk.download('punkt')

#Spark MLLib libraries
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF, IDF, LabeledPoint
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel

#Declare Spark context
#conf = SparkConf().setAppName("Category Learning Model").setMaster("spark://158.85.217.74:7077")
sc = SparkContext(appName = "Category Learning Model")
#sqlContext = SQLContext(sc)

# Module-level global variables for the `tokenize` function below
PUNCTUATION = set(string.punctuation)
#STOPWORDS = set(stopwords.words('english'))
STEMMER = PorterStemmer()

# Function to break text into "tokens", lowercase them, remove punctuation and stopwords, and stem them
def tokenize(text):
    tokens = word_tokenize(text)
    lowercased = [t.lower() for t in tokens]
    #no_punctuation = []
    #for word in lowercased:
    #    punct_removed = ''.join([letter for letter in word if not letter in PUNCTUATION])
    #    no_punctuation.append(punct_removed)
    #no_stopwords = [w for w in no_punctuation if not w in STOPWORDS]
    stemmed = [STEMMER.stem(w) for w in lowercased]
    return [w for w in stemmed if w]

#Split data into target and predictor variables
def parsePoint(data):
	labels = float(data[0])
	features = tf.transform(data[1])	
	return LabeledPoint(labels,features)

#Too many categories to predict them all. With more time, we could clean and generalize the data better.
#For now, we will predict the top 10 categories.
#All other categories will be classified as "Other"
categories = ["Stimulants", "Ecstasy", "Cannabis", "Psychadelics", "Relaxants", "Opioids", "Sildenafil Citrate","Dissociatives","Analgesics","Steroids / PEDs"]

#Read in data
all = pd.read_csv("cleaned_output_lim.csv")
#Training data includes only those with a labeled Category
train = all[pd.isnull(all["Cat2"])==False]

#Selects only the Cat2 (target) and Title (predictor) columns
train = train[['Cat2','title']]
#all = all[['title']]

#Reclassifies categories not in the top 10 as "Other"
train["Cat2"] = [x if x in categories else "Other" for x in train["Cat2"]]

le = preprocessing.LabelEncoder()
le.fit(["Stimulants", "Ecstasy", "Cannabis", "Psychadelics", "Relaxants", "Opioids", "Sildenafil Citrate","Dissociatives","Analgesics","Steroids / PEDs","Other"])
train["Cat2"] = le.transform(train["Cat2"]).astype("int")



#Exports data to csv
train.to_csv("product_train.csv",",",index=False)
#all.to_csv("product_all.csv",",",index=False)


# Read the training data file created above into an RDD
train = sc.textFile( "product_train.csv" ).map(lambda line: (line.split(',')))
header = train.first() #extract header
train2 = train.filter(lambda x:x !=header)

train_title = sc.textFile( "product_train.csv" ).map(lambda line: (line.split(',')[1]))

hashingTF = HashingTF(50000)
tf = train_title.map(lambda title: hashingTF.transform(title.split(" ")))
tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)
print(tfidf.first())

data_pared = train2.map(lambda line: (line[0], line[1]))
data_pared2 = train2.map(lambda line: (line[0]))
train_cleaned = data_pared.map(lambda (label, text): (label, tokenize(text)))
#parsedData = train_cleaned.map(lambda (label,text): LabeledPoint(label, idf.transform(text)))
parsedData = train_cleaned.map(lambda (label, text): LabeledPoint(label, hashingTF.transform(text)))


# Split the data into two RDDs. 70% for training and 30% test data sets
( trainingData, testData ) = parsedData.randomSplit( [0.7, 0.3] )

# Build a Naive Bayes model on the training data
model = NaiveBayes.train(trainingData, 0.000001)

# Make prediction and test accuracy.
predictionAndLabel = testData.map(lambda p: (model.predict(p.features), p.label))
accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / testData.count()
print( "Accuracy: " + str( accuracy ) )

sc.stop()

