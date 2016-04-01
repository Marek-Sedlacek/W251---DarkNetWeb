#!/usr/bin/env python

# General libraries.
import os
from dateutil.parser import parse
import hashlib
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')

import string
import numpy as np
import pandas as pd

from elasticsearch import Elasticsearch

#Read in cleaned data
col_names_topic = ["Topic","Date","Author","Author_numposts","Author_type","Author_karma","Response_date",
                   "Responder","Responder_numposts","Responder_type","Responder_karma"]
col_names_profile = ["User","User_numposts","User_type","User_karma","Date_registered","Last_active"]

col_names_topic = ["Topic","Date","Author","Author_numposts","Author_type","Author_karma","Response_date",
                   "Responder","Responder_numposts","Responder_type","Responder_karma"]
col_names_profile = ["User","User_numposts","User_type","User_karma","Date_registered","Last_active"]

profile_file_name = 'df_profile_SR1_combined.csv'
topic_file_name = 'df_topic_SR1_combined.csv'

df_profile_combined = pd.read_csv(profile_file_name,header=0,names=col_names_profile)         
df_topic_combined = pd.read_csv(topic_file_name,header=0,names=col_names_topic)

profile_file_name2 = 'df_profile_SR2_combined.csv'
topic_file_name2 = 'df_topic_SR2_combined.csv'

df_profile_combined2 = pd.read_csv(profile_file_name2,header=0,names=col_names_profile)         
df_topic_combined2 = pd.read_csv(topic_file_name2,header=0,names=col_names_topic)

# for final project 
ES_HOST = {"host" : "158.85.217.74", "port" : 9200}

# for hw12
#ES_HOST = {"host" : "50.97.209.18", "port" : 9200}

# create ES client, create index
es = Elasticsearch(hosts = [ES_HOST], timeout=300)

INDEX_NAME = 'darknetmarket'

#Clears data in the current index before loading
#if es.indices.exists(INDEX_NAME):
#    print("deleting '%s' index..." % (INDEX_NAME))
#    res = es.indices.delete(index = INDEX_NAME)
#    print(" response: '%s'" % (res))

#since we are running locally, use one shard and no replicas
#request_body = {
#    "settings" : {
#        "number_of_shards": 1,
#        "number_of_replicas": 0
#    }
#}

#Creates index
#print("creating '%s' index..." % (INDEX_NAME))
#res = es.indices.create(index = INDEX_NAME, body = request_body)
#print(" response: '%s'" % (res))

def df_to_es(df, db_type, SR_type):
    c = 0
    if db_type == 'profile':
        #Kibana will not load rows with NaN values
        #Replace null strings with "Missing" and null numerics with -1
        df["User"] = df["User"].fillna("Missing")
        df["User_numposts"] = df["User_numposts"].fillna("Missing")
        df["User_type"] = df["User_type"].fillna("Missing")
        df["User_karma"] = df["User_karma"].fillna("Missing")
        df["Date_registered"] = df["Date_registered"].fillna("Missing")
        df["Last_active"] = df["Last_active"].fillna("Missing")

        TYPE_NAME = 'profile'
        ID_FIELD = 'user_date'
        
        bulk_data2 = []
        for index, row in df.iterrows():
            try:
                Date_registered = str(parse(row["Date_registered"]))
                Username = str(row["User"])
                Username = Username.encode('ascii', 'ignore')
                User_numposts = str(row["User_numposts"])
                User_type = str(row["User_type"])
                User_karma = str(row["User_karma"])
                Source = SR_type
                User_Date = hashlib.md5((Username + Date_registered).encode()).hexdigest()

                data_dict = {"user_date":User_Date,"username":Username,"user_numposts":User_numposts,
                       "user_type":User_type,"user_karma":User_karma,"date_registered":Date_registered,
                       "source":Source}

                #Creates a dictionary of Index, Type, and ID
                op_dict = {
                    "index": {
                        "_index": INDEX_NAME, 
                        "_type": TYPE_NAME, 
                        "_id": data_dict[ID_FIELD]
                    }
                }

                #Adds dictionaries to the bulk data load file
                bulk_data2.append(op_dict)
                bulk_data2.append(data_dict)

            except:
                print "error, skip row"
            c+=1
                
            if c%1000==0: print c

            if c%5000==0:
                # bulk index the data
                print("bulk indexing...")
                res = es.bulk(index = INDEX_NAME, body = bulk_data2, refresh = True, timeout=300)
                bulk_data2 = []

    elif db_type == 'topic': 
        #Kibana will not load rows with NaN values
        #Replace null strings with "Missing" and null numerics with -1
        df["Topic"] = df["Topic"].fillna("Missing")
        df["Date"] = df["Date"].fillna("Missing")
        df["Author"] = df["Author"].fillna("Missing")
        df["Author_type"] = df["Author_type"].fillna("Missing")
        df["Author_karma"] = df["Author_karma"].fillna("Missing")
        df["Response_date"] = df["Response_date"].fillna("Missing")
        df["Responder"] = df["Responder"].fillna("Missing")
        df["Responder_type"] = df["Responder_type"].fillna("Missing")
        df["Responder_karma"] = df["Responder_karma"].fillna("Missing")     
        df["Author_numposts"] = df["Author_numposts"].fillna(-1)
        df["Responder_numposts"] = df["Responder_numposts"].fillna(-1)

        TYPE_NAME = 'topic'
        ID_FIELD = 'author_date_responder_date'
        
        bulk_data2 = []
        for index, row in df.iterrows():
            try:
                Date = str(parse(row["Date"]))
                Topic = str(row["Topic"])
                Topic = Topic.encode('ascii', 'ignore')
                Author = str(row["Author"])
                Author = Author.encode('ascii', 'ignore')
                Author_numposts = str(row["Author_numposts"])
                Author_type = str(row["Author_type"])
                Author_karma = str(row["Author_karma"])
                Response_date = str(parse(row["Response_date"]))
                Responder = str(row["Responder"])
                Responder = Responder.encode('ascii', 'ignore')
                Responder_numposts = str(row["Responder_numposts"])
                Responder_type = str(row["Responder_type"])
                Responder_karma = str(row["Responder_karma"])
                Source = SR_type
                Author_Date_Responder_Date = hashlib.md5((Author+Date+Responder+Response_date)
                                                         .encode()).hexdigest()

                data_dict = {"author_date_responder_date":Author_Date_Responder_Date,"topic":Topic,"date":Date,
                       "author":Author,"author_numposts":Author_numposts,"author_type":Author_type,
                       "author_karma":Author_karma,"response_date":Response_date,"responder":Responder,
                       "responder_numposts":Responder_numposts,"responder_type":Responder_type,
                       "responder_karma":Responder_karma,"source":Source}

                #Creates a dictionary of Index, Type, and ID
                op_dict = {
                    "index": {
                        "_index": INDEX_NAME, 
                        "_type": TYPE_NAME, 
                        "_id": data_dict[ID_FIELD]
                    }
                }
                
                #Adds dictionaries to the bulk data load file
                bulk_data2.append(op_dict)
                bulk_data2.append(data_dict)                
                
            except:
                print "error, skip row"
            
            c+=1
                
            if c%1000==0: print c

            if c%5000==0:
                # bulk index the data
                print("bulk indexing...")
                res = es.bulk(index = INDEX_NAME, body = bulk_data2, refresh = True, timeout=300)
                bulk_data2 = []

    res = es.bulk(index = INDEX_NAME, body = bulk_data2, refresh = True, timeout=300)
    bulk_data2 = []

print "starting"
df_to_es(df_profile_combined, "profile", "Silk_Road_1")
print "SR1 profile complete"
df_to_es(df_topic_combined, "topic", "Silk_Road_1")
print "SR1 topic complete"

df_to_es(df_profile_combined2, "profile", "Silk_Road_2")
print "SR2 profile complete"
df_to_es(df_topic_combined2, "topic", "Silk_Road_2")
print "SR2 topic complete"
