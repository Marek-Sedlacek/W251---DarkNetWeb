from bs4 import BeautifulSoup
import datetime
from time import strftime
import hashlib
import numpy as np
import pandas as pd
import csv
import re
import os

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
pool = ConnectionPool('dnm', ['158.85.217.74:9160'])  #Needs to be in the format of '169.53.141.8:9160'
cf = ColumnFamily(pool, 'products')

wdir = "/sandisk1/darknetmarket/silkroad2"

#Ensure the current directory is correctly set
os.chdir(wdir)

c = 0 #Counter
non_decimal = re.compile(r'[^\d.]+') #Clean strings with numbers

#####CODE TO READ IN CSV OF BITCOIN PRICES GOES HERE#####
bitcoin = pd.read_csv('/sandisk1/darknetmarket/Bitcoin Prices.csv',sep=',')#Reads in historical bitcoin prices
bitcoin['Date'] = pd.to_datetime(bitcoin['Date']) #Converts 'Date' field to Datetime
#########################################################

col_names = ["Key","Title","Category","Price","Price Dollar","Vendor","Ships From", "Ships To", "Date","File"]
df = pd.DataFrame(columns = col_names)

#Loops through the directories, corresponding to Dates
for X in next(os.walk(os.getcwd()))[1]:
	print X	#Prints the current date folder
	#Sets the working directory to the item folder for the current date
	curr_date = datetime.datetime(*[int(item) for item in X.split('-')])
    
	#####CODE TO SET BITCOIN EXCHANGE RATE GOES HERE#####
	curr_rate = bitcoin.loc[bitcoin['Date'] == curr_date].iloc[0]['Close Price']
	#####################################################

	#Explore the Items listing next
	#This is done so any duplicates will overwrite the more sparse information in the Users data
	os.chdir(os.getcwd()+"/"+X+"/items")    
	#Loops through all files in the Items directory
	for i in os.listdir(os.getcwd()):
		try:
			soup = BeautifulSoup(open(i,'r').read()) #Reads in the HTML file
			sidebar = soup.find("div",attrs={"class":"sidebar"}) #Sidebar section of the HTML
			body = soup.find("div",attrs={"class":"body"} )#Body section of the HTML
			
			title = body.find("h2").get_text() #Pulls product title
			title = title.encode('ascii', 'ignore')
			price = float(non_decimal.sub("",body.find("div",attrs={"class":"price_big"}).get_text())) #Pulls product price
			vendor = body.find("a").get_text() #Pulls Vendor
			Category = sidebar.find("a").get_text() #Pulls Category from the Sidebar
			price_dollar = price*curr_rate #Converts bitcoin price to Dollars
			
			#Identifies Shipping To and From out of a block of text
			S1 = body.find_all("p")
			if S1[0].get_text()=="":
				S1 = S1[1].get_text().replace("ships from: ","").replace("ships to: ","")
			else: S1 = S1[0].get_text().replace("ships from: ","").replace("ships to: ","")
			Ship_From = S1.splitlines()[1].strip()
			Ship_To = S1.splitlines()[2].strip()
			#Title_Date = title + vendor + str(curr_date)
			Title_Date = hashlib.md5((title + vendor + str(curr_date)).encode()).hexdigest()
			
			#Dictionary, for when we migrate to Cassandra
			row = {"title_date":Title_Date,"date":curr_date,"title":title,"category":Category,"price":price,"price_dollar":price_dollar,"vendor":vendor,"market":"Silk Road 2","ships_from":Ship_From,"ships_to":Ship_To}
			key = (row["title_date"]) #Creates a composite key of Title and Date
			cf.insert(key,row) #Inserts the new row into the Cassandra table
			
			#Creates a numpy array with the scraped values
			new = np.array([Title_Date,title,Category,price,price_dollar,vendor,Ship_From,Ship_To,curr_date,i])
			#Appends array to data frame
			df = df.append(pd.DataFrame(new, index=col_names).transpose())


			c+=1 #Increments counter
			if c%1000==0: print str(c) + ", " + datetime.datetime.now().time().strftime("%H:%M:%S") #Tracking Metric
			
		except: print "File " + i + " has failed"
			
	os.chdir(wdir) #Reset current working directory
		
print "Files successfully read: %s" %c

#Remove duplicates from "Feedback" pages
df = df.drop_duplicates(subset=['Vendor',"Title","Date"],keep="first")
#Re-indexes the dataframe
df=df.sort_values(by=["Vendor","Title"])
df.index = range(df.shape[0])
#Writes dataframe to csv
df.to_csv("Test_Load.csv",sep=',',encoding='utf-8')