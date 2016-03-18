from bs4 import BeautifulSoup
import datetime
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

	#Explore the Users listings first
	os.chdir(os.getcwd()+"/"+X+"/users")
	#Loops through all User folders
	for x in next(os.walk(os.getcwd()))[1]:

		os.chdir(os.getcwd()+"/"+x)
		
		for filename in os.listdir(os.getcwd()):
			try:
				soup = BeautifulSoup(open(filename,'r').read()) #Reads in the HTML file
				body = soup.find("div",attrs={"class":"body"} ) #Body section of the HTML
				items = body.find_all("div",attrs={"class":"item"}) #Item section of the body

				#Loops through all item listings in the HTML body
				for i in range(1,len(items)):
					try: 
						title = items[i].find("div",attrs={"class":"item_title"}).find("a").get_text() #Get Title
						title = title.encode('ascii', 'ignore')
						vendor = x.encode('ascii', 'ignore') #Vendor is the folder name
						try: price = float(non_decimal.sub("",items[i].find("div",attrs={"class":"price_big"}).get_text())) #Get Price
						except: price = float(non_decimal.sub("",items[i].find("div",attrs={"class":"price"}).get_text())) #Get Price Alternate
						price_dollar = price*curr_rate #Converts bitcoin price to Dollars (price from 2014-06-02)
						Ship_From = items[i].find("div",attrs={"class":"item_details"}).get_text().splitlines()[2].replace("ships from: ","").strip()
						Ship_To = items[i].find("div",attrs={"class":"item_details"}).get_text().splitlines()[3].replace("ships to: ","").strip()
						Category = "" #No Categories in this data
						#Title_Date = title + str(curr_date)
						Title_Date = hashlib.md5((title + vendor + str(curr_date)).encode()).hexdigest()
						
						#Dictionary, for when we migrate to Cassandra
						row = {"title_date":Title_Date,"date":curr_date,"title":title,"category":Category,"price":price,"price_dollar":price_dollar,"vendor":vendor,"market":"Silk Road 2","ships_from":Ship_From,"ships_to":Ship_To}
						key = (row["title_date"]) #Creates a composite key of Title and Date
						cf.insert(key,row) #Inserts the new row into the Cassandra table
						
						#Creates a numpy array with the scraped values
						new = np.array([Title_Date,title,Category,price,price_dollar,vendor,Ship_From,Ship_To,curr_date,x])
						#Appends array to data frame
						df = df.append(pd.DataFrame(new, index=col_names).transpose())

						c+=1 #Increments counter
						if c%1000==0: print c #Tracking metric

					except: print "User " + x + " has failed"
			except: print "User " + x + " has failed"
		os.chdir(wdir+"/"+X+"/users") #Reset to User directory
			
	os.chdir(wdir) #Reset current working directory

	
print "Files successfully read: %s" %c

#Remove duplicates from "Feedback" pages
df = df.drop_duplicates(subset=['Vendor',"Title","Date"],keep="first")
#Re-indexes the dataframe
df=df.sort_values(by=["Vendor","Title"])
df.index = range(df.shape[0])
#Writes dataframe to csv
df.to_csv("SilkRoadProducts_U.csv",sep=',',encoding='utf-8')