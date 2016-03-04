from bs4 import BeautifulSoup
import datetime
import numpy as np
import pandas as pd
import re
import requests
import codecs
import os

wdir = "C:/Users/MarekSedlacek/Downloads/W251_Project/SilkRoad2/"


#ACTUAL CODE FOR PULLING PRODUCT DATA

#Ensure the current directory is correctly set
os.chdir(wdir)
#Build a Pandas data frame to hold the new data
col_names = ["Title","Category","Price","Price Dollar","Vendor","Ships From", "Ships To", "Date"]
df = pd.DataFrame(columns = col_names)

c = 0 #Counter
non_decimal = re.compile(r'[^\d.]+')

#Loops through the directories (scraped dates)
for X in next(os.walk(os.getcwd()))[1]:
    #Sets the working directory to the item folder for the current date
    os.chdir(os.getcwd()+"\\"+X+"\items")
    curr_date = datetime.datetime(*[int(item) for item in X.split('-')])
    print os.getcwd()
    
    
    for i in os.listdir(os.getcwd()): #Loops through all files in the current directory
        try:
            if i.endswith(".html"): #Only reads HTML files
                c+=1
                
                soup = BeautifulSoup(open(i,'r').read()) #Reads in the HTML file

                
                sidebar = soup.find("div",attrs={"class":"sidebar"}) #Identifies the Sidebar section of the HTML doc
                Category = sidebar.find("a").get_text() #Pulls Category from the Sidebar

                body = soup.find("div",attrs={"class":"body"} )#Identifies the Body section of the HTML doc
                title = body.find("h2").get_text() #Pulls product title
                price = float(non_decimal.sub("",body.find("div",attrs={"class":"price_big"}).get_text())) #Pulls product price
                price_dollar = price*658.79 #Converts bitcoin price to Dollars (price from 2014-06-02)
                vendor = body.find("a").get_text() #Pulls Vendor
                
                #Identifies Shipping To and From out of a block of text
                S1 = body.find_all("p")
                if S1[0].get_text()=="":
                    S1 = S1[1].get_text().replace("ships from: ","").replace("ships to: ","")
                else: S1 = S1[0].get_text().replace("ships from: ","").replace("ships to: ","")
                Ship_From = S1.splitlines()[1]
                Ship_To = S1.splitlines()[2]
                
                #Creates a numpy array with the scraped values
                new = np.array([title,Category,price,price_dollar,vendor,Ship_From,Ship_To,curr_date])
                #Appends array to data frame
                df = df.append(pd.DataFrame(new, index=col_names).transpose())
                
                #Dictionary, for when we migrate to Cassandra
                row = {"Date":curr_date,"Title":title,"Category":Category,"Price":price,"price_dollar":price_dollar,"Vendor":vendor,"Ships_From":Ship_From,"Ships_To":Ship_To}

                #if c%100==0: print c #Tracking metric

            else:
                continue

        except:
            print "File " + i + " has failed"
            
    os.chdir(wdir) #Reset current working directory
	

#Remove duplicates from "Feedback" pages
df = df.drop_duplicates(subset=['Vendor',"Title","Date"],keep="first")

#Re-indexes the dataframe
df.index = range(df.shape[0])

df
        