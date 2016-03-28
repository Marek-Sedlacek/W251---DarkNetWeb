import string
import pandas as pd
import numpy as np
import datetime
import re

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.protocol import NumpyProtocolHandler, LazyProtocolHandler
from cassandra.query import dict_factory

cluster = Cluster(['158.85.217.74'],control_connection_timeout=None)
session = cluster.connect('dnm')
session.default_timeout = None
session.row_factory = dict_factory

Drugs = pd.read_csv("Drug Categories.csv")

####DATA EXTRACTION FUNCTIONS####

#Find_weight searches the title of a product for a numerical weight and returns if found.
def find_weight(title):
    #Keywords for weight
    words = [" g ","mg", "kg", "ug", "grams", "gr.","g", "gr", "gram", "g.", "gramm", "oz","ounce"]
    
    #Basic cleaning
    s = title.lower() #Set all characters to lowercase
    s= re.sub("[()]","",s) #Remove certain punctuation
    s=s.replace("***"," ") #Replace *** symbol with whitespace
    s=s.replace("..."," ") #Replace *** symbol with whitespace
    s = s.split() #Split the title into tokens
    
    #Searches the title for any of the keywrods
    for word in words:
        #Loops through each token in the title
        for i in range(0,len(s)):
            #If the token equals the keyword:
            if s[i]==word:
                #Try to convert the preceding token into a float (ex. 20 grams)
                try:
                    #Converts all weights to grams
                    if word in ["mg"]: return float(s[i-1].split("-",1)[0])/1000
                    elif word in ["kg"]: return float(s[i-1].split("-",1)[0])*1000
                    elif word in ["ug"]: return float(s[i-1].split("-",1)[0])/1000000
                    elif word in ["oz","ounce"]: return float(s[i-1].split("-",1)[0])*28.3495
                    else: return float(s[i-1].split("-",1)[0])
                #If the preceding token is not a number, continue
                except: continue
            #If the token contains the keyword:
            elif word in s[i]:
                #Try to convert the current token (with the keyword removed) into a float (ex. 200mg)
                try:
                    if word in ["mg"]: return float(s[i].replace(word,"").split("-",1)[0])/1000
                    elif word in ["kg"]: return float(s[i].replace(word,"").split("-",1)[0])*1000
                    elif word in ["ug"]: return float(s[i].replace(word,"").split("-",1)[0])/1000000
                    elif word in ["oz","ounce"]: return float(s[i-1].split("-",1)[0])*28.3495
                    else: return float(s[i].replace(word,"").split("-",1)[0])
                #If the current token is not a number, continue
                except: continue
    #Returns None if no weight can be found
    return None 

#Find_count searches the title of a product for a numerical count and returns if found.
def find_count(title):
    #Keywords for count
    words = ["x", "tabs", "tablets", "capsules", "pills"]
    
    #Basic cleaning
    s = title.lower() #Set all characters to lowercase
    s= re.sub("[()]","",s) #Remove certain punctuation
    s=s.replace("***"," ") #Replace *** symbol with whitespace
    s = s.split() #Split the title into tokens
    
    #Searches the title for any of the keywrods
    for word in words:
        #Loops through each token in the title
        for i in range(0,len(s)):
            #If the token equals the keyword:
            if s[i]==word:
                #Try to convert the preceding token into a float (ex. 20 x pills)
                try: return float(s[i-1].split("-",1)[0])
                #Else to convert the price token into a float (ex. pills x 20)
                except:
                    try: return float(s[i+1].split("-",1)[0])
                    except: continue
            #If the token contains the keyword:
            elif word in s[i]:
                #Try to convert the current token (with the keyword removed) into a float
                try: return float(s[i].replace(word,"").split("-",1)[0])
                except: continue
    
    #If the keyword search fails, try returning the first number found that does not equal the weight.
    for i in range(0,len(s)):
        if s[i].isdigit() and float(s[i])<>find_weight(title): 
            try: return float(s[i])
            except: continue
    
    #If no number can be found, return a count of 1
    return 1 

#Find_category takes a product entry and classifies it into a Category
def find_category(row):
    #Converts the product title to lowercase
    y = str(row["title"]).lower()
    #Loops through a lookup table of known types and their categories
    for i in range(0,len(Drugs["Lookup"])):
        word = Drugs["Lookup"][i] #Pulls the lookup keyword
        word = word.lower() #Converts to lowercase
        
        #If the Lookup word is in the product title, return the Category and Subcategory associated with it
        if word in y: return (Drugs["Subcategory"][i], Drugs["Category"][i])
    
    #Some products have a populated Category, but the classifications are not standard.
    #Some are very specific (equivalent to the title) while other are too generic.
    #Converts product category to lowercase
    y = str(row["category"]).lower()
    #Loops through the same lookup table
    for i in range(0,len(Drugs["Lookup"])): 
        word = Drugs["Lookup"][i]
        word = word.lower()
        #If the Lookup word is in the product category, return the Category and Subcategory associated with it
        if word in y: return (Drugs["Subcategory"][i], Drugs["Category"][i])
        
    #If none of the lookup values are in the Title or the Category, use the original Category as Cat and SubCat
    orig_cat =  row["category"].decode('utf-8','ignore').encode("utf-8")
    return (orig_cat,orig_cat)

def clean_text(row):
    return row.decode('unicode_escape').encode('ascii', 'ignore')
	
if __name__ == "__main__":
	#Pull in data from Cassandra
	result = session.execute("select * from products LIMIT 400000 ")

	start_time = datetime.datetime.now() #Record start time
	df = pd.DataFrame()
	df_temp = pd.DataFrame()
	for i in result:    
		df_temp = df_temp.from_dict(i,orient="index").transpose() #Convert the dictionary from Cassandra to a Dataframe
		df = pd.concat([df,df_temp],axis=0) #Join the row to the total dataframe
	print datetime.datetime.now()-start_time #Print run time
	
	df = df.reset_index(drop=True) #Reset the index
	new=df
	
	new["title"] = new["title"].str.strip(' ')
	new["category"] = new["category"].str.strip(' ')
	new["vendor"] = new["vendor"].str.strip(' ')
	new["ships_from"] = new["ships_from"].str.strip(' ')
	new["ships_to"] = new["ships_to"].str.strip(' ')

	x,y,a,b = [],[],[],[]

	#For each product in the raw data
	for i in range(0, new.shape[0]):
		x.append(find_weight(str(new.iloc[i,]["title"]))) #Search for weight
		y.append(find_count(str(new.iloc[i,]["title"])))  #Search for count
		c,d = find_category(new.iloc[i,])                 #Search for Category and SubCategory
		a.append(c)
		b.append(d)
		if i%1000==0 and i>0: print i
		
	new["Weight"]=pd.DataFrame(x)  #Create "Weight" field in data with results of find_weight()
	new["Count"]=pd.DataFrame(y)   #Create "Count" field in data with results of find_count()
	new["SubCat2"]=pd.DataFrame(a) #Create "SubCat2" field in data with results of find_category()
	new["Cat2"]=pd.DataFrame(b)    #Create "Cat2" field in data with results of find_category()
	
	df['SubCat2'] = df['SubCat2'].apply(clean_text)
	
	new.to_csv("cleaned_output2.csv",",",index=False)

	#Create new price metrics
	new["price_dollar"]=pd.to_numeric(new["price_dollar"],errors='coerce') #Converts price_dollar to numeric 
	new["PPW"] = new["price_dollar"]/new["Weight"] #Price per weight (grams)
	new["PPWC"] = new["PPW"]/new["Count"] #Price per weight, controlling for count

	#Export new data to csv file
	new.to_csv("cleaned_output2.csv",",",index=False)
	
	#Write query statment for adding data into Cassandra
	insert_user = session.prepare("INSERT INTO products_clean (title_date,cat2,category,counts,date,market,ppw,ppwc,price,price_dollar,ships_from,ships_to,subcat2,title,weight,vendor) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	#Create new Batch_Statment
	batch = BatchStatement()

	#Loop through every record in the dataframe
	for i in range(0,df.shape[0]):
		#Add the record to the Batch Update
		batch.add(insert_user, (new["title_date"][i],new["Cat2"][i],new["category"][i],new["Count"][i],str(new["date"][i]),new["market"][i],new["PPW"][i],new["PPWC"][i],new["price"][i],new["price_dollar"][i], new["ships_from"][i],new["ships_to"][i],new["SubCat2"][i],new["title"][i],new["Weight"][i],new["vendor"][i]))
	
		#If the Batch Update contains 50,000 records, execute Batch Statement
		if i%50000==0:
			start_time = datetime.datetime.now()
			session.execute(batch)
			print datetime.datetime.now()-start_time #Print runtime
			batch = BatchStatement() #Reset BatchStatement
        
	#Run final Batch Update
	start_time = datetime.datetime.now()
	session.execute(batch)
	print datetime.datetime.now()-start_time
	
	
	
	
	
	