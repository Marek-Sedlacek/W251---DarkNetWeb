## W251---DarkNetWeb
#### Data Source
http://www.gwern.net/Black-market%20archives#download

https://archive.org/download/dnmarchives

#### Loading
Scripts for building a Cassandra DB, extracting data from the raw HTML files, and loading the resulting data to Cassandra

#### Cleaning
Scripts to clean some of the data extracted from the HTML files. This includes categorizing products with a machine learning algorithm and extracting additional features out of text description fields.

#### Analysis
Exploration and analysis of the data. Part of the analysis was done within Spark, but the data was also loaded into Elasticsearch so it could be visually explored using Kibana.

#### Other Files
###### Bitcoin Prices.csv
Daily historical prices of bitcoin in US dollars. Used to convert the prices of listings.

###### Drug Categories.csv
Lookup table to help classify listings into categories and subcategories.
