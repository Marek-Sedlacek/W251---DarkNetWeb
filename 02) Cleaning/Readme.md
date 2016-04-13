## Cleaning Data

#### 01) Additional Feature Extraction
The initial data load obtained features by identifying tags in the HTML files and pulling the text within those tags, two of which were Title and Category. This step of the process looks for additional attributes implicit in the text of the Title and Category of the product.

#### 02) Category Model
Many of the products were missing the Category field. Even with a keyword search within the Title field, there were still a high number of uncategorized products. We trained a Naive Bayes model using tokenized text from the Title field to predict the Category of a product. We limited the scope to only the top 10 Categories for simplicity. We also built the model to run in a local or Spark environment.
