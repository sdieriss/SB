import requests
import json
import math
from datetime import date
import pandas as pd
import os
import logging

 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = '%(asctime)s %(filename)s: %(message)s'
logging.basicConfig(filename="errors.log", format=log_format)


# Get the count of the complete dataset
#dataset_count = 'https://data.cityofnewyork.us/resource/nc67-uf89.json?$$app_token=MBDIGmHgRvStyouz0IwfEaXFr&$select=count(*)'

try: 
    # Get the count of the dataset for today
    the_date = " '"+str(date.today())+"'"
    dataset_count = "https://data.cityofnewyork.us/resource/nc67-uf89.json?$$app_token=MBDIGmHgRvStyouz0IwfEaXFr&$where=:updated_at >" + the_date + "&$select=count(*)"
    r = requests.get(dataset_count)

    # Print the text of the response
    #print(r.text)


    the_count_json = json.loads(r.text)
    the_count = the_count_json[0].get("count")

    #set number or records returned to 10000
    limit = 10000
    num_of_iterations = int(the_count)/limit
    offset_val = 0
    df = pd.DataFrame()

    #Statement to retrieve all columns
    stmt = "&$select=*"


    #Retrieve all records for today in batches
    for batch_num in range(math.ceil(num_of_iterations)):
        print("Processing batch {}".format(batch_num))
        batch ="&$limit={}&$offset={}&$order=issue_date".format(limit,offset_val)
        url = "https://data.cityofnewyork.us/resource/nc67-uf89.json?$$app_token=MBDIGmHgRvStyouz0IwfEaXFr&$where=:updated_at >" + the_date + stmt + batch
        #print("This is the url {}".format(url))
        res_batch = requests.get(url)
        df = df.append(pd.DataFrame(res_batch.json()))
        offset_val+=limit
       
    #print("The current shape {}".format(df.shape))
    print("The total amount of records {} ".format(the_count))

except:
    print("Error retrieving records") 
    logging.error("Error retrieving records", exc_info=True)



#Store file locally
filename =  "records_" + date.today().strftime("%m_%d_%Y") + ".xlsx"
try:
    df.to_excel(filename)
    print("The file {} is stored at {}".format(filename, os.getcwd()))
except:
    print("Error writing file") 
    logging.error("Error writing records file", exc_info=True)








