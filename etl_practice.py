import requests
import zipfile
from zipfile import ZipFile
import io
from io import BytesIO

url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip'
r=requests.get(url)
#r.status_code
filename = url.split('/')[-1]
with open(filename, 'wb') as f:
    f.write(r.content)
#print(filename)
with ZipFile(filename,'r') as extract_source:
    extract_source.extractall(path=r'C:\Users\user\Documents\IBM Data Engineering\Python Project for Data Engineering\ETL PRACTICE\data_source')

import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'log_file.txt'
target_file = 'transformed_data.csv'

#extracting files
def extract_from_csvs(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_jsons(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

def extract_from_xmls(file_to_process):
    dataframe = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find('car_model').text
        year_of_manufacture = car.find('year_of_manufacture').text
        price = float(car.find('price').text)
        fuel = car.find('fuel').text
        dataframe = pd.concat([dataframe, pd.DataFrame([{'car_model':car_model, 'year_of_manufacture':year_of_manufacture, 'price':price, 'fuel':fuel}])], ignore_index=True)
    return dataframe

def extract_files():
    path = 'C:/Users/user/Documents/IBM Data Engineering/Python Project for Data Engineering/ETL PRACTICE/data_source'
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    #for csv files
    for csvfile in glob.glob(path + '/*.csv'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csvs(csvfile))], ignore_index=True)
    #for json files
    for jsonfile in glob.glob(path + '/*.json'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_jsons(jsonfile))], ignore_index=True)
    #for xml files
    for xmlfile in glob.glob(path + '/*.xml'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xmls(xmlfile))], ignore_index=True)
    return extracted_data

def transform(data):
    #rounding the values in price to 2dp
    data['price'] = round(data.price, 2)
    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + ',' + message + '\n')

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
  
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract_files() 
  
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
  
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
  
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
  
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
  
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
