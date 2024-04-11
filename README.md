# ETL-BASICS-WITH-PYTHON
This is a practice exercise in the python project for data engineering on etl operations

In this exercise on etl operations, the data in the link below was downloaded and unzipped.
https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip
The data was then extracted,it contained multiple csv, json, and xml files.
An extraction function was implemented to extract all data in the files
The data contained four headers: 'car_model', 'year_of_manufacture', 'price', 'fuel'.
The values under the 'price' header were transformed by rounding to 2 decimal places
A loading function for the transformed data to a target file 'transformed_data.csv' was implemented
A logging function for the entire process was implemented and saved in 'log_file.txt'
