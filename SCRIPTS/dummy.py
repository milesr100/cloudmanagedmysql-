import dbm
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker
import uuid
import random
from dotenv import load_dotenv

load_dotenv()

GCP_MYSQL_HOSTNAME = os.getenv("GCP_MYSQL_HOSTNAME")
GCP_MYSQL_USER = os.getenv("GCP_MYSQL_USERNAME")
GCP_MYSQL_PASSWORD = os.getenv("GCP_MYSQL_PASSWORD")
GCP_MYSQL_DATABASE = os.getenv("GCP_MYSQL_DATABASE")



fake_patients = [
    {
    
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number()
    } for x in range(25)]


df_fake_patients = pd.DataFrame(fake_patients)
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])





icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort_1k = icd10codesShort.sample(n=1000)


ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)


insertQuery = "INSERT INTO production_medications (med_ndc, med_name) VALUES (%s, %s)"

medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    db_gcp.execute(insertQuery, (row['MEDCORE'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    if medRowCount == 100:
        break

df_conditions = pd.read_sql_query("SELECT icd10_code FROM production_conditions", db_gcp)
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", db_gcp)

df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
for index, row in df_patients.iterrows():
numConditions = random.randint(1, 5)
df_conditions_sample = df_conditions.sample(n=numConditions)
df_conditions_sample['mrn'] = row['mrn']
df_patient_conditions = df_patient_conditions.append(df_conditions_sample)
print(df_patient_conditions)


cptcodes = pd.read_csv('https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv')
list(cptcodes.columns)
cptcodesShort = cptcodes[['com.medigy.persist.reference.type.clincial.CPT.code', 'label']]
cptcodesShort_1k = cptcodesShort.sample(n=1000, random_state=1)


