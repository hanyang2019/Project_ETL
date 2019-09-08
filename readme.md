![H1B](https://images.techhive.com/images/article/2016/12/h-1b-visa9-100698034-large.jpg)
# __H1B VISA PREDICTION__
## __Goal__
To predict the approval rate of obtaining an H1B VISA in United States of America based on job industry sectors through availabe datasets of the most recent 4 fiscal years.

---------
## __Data Source__
1. [H1B VISA Datasets](https://www.kaggle.com/abishekanbarasan1995/h1b-case-status-prediction) 
*	H-1B_Disclosure_RAW_Data_FY15.csv 
*	H-1B_Disclosure_RAW_Data_FY16.csv 
*	H-1B_Disclosure_RAW_Data_FY17.csv 
*	H-1B_Disclosure_RAW_Data_FY18.csv 

(NAICS_CODE in FY17 and FY18 were manually changed to NAIC_CODE to make it identical across all datasets.)

2. [NAICS Codes](https://www.naics.com/search-naics-codes-by-industry/)

---------
## __Web Scraping of NAICS Codes__
Install dependencies
```python
from bs4 import BeautifulSoup as bs
from splinter import Browser
import pandas as pd
```
Visit web page 
```python
executable_path={'executable_path':'/usr/local/bin/chromedriver'}
browser=Browser('chrome',**executable_path, headless=False)
url='https://www.naics.com/search-naics-codes-by-industry/'
browser.visit(url)
html=browser.html
```
The table on the page provides links to tables of a full list of codes for each category. Those links were obtained in order to access those tables.
```python
soup=bs(html,'html.parser')
results=soup.find_all('td',{'class':'noWrap'})
code_url_list=[result.a['href'] for result in results]
browser.quit()
```
Use Pandas to scrape the full code table for each category and concatdenate them into a single tables.
```python
table_list=[]
for url in code_url_list:
    tables=pd.read_html(url) 
    df=tables[0] 
    df_new=df[['Codes','Titles']] 
    table_list.append(df_new)
big_df=pd.concat([*table_list])
big_df.to_csv('../H1B_Data/NAICS_CODE.csv',index=False, header=True)
```

---
## __H1B VISA Datasets Cleaning__
Install dependencies
```python
import pandas as pd
```
Define a function to process each dataset respectively to remove unnecessary columns and concatdenate them into a single file.
```python
data_list=['../H1B_Data/H-1B_Disclosure_RAW_Data_FY15.csv','../H1B_Data/H-1B_Disclosure_RAW_Data_FY16.csv','../H1B_Data/H-1B_Disclosure_RAW_Data_FY17.csv','../H1B_Data/H-1B_Disclosure_RAW_Data_FY18.csv']

def clean(data):
    df=pd.read_csv(data, encoding="ISO-8859-1")
    new_df=df[["CASE_NUMBER","CASE_STATUS", "DECISION_DATE", "EMPLOYER_NAME","EMPLOYER_CITY","EMPLOYER_STATE", "EMPLOYER_COUNTRY", "NAIC_CODE"]]
    us_df=new_df[new_df["EMPLOYER_COUNTRY"]=="UNITED STATES OF AMERICA"]
    return us_df

H1B_df_list=[]
for data in data_list:
    h1b_df=clean(data)
    H1B_df_list.append(h1b_df)
all_H1B_df=pd.concat([*H1B_df_list]) 
all_H1B_df['EMPLOYER_NAME']=all_H1B_df['EMPLOYER_NAME'].str.replace(',','') 
all_H1B_df.to_csv('../H1B_Data/Pre_Cleaned_H1B_Data.csv',index=False, header=True)
```
Keep columns that are relevant to employers, reset index twice to rename it EMP_ID as a foreign key in database and export to employer.csv.
```python
H1B_no_missing=all_H1B_df.dropna(how='any')
employee_df=H1B_no_missing[["CASE_NUMBER","CASE_STATUS", "DECISION_DATE", "EMPLOYER_NAME","NAIC_CODE"]]

employer_df.drop_duplicates(subset="EMPLOYER_NAME",keep="first",inplace=True)
reset_employer_df=employer_df.reset_index(inplace=False, drop=True)
new_reset_employer_df=reset_employer_df.reset_index(inplace=False, drop=False)

new_employer_df=new_reset_employer_df.rename(columns={'index':'EMP_ID'})
new_employer_df.head(10)
new_employer_df.to_csv('../H1B_Data/employer.csv',index=False)
```
Keep columns that are relevant to employees, add EMP_ID to it as a foreign key for the popurse of data normaliztion.
```python
employer_df=H1B_no_missing[["EMPLOYER_NAME","EMPLOYER_CITY","EMPLOYER_STATE"]]
merge_df=pd.merge(employee_df,new_employer_df,on="EMPLOYER_NAME")
new_employee_df=merge_df[["CASE_NUMBER","CASE_STATUS", "DECISION_DATE", "EMP_ID","NAIC_CODE"]]
new_employee_df.head(10)
employee_no_missing_df=new_employee_df.dropna(how='any')
employee_no_missing_df['NAIC_CODE'].astype(int)
```
Since NAICS codes are updated yearly, codes of previous years might not be in the table was obtained through web scraping. Only keep those records whose NAICS codes are compatible with our table.
```python
nacis_df=pd.read_csv('../H1B_Data/NAICS_CODE.csv')
nac_employee_merge=pd.merge(employee_no_missing_df,nacis_df,left_on='NAIC_CODE',right_on='Codes')
update_employee=nac_employee_merge[["CASE_NUMBER","CASE_STATUS", "DECISION_DATE", "EMP_ID","NAIC_CODE"]]
update_employee.to_csv('../H1B_Data/employee.csv',index=False)
```
---
## __postgreSQL DataBase__
![ERD]('ERD.png')
All datasets (NAICS_CODE.csv, employee.csv and employer.csv) were stored into PostgresSQL database for further analysis. 
```SQL
CREATE TABLE "NAICS_CODE" (
    "CODES" DEC   NOT NULL,
    "TITLES" VARCHAR   NOT NULL,
    CONSTRAINT "pk_NAICS_CODE" PRIMARY KEY (
        "CODES"
     )
);

CREATE TABLE "EMPLOYEE" (
    "CASE_NUMBER" VARCHAR   NOT NULL,
    "CASE_STATUS" VARCHAR  NOT NULL,
    "DECISION_DATE" DATE   NOT NULL,
    "EMP_ID" INT   NOT NULL,
    "NAIC_CODE" DEC   NOT NULL
);

CREATE TABLE "EMPLOYER" (
    "EMP_ID" INT   NOT NULL,
    "EMPLOYER_NAME" VARCHAR   NOT NULL,
    "EMPLOYER_CITY" VARCHAR  NOT NULL,
    "EMPLOYER_STATE" VARCHAR  NOT NULL,
    CONSTRAINT "pk_EMPLOYER" PRIMARY KEY (
        "EMP_ID"
     )
);

ALTER TABLE "EMPLOYEE" ADD CONSTRAINT "fk_EMPLOYEE_NAIC_CODE" FOREIGN KEY("NAIC_CODE")
REFERENCES "NAICS_CODE" ("CODES");

ALTER TABLE "EMPLOYEE" ADD CONSTRAINT "fk_EMPLOYEE_EMP_ID" FOREIGN KEY("EMP_ID")
REFERENCES "EMPLOYER" ("EMP_ID");
```

