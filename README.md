# Linkedin Job Postings - ETL
By: Emmanuel Quintero & María de los Ángeles Amú

## Overview
In this project, we are going to analyze, manipulate and visualize data about job postings in Linkedin, as part of an ETL project. We used SQLAlchemy as an Object-Relational Mapping (ORM) tool connected to PostgreSQL, and we will generate visual representations using PowerBI.

## Tools used

- Python
    
- Pandas
    
- PowerBI
    
- SQLAlchemy
    
- PostgreSQL
    
- Jupyter Notebook

## Dataset used
The dataset used in this project is the Linkedin Job Postings - 2023 , imported from Kaggle. It contains 28 columns and 33246 rows that correspond to some works published on Linkedin.
The columns names of “job_postings.csv” before data transformation are:

1. job_id: The job ID as defined by LinkedIn

2. company_id: Identifier for the company associated with the job posting (maps to companies.csv)

3. title: Job title.

4. description: Job description.

5. max_salary: Maximum salary

6. med_salary: Median salary

7. min_salary: Minimum salary

8. pay_period: Pay period for salary (Hourly, Monthly, Yearly)

9. formatted_work_type: Type of work (Fulltime, Parttime, Contract)

10. location: Job location

11. applies: Number of applications that have been submitted

12. original_listed_time: Original time the job was listed

13. remote_allowed: Whether job permits remote work

14. views: Number of times the job posting has been viewed

15. job_posting_url: URL to the job posting on a platform

16. application_url: URL where applications can be submitted

17. application_type: Type of application process (offsite, complex/simple onsite)

18. expiry: Expiration date or time for the job listing

19. closed_time: Time to close job listing

20. formatted_experience_level: Job experience level (entry, associate, executive, etc)

21. skills_desc: Description detailing required skills for job

22. listed_time: Time when the job was listed

23. posting_domain: Domain of the website with application

24. sponsored: Whether the job listing is sponsored or promoted.

25. work_type: Type of work associated with the job

28. currency: Currency in which the salary is provided.

29. compensation_type: Type of compensation for the job.

The columns names of “job_postings.csv”

## Requirements

- Python 3x 

- PostgreSQL 16x (psycopg2)
  
- Openpyxl
  
- Matplotlib
  
- PowerBI desktop
  
- SQLAlchemy
  
- JSON credentials file (keys.json) with the next format:
```json
{
"user": "your_user",
"password": "your_password",
"port": "your_postgres_number_port",
"server": "your_server_address",
"db": "your_database_name"
}
```
## Project Setup

1. Clone de repository:
```bash
https://github.com/mdlangeles/linkedin_job_postings_etl.git
```
2. Go to the project directory:
```bash
cd linkedin_job_postings_etl
```
3. Create a virtual environment:
```bash
python -m venv env
```
4. Activate virtual environment:
```bash
.\venv\Scripts\activate
```
5. Install libraries:
```bash
pip install requirements
```
6. Create a database in PostgreSQL
7. The project have an Jupyter Notebook, "eda.ipynb" and this notebook is divided into 5 sections :
- I recommend you start with section #1: Import modules and libraries, Connection to PostgreSQL, which is where you will import the libraries and modules that were necessary for the development of the  workshop.
    ##### Note: In the second block of code in section #1 you must change the name of the JSON file to the name of the JSON file that you need to create to be able to make the connection to the database. In my case the name of my file was `connection.json`. If you decide to name your file the same way, remember to change the values specified in the `Database Configuration` field located in the README.
- The second step is to run section 2, which is where the table is created in the database and the csv data is inserted into the table once the table is created in PostgreSQL
- The third step is to run section 3, which is where the exploratory analysis of the data was carried out in order to better understand its structure, dimensions of the table and some histograms.
- The fourth step is to run section 4, which is where the logic was applied to classify the hired candidates in a new column called "Hired", also where the technologies were classified by categories for better analysis, and finally a new one table was created (candidatesHired) in the database to which the data was inserted with the new columns that were created for the analysis.
- The last step is to run section 5. In this section the required graphs are generated with matplotlib. However, if you want to see my dashboard made in Power BI, you can find it at the end of the README.
8. Go to Power BI:

  
8.1 Create a new dashboard.
![image](https://github.com/emmanuelqp/WorkShop1/assets/111546312/5c08f327-7312-4e49-8fb6-fe5982eea0e0)

8.2 Select the option Get/Import data and search PostgresSQL Database
![Imagen de WhatsApp 2024-03-05 a las 12 50 40_c0c81374](https://github.com/mdlangeles/linkedin_job_postings_etl/assets/111546312/c1600ad8-bf3c-4381-8de1-6a57cfc073c7)
![image](https://github.com/mdlangeles/linkedin_job_postings_etl/assets/111546312/b5a79359-60e9-4b77-a728-730ca4cb9337)


8.3 Insert your PostgreSQL server and your database name and accept:


![image](https://github.com/emmanuelqp/WorkShop1/assets/111546312/84572a44-9e86-4ef1-b9b8-6336ecacd1c4)

8.4 If you manage to connect to the database the following will appear:


![image](https://github.com/mdlangeles/linkedin_job_postings_etl/assets/111546312/80aa841d-1830-467e-a171-e6c0aaf4b4fa)
##### Note: For our analysis and the creation of our visualizations we used the LinkedinSalary table since this table was where the entire imputation, filtering and standardization procedure was carried out. Basically it is our final table

8.6 Congrats!, you can now select the table you want to work with, you can upload it to the dashboard and make your own dashboard

## Our Dashboard


