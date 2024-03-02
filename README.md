### Linkedin Job Postings
       By: Emmanuel Quintero & María de los Ángeles Amú

## Overview
In this project, we are going to analyze, manipulate and visualize data about job postings in Linkedin, as part of an ETL project. We used SQLAlchemy as an Object-Relational Mapping (ORM) tool connected to PostgreSQL, and we will generate visual representations using PowerBI.

## Tools used

    Python
    Pandas
    PowerBI
    SQLAlchemy
    PostgreSQL
    Jupyter Notebook

## Dataset used
The dataset used in this project is the Linkedin Job Postings - 2023 , imported from Kaggle. It contains 28 columns and 33246 rows that correspond to some works published on Linkedin.
The columns names of “job_postings.csv” before data transformation are:

    job_id: The job ID as defined by LinkedIn 
    company_id: Identifier for the company associated with the job posting (maps to companies.csv)
    title: Job title.
    description: Job description.
    max_salary: Maximum salary
    med_salary: Median salary
    min_salary: Minimum salary
    pay_period: Pay period for salary (Hourly, Monthly, Yearly)
    formatted_work_type: Type of work (Fulltime, Parttime, Contract)
    location: Job location
    applies: Number of applications that have been submitted
    original_listed_time: Original time the job was listed
    remote_allowed: Whether job permits remote work
    views: Number of times the job posting has been viewed
    job_posting_url: URL to the job posting on a platform
    application_url: URL where applications can be submitted
    application_type: Type of application process (offsite, complex/simple onsite)
    expiry: Expiration date or time for the job listing
    closed_time: Time to close job listing
    formatted_experience_level: Job experience level (entry, associate, executive, etc)
    skills_desc: Description detailing required skills for job
    listed_time: Time when the job was listed
    posting_domain: Domain of the website with application
    sponsored: Whether the job listing is sponsored or promoted.
    work_type: Type of work associated with the job
    currency: Currency in which the salary is provided.
    compensation_type: Type of compensation for the job.

The columns names of “job_postings.csv”

## Requirements

    Python 3x 
    PostgreSQL 16x (psycopg2)
    Openpyxl
    Matplotlib
    PowerBI desktop 
    SQLAlchemy
    JSON credentials file (keys.json) with the next format:
        {
             "user": "your_user",
            "password": "your_password",
            "port": 5432,
            "server": "your_server_address",
            "db": "your_database_name"
        }

## Project Setup
