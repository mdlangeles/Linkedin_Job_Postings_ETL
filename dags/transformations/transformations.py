import pandas as pd
import json
import logging





#LINKEDIN DATA

def select_columns(df_linkedin):
    keep_columns = ['job_id', 'company_id', 'title', 'description', 'max_salary', 'med_salary', 'min_salary', 'pay_period', 'formatted_work_type', 'location', 'views', 'job_posting_url', 'application_type', 'formatted_experience_level', 'posting_domain', 'sponsored', 'currency', 'compensation_type', 'scraped', 'industry_id', 'industry_name']
    condition_1 = (df_linkedin['formatted_work_type'] == 'Full-time')
    condition_2 = (df_linkedin['pay_period'].isin (['YEARLY', 'HOURLY', 'MONTHLY', 'WEEKLY']))
    df_linkedin = df_linkedin.loc[condition_1 & condition_2][keep_columns]

    return df_linkedin

# def merge(df_linkedin):
#     df_linkedin.merge(read_linkedin_jobs, on='job_id')\
#     .merge(read_linkedin_industries, on='industry_id')

#     return df_linkedin

def salary_standardization(df_linkedin):
    df_linkedin['avg_salary'] = (df_linkedin[[ 'min_salary', 'max_salary']].mean(axis=1))
    df_linkedin.head()

    return df_linkedin

def average_salary(df_linkedin):
    df_linkedin['Salary'] = df_linkedin['avg_salary'].combine_first(df_linkedin['med_salary'])

    return df_linkedin

def delete_columns1(df_linkedin):
    df_linkedin.drop(['max_salary', 'med_salary', 'min_salary', 'avg_salary'], axis = 1 ,inplace=True)

    return df_linkedin

def annual(df_linkedin):
    def c_annual_salary(row):
        if row['pay_period'] == 'YEARLY':
            return row['Salary']
        elif row['pay_period'] == 'MONTHLY':
            return row['Salary'] * 12 # 12 MESES EN UN AÑO
        elif row['pay_period'] == 'WEEKLY':
            return row['Salary'] * 52 # 52 SEMANAS EN UN AÑO
        elif row['pay_period'] == 'HOURLY':
            return row['Salary'] * 2080 # 2,080 HORAS DE TRABAJO COMPLETO EN UN AÑO
        else:
            return None
    
    df_linkedin['annual_salary'] = df_linkedin.apply(c_annual_salary, axis=1)
    return df_linkedin

def delete_columns2(df_linkedin):
    df_linkedin.drop(columns=['pay_period', 'Salary'], inplace=True)

    return df_linkedin

def last_changes(df_linkedin):
    # Imputar "-1" en las columnas company_id y views
    df_linkedin['company_id'].fillna(-1, inplace=True)
    df_linkedin['views'].fillna(-1, inplace=True)

    # Imputar "Not Specified" en las columnas con valores faltantes
    df_linkedin['formatted_experience_level'].fillna('Not specified', inplace=True)
    df_linkedin['industry_name'].fillna('Not specified', inplace=True)
    df_linkedin['posting_domain'].fillna('Not specified', inplace=True)

    df_linkedin['currency'].fillna('USD', inplace=True)
    df_linkedin['compensation_type'].fillna('BASE_SALARY', inplace=True)

    return df_linkedin



