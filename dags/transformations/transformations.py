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

# API DATA

def drop_duplicates(df_api):
    df_api.drop_duplicates()

    return df_api

def replacing_values(df_api):
    df_api['experience_level'] = df_api['experience_level'].replace({
        'EN': 'Entry Level',
        'EX': 'Experienced',
        'MI': 'Mid-Level',
        'SE': 'Senior'
    })

    df_api['employment_type'] = df_api['employment_type'].replace({
        'FT': 'Full time',
        'PT': 'Part time',
        'CT': 'Contractor',
        'FL': 'Freelancer'
    })

    df_api['company_size'] = df_api['company_size'].replace({
        'L' : 'Large',
        'M' : 'Medium',
        'S' : 'Small'
    })

    return df_api

def mapping_employee_residence(df_api):
    df_api['employee_residence'] = df_api['employee_residence'].map({
        'ES': 'Spain', 'US': 'United States', 
        'CA': 'Canada', 'DE': 'Germany', 
        'GB': 'United Kingdom', 'NG': 'Nigeria', 'IN': 'India', 'HK': 'Hong Kong', 'PT': 'Portugal', 'NL': 'Netherlands', 
        'CH': 'Switzerland', 'CF': 'Central African Republic', 'FR': 'France', 'AU': 'Australia', 'FI': 'Finland', 
        'UA': 'Ukraine', 'IE': 'Ireland', 'IL': 'Israel', 'GH': 'Ghana', 'AT': 'Austria', 'CO': 'Colombia', 
        'SG': 'Singapore', 'SE': 'Sweden', 'SI': 'Slovenia', 'MX': 'Mexico', 'UZ': 'Uzbekistan', 'BR': 'Brazil', 
        'TH': 'Thailand', 'HR': 'Croatia', 'PL': 'Poland', 'KW': 'Kuwait', 'VN': 'Vietnam', 'CY': 'Cyprus', 
        'AR': 'Argentina', 'AM': 'Armenia', 'BA': 'Bosnia and Herzegovina', 'KE': 'Kenya', 'GR': 'Greece', 
        'MK': 'North Macedonia', 'LV': 'Latvia', 'RO': 'Romania', 'PK': 'Pakistan', 'IT': 'Italy', 'MA': 'Morocco', 
        'LT': 'Lithuania', 'BE': 'Belgium', 'AS': 'American Samoa', 'IR': 'Iran', 'HU': 'Hungary', 'SK': 'Slovakia', 
        'CN': 'China', 'CZ': 'Czech Republic', 'CR': 'Costa Rica', 'TR': 'Turkey', 'CL': 'Chile', 'PR': 'Puerto Rico', 
        'DK': 'Denmark', 'BO': 'Bolivia', 'PH': 'Philippines', 'DO': 'Dominican Republic', 'EG': 'Egypt', 'ID': 'Indonesia', 
        'AE': 'United Arab Emirates', 'MY': 'Malaysia', 'JP': 'Japan', 'EE': 'Estonia', 'HN': 'Honduras', 'TN': 'Tunisia', 
        'RU': 'Russia', 'DZ': 'Algeria', 'IQ': 'Iraq', 'BG': 'Bulgaria', 'JE': 'Jersey', 'RS': 'Serbia', 'NZ': 'New Zealand', 
        'MD': 'Moldova', 'LU': 'Luxembourg', 'MT': 'Malta'})
    
    return df_api

def mapping_company_location(df_api):
    df_api['company_location'] = df_api['company_location'].map({
        'ES': 'Spain', 'US': 'United States', 'CA': 'Canada', 'DE': 'Germany', 'GB': 'United Kingdom', 'NG': 'Nigeria', 
        'IN': 'India', 'HK': 'Hong Kong', 'NL': 'Netherlands', 'CH': 'Switzerland', 'CF': 'Central African Republic', 
        'FR': 'France', 'FI': 'Finland', 'UA': 'Ukraine', 'IE': 'Ireland', 'IL': 'Israel', 'GH': 'Ghana', 'CO': 'Colombia', 
        'SG': 'Singapore', 'AU': 'Australia', 'SE': 'Sweden', 'SI': 'Slovenia', 'MX': 'Mexico', 'BR': 'Brazil', 
        'PT': 'Portugal', 'RU': 'Russia', 'TH': 'Thailand', 'HR': 'Croatia', 'VN': 'Vietnam', 'EE': 'Estonia', 
        'AM': 'Armenia', 'BA': 'Bosnia and Herzegovina', 'KE': 'Kenya', 'GR': 'Greece', 'MK': 'North Macedonia', 
        'LV': 'Latvia', 'RO': 'Romania', 'PK': 'Pakistan', 'IT': 'Italy', 'MA': 'Morocco', 'PL': 'Poland', 'AL': 'Albania',
        'AR': 'Argentina', 'LT': 'Lithuania', 'AS': 'American Samoa', 'CR': 'Costa Rica', 'IR': 'Iran', 
        'BS': 'Bahamas', 'HU': 'Hungary', 'AT': 'Austria', 'SK': 'Slovakia', 'CZ': 'Czech Republic', 'TR': 'Turkey', 
        'PR': 'Puerto Rico', 'DK': 'Denmark', 'BO': 'Bolivia', 'PH': 'Philippines', 'BE': 'Belgium', 'ID': 'Indonesia', 
        'EG': 'Egypt', 'AE': 'United Arab Emirates', 'LU': 'Luxembourg', 'MY': 'Malaysia', 'HN': 'Honduras', 'JP': 'Japan', 
        'DZ': 'Algeria', 'IQ': 'Iraq', 'CN': 'China', 'NZ': 'New Zealand', 'CL': 'Chile', 'MD': 'Moldova', 'MT': 'Malta'})

    return df_api


def outliers(df_api):
    Q1 = df_api['salary'].quantile(0.25)
    Q3 = df_api['salary'].quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    df_api = df_api[(df_api['salary'] >= lower_bound) & (df_api['salary'] <= upper_bound)]

    return df_api

def remove_columns(df_api):
    df_api.drop(columns=['remote_ratio'], inplace=True)

    return df_api
