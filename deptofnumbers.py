import pandas as pd
import requests
import boto3
import io
import s3fs

access_id = 
access_key = 

s3 = boto3.client('s3', aws_access_key_id = access_id, aws_secret_access_key = access_key)




url = "https://www.deptofnumbers.com/rent/pennsylvania/pittsburgh/"

r = requests.get(url)
df= pd.read_html(r.text) # this parses all the tables in webpages to a list
#df
#########################################################################


df[0].rename(columns={'Unnamed: 0':'_Nominal_Real_Gross_Rent_2019'},inplace=True)
Nominal_real_gross = df[0]
Nominal_real_gross = Nominal_real_gross.set_index(Nominal_real_gross.columns[0])
#Nominal_real_gross
csv_buffer =io.StringIO()
Nominal_real_gross.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='Nominal_Gross_Rent_in_Pittsburgh_Pennsylvania.csv', Body=csv_buffer.getvalue())
#####################################################

historic_rent = df[1]
#historic_rent
csv_buffer =io.StringIO()
historic_rent.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='History_Nominal_Gross_Rent_in_Pittsburgh_Pennsylvania.csv', Body=csv_buffer.getvalue())

#########################################################


df[2].rename(columns={'Unnamed: 0':'Rental_Gross_Rate'},inplace=True)
real_gross = df[2]
real_gross= real_gross.set_index(real_gross.columns[0])
#real_gross
csv_buffer =io.StringIO()
real_gross.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='Real_Gross_Rent_in_Pittsburgh_Pennsylvania.csv', Body=csv_buffer.getvalue())

###########################################################33

historic_real_gross = df[3]
#historic_real_gross
csv_buffer =io.StringIO()
historic_real_gross.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='History_Real_Gross_Rent_in_Pittsburgh_Pennsylvania.csv', Body=csv_buffer.getvalue())
##############################################################3


df[4].rename(columns={'Unnamed: 0':'Rental_Vacancy_Rate'},inplace=True)
vacancy_rate = df[4].set_index(df[4].columns[0])
#vacancy_rate 

csv_buffer =io.StringIO()
vacancy_rate.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='Pittsburgh_Rental_Vacancy_Rate.csv', Body=csv_buffer.getvalue())
##############################################################

historic_vacancy = df[5]
#historic_vacancy
csv_buffer =io.StringIO()
historic_vacancy.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='Historical_Pittsburgh_Rental_Vacancy_Rate.csv', Body=csv_buffer.getvalue())
#########################################################



df[6].rename(columns={'Unnamed: 0':'Rental_as_a_Fraction_income'},inplace=True)
fraction_income = df[6].set_index(df[6].columns[0])
#fraction_income
csv_buffer =io.StringIO()
fraction_income.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='Rent_as_a_Fraction_of_Income.csv', Body=csv_buffer.getvalue())
#########################################################################################


historic_fraction_income = df[7]
#historic_fraction_income
csv_buffer =io.StringIO()
historic_fraction_income.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='Historical_Rent_as_a_Fraction_of_Income.csv', Body=csv_buffer.getvalue())
#########################################################################################




df[8].rename(columns={'Unnamed: 0':'Renter Fraction'},inplace=True)
renter_fraction_income = df[8].set_index(df[8].columns[0])
#fraction_income
csv_buffer =io.StringIO()
renter_fraction_income.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='Pittsburgh_Fraction_of_Renters_by_Household_Units.csv', Body=csv_buffer.getvalue())
#########################################################################################



historic_renter_fraction_income = df[9]
#historic_rent_fraction
csv_buffer =io.StringIO()
historic_renter_fraction_income.to_csv(csv_buffer)
s3.put_object(Bucket='rasit-documents', Key='Historical_Pittsburgh_Renter_Fraction.csv', Body=csv_buffer.getvalue())
#########################################################################################





