import pandas as pd
import numpy as np
import boto3
import s3fs
import io
access_id = 'AKIAVKU464LTWLHQYF43'
access_key = 'Yq2LTaP6f4p7rZSFzgJ3MiUJh9gPBZnXKAXffo+K'



s3 = boto3.client('s3', aws_access_key_id = access_id, aws_secret_access_key = access_key)
object = s3.get_object(Bucket='rasit-documents', Key='parcel_id.csv')
client = object['Body'].read().decode('utf-8')
parcel = pd.read_csv(io.StringIO(client))["PARID"]



df = pd.DataFrame()
counter = 1
for i in parcel.values:
    try:
    
        df_new = pd.DataFrame()
        gen = pd.read_html("https://www2.alleghenycounty.us/RealEstate/GeneralInfo.aspx?ParcelID="+ i +"&SearchType=3&CurrRow=0&SearchName=&SearchStreet=&SearchNum=&SearchMuni=&SearchParcel="+ i +"&pin="+ i)
        try:
            df_new.loc[0,["Parcel Id", "Property Address"]] = gen[1].loc[0:1,1].values
        except:
            pass
        try:
            df_new.loc[0,['Municipality', 'Owner Name']] = gen[1].loc[0:1,3].values
        except:
            pass
        try:
            df_new.loc[0,['School District', 'Tax Code', 'Class', 'Use Code',
                 'Homestead', 'Farmstead', 'Clean and Green', 'Other Abatement']] = gen[2].loc[0:7,1].values
        except:
            pass
        try:    
            df_new.loc[0,['Neighborhood Code','Owner Code', 'Recording Date',
                 'Sale Date', 'Sale Price', 'Deed Book', 'Deed Page', 'Lot Area']] = gen[2].loc[0:7,3].values
        except:
            pass
        try:
            df_new.loc[0,"Sale Code"] = gen[2].loc[9,3]
        except:
            pass
        try:
            df_new.loc[0,['2021 Full Base Year Market Value - Land Value',
                 '2021 Full Base Year Market Value - Buiding Value',
                 '2021 Full Base Year Market Value - Total Value']] = gen[2].loc[12:14,1].values
        except:
            pass
        try:
            df_new.loc[0,['2021 County Assessed Value - Land Value',
                 '2021 County Assessed Value - Buiding Value',
                 '2021 County Assessed Value - Total Value']] = gen[2].loc[12:14,3].values
        except:
            pass
        try:
            df_new.loc[0,['2020 Full Base Year Market Value - Land Value',
                 '2020 Full Base Year Market Value - Buiding Value',
                 '2020 Full Base Year Market Value - Total Value']] = gen[2].loc[18:20,1].values
        except:
            pass
        try:
            df_new.loc[0,['2020 County Assessed Value - Land Value',
                 '2020 County Assessed Value - Buiding Value',
                 '2020 County Assessed Value - Total Value']] = gen[2].loc[18:20,3].values
        except:
            pass
        try:
            df_new.loc[0,"Owner Mailing"] = gen[3].loc[1,2]
        except:
            pass
        build = pd.read_html("https://www2.alleghenycounty.us/RealEstate/Building.aspx?ParcelID="+ i +"&SearchType=3&CurrRow=0&SearchName=&SearchStreet=&SearchNum=&SearchMuni=&SearchParcel="+ i +"&pin="+ i)
        try:
            df_new.loc[0,["Use Code", "Style", "Stories", "Year Built", "Exterior Finish", "Roof Type"]] = build[2].loc[2:7,1].values
        except:
            pass
        try:
            df_new.loc[0,["Total Rooms", "Bedrooms", "Full Baths", "Half Baths", "Heating/Cooling"]] = build[2].loc[2:6,3].values
        except:
            pass
        try:
            df_new.loc[0,["Basement", "Grade", "Condition", "Fireplace(s)", "Basement Garage", "Living Area"]] = build[2].loc[2:7,5].values
        except:
            pass
        tax = pd.read_html("https://www2.alleghenycounty.us/RealEstate/Tax.aspx?ParcelID="+ i +"&SearchType=3&CurrRow=0&SearchName=&SearchStreet=&SearchNum=&SearchMuni=&SearchParcel="+ i +"&pin="+ i)
        try:
            df_new.loc[0,["Online Tax Bill Mailing Address"]] = tax[3].loc[0,0].replace("Pay Taxes Online  Tax Bill Mailing Address:  ", "")
        except:
            pass
        try:
            df_new.loc[0, ["Net Tax Due"]] = tax[4].loc[0,1].replace("Net Tax Due ", "")
        except:
            pass
        try:
            df_new.loc[0, ["Gross Tax Due"]] = tax[4].loc[1,1].replace("Gross Tax Due ", "")
        except:
            pass
        try:
            df_new.loc[0,["Millage Rate", "Taxable Market Value"]] = tax[4].loc[2:3, 2].values
        except:
            pass
        owner = pd.read_html("https://www2.alleghenycounty.us/RealEstate/Sales.aspx?ParcelID="+ i +"&SearchType=3&CurrRow=0&SearchName=&SearchStreet=&SearchNum=&SearchMuni=&SearchParcel="+ i +"&pin="+ i)[3]
        try:
            if owner[0].isnull().sum() == 2:
                df_new.loc[0, ["First Owner","First Sale Date", "First Sale Price"]] = owner.loc[1, 0:2].values
            elif owner[0].isnull().sum() == 1:
                df_new.loc[0, ["First Owner","First Sale Date", "First Sale Price"]] = owner.loc[2, 0:2].values
                df_new.loc[0, ["Second Owner","Second Sale Date", "Second Sale Price"]] = owner.loc[1, 0:2].values
            elif owner[0].isnull().sum() == 0:
                df_new.loc[0, ["First Owner","First Sale Date", "First Sale Price"]] = owner.loc[3, 0:2].values
                df_new.loc[0, ["Second Owner","Second Sale Date", "Second Sale Price"]] = owner.loc[2, 0:2].values
                df_new.loc[0, ["Third Owner","Third Sale Date", "Third Sale Price"]] = owner.loc[1, 0:2].values
        except:
            pass
        
        df = pd.concat([df,df_new], ignore_index=True)
    except:
        print('error')
    try:
        if (counter % 50 == 0) or (counter == len(parcel)):
            object = s3.get_object(Bucket='rasit-documents', Key='allegheny_new_try.csv')
            client = object['Body'].read().decode('utf-8')
            df_from_s3 = pd.read_csv(io.StringIO(client), index_col=0)
            df.set_index('Parcel Id', inplace=True)
            for i in df.index:
                df_from_s3.loc[i] = df.loc[i]
            csv_buffer=io.StringIO()
            df_from_s3.to_csv(csv_buffer)
            s3.put_object(Bucket='rasit-documents', Key='allegheny_new_try.csv', Body=csv_buffer.getvalue())
            df = pd.DataFrame()

            print(counter)
    except:
        csv_buffer = io.StringIO()
        df.set_index('Parcel Id', inplace=True)
        df.to_csv(csv_buffer)
        s3.put_object(Bucket='rasit-documents', Key='allegheny_new_try.csv', Body=csv_buffer.getvalue())
        
        df = pd.DataFrame()
    
    counter += 1
    
