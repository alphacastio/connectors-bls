#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import json
import numpy as np
import pandas as pd
from datetime import date
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


# In[ ]:


#Defino el periodo de inicio y final
#Para actualizarlo no se trae todo el historico, sino a partir de los 2 años más recientes
final_year = str(date.today().year)
start_year = str(date.today().year - 2)


# In[ ]:


# CUSR0000SAF111 Cereals and bakery products
# CUSR0000SAF112 Meats, poultry, fish, and eggs
# CUSR0000SEHK01 Major appliances
# CUSR0000SEFJ01 Milk
# CUSR0000SEHF02 Utility (piped) gas service

headers = {'Content-type': 'application/json'}

post_data = json.dumps({"seriesid": ['CUSR0000SAF111','CUSR0000SAF112', 'CUSR0000SEHK01', 'CUSR0000SEFJ01', 'CUSR0000SEHF02'],
             "startyear":start_year, "endyear":final_year})
response = requests.post('https://api.bls.gov/publicAPI/v1/timeseries/data/', headers=headers, data=post_data)


# In[ ]:


for i in range(5):
    #Extraigo la categoria
    serie = eval(response.content.decode('utf-8'))['Results']['series'][i]['seriesID']
    #Paso los datos a un dataframe
    df_temp = pd.DataFrame(eval(response.content.decode('utf-8'))['Results']['series'][i]['data'])
    #Agrego como columna el codigo de la serie
    df_temp['seriesID'] = serie
    
    #Apilo en un dataframe cada una de las series
    if i==0:
        df = df_temp.copy()
    else:
        df = df.append(df_temp, ignore_index=True)


# In[ ]:


dict_categories = {'CUSR0000SAF111':'Cereals and bakery products', 'CUSR0000SAF112':'Meats, poultry, fish, and eggs',
                   'CUSR0000SEHK01':'Major appliances','CUSR0000SEFJ01':'Milk', 
                   'CUSR0000SEHF02':'Utility (piped) gas service'}

df['category'] = pd.NA

for k, v in dict_categories.items():
    df['category'] = np.where(df['seriesID'] == k, v, df['category'])


# In[ ]:


#Remuevo la M de la columna de meses
df['period'] =df['period'].replace('M', '', regex=True)
#Concateno año y mes para tener la fecha
df['Date'] = pd.to_datetime(df['year'] + '-' + df['period'], format='%Y-%m')


# In[ ]:


#Armo la pivot para que cada serie quede como columna
df = df.pivot(index='Date', columns='category', values='value')


# In[ ]:


df.rename_axis(None, axis=1, inplace=True)
df['country'] = 'USA'


# In[ ]:


# dataset_name = 'Prices - USA - BLS - Consumer Price Index Disaggregated By Component'

# #Para raw data
# dataset = alphacast.datasets.create(dataset_name, 965, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[ ]:


alphacast.datasets.dataset(8473).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




