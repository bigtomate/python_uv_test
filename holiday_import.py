import psycopg2
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import json

def extract_holidays(region):
    df = pd.DataFrame(columns=['division','title','date','notes','bunting'])
    with open("holiday.json") as json_file:
       items = json.load(json_file)
       dict = items[region]
       for event in dict['events']:
        event['division'] = region
    
        df_append = pd.DataFrame.from_dict(event, orient='index').T
        df = pd.concat([df, df_append], ignore_index=True)
    return df

def transform(dataframe):
        def parse_date(date_str):
         if pd.isna(date_str):
             return pd.Nat
         try: 
          date = datetime.strptime(date_str, "%Y-%m-%d").date()
          if date is not None:
             return date
          else:
             return pd.Nat
         except:
          print(f'invalid date format {date_str}')
          return pd.Nat
        dataframe = dataframe.copy()
        dataframe['date'] = dataframe['date'].apply(parse_date)
        return dataframe

def save_in_db(df):
    conn = psycopg2.connect(
    database='etl',
    user='postgres',
    password='password',
    host='localhost',
    port=5433
   )
    cursor = conn.cursor()
    SQL_CREATE = """ create table if not exists holiday
    (
        id SERIAL not null,
        division varchar (255) not null,
        date DATE,
        title varchar (255),
        notes varchar (255),
        bunting boolean,
        primary key (id)
    ) ;
"""
    result = cursor.execute(SQL_CREATE)
    conn.commit()

    engine = create_engine('postgresql://postgres:password@localhost:5433/etl',
                                echo=False,
                                pool_pre_ping=True)

    df.to_sql('holiday',  con=engine, if_exists='append', index=False) 
    

df = extract_holidays('england-and-wales')
df = transform(df)
print(df.head())
save_in_db(df)