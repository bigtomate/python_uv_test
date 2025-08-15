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
    SQL_CREATE_UNIQUE_IDX = """CREATE UNIQUE INDEX if not exists uidx_division_date
    ON holiday (division, date);"""
    cursor.execute(SQL_CREATE)
    cursor.execute(SQL_CREATE_UNIQUE_IDX)
    conn.commit()

    engine = create_engine('postgresql://postgres:password@localhost:5433/etl',
                                echo=False,
                                pool_pre_ping=True)
    
    existing = pd.read_sql_query(
        "SELECT division, date FROM holiday;",
        engine
    )

    existing_keys = set(existing.itertuples(index=False, name=None))
    new_keys = set(df[['division', 'date']].itertuples(index=False, name=None))
    keys_to_insert = new_keys - existing_keys

    filtered_df = df[
        df[['division', 'date']]
        .apply(tuple, axis=1)
        .isin(keys_to_insert)
    ]

    filtered_df.to_sql('holiday',  con=engine, if_exists='append', index=False) 
    print(f"Inserting {len(filtered_df)} new entries.")

region = input('enter a region: ')
df = extract_holidays(region)
df = transform(df)
save_in_db(df)