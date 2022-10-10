# Databricks notebook source
# DBTITLE 1,Settings that you may want to change
simulation_dir = "dbfs:/tmp/craig_lukasik_dlt_demo/"

raw_fake_data_dir = f"{simulation_dir}/raw_data"

schema = "craig_lukasik_dlt_demo"

event_schema = "timestamp timestamp, account_id string, profile_id string, media_id string, event_type string, ingest_date date"

sleep_between_drops_seconds = 10

start_date = "2021-01-01"

end_date = "2022-10-01"

# COMMAND ----------

# Comment this line if you run this notebook more than once:
spark.sql(f"drop database if exists {schema} cascade")
spark.sql(f"create database if not exists {schema}")

# COMMAND ----------

import time
from datetime import datetime, timedelta, date
import uuid
import numpy as np
import pandas as pd

# COMMAND ----------

# Comment this line if you run this notebook more than once:
dbutils.fs.rm(simulation_dir, True)

# COMMAND ----------

movie_names = """The 400 Blows (1959)
All Quiet on the Western Front (1930)
Wet Hot American Summer (2001)
Duck Soup (1933)
Mafioso (1962)
Stalag 17 (1953)
The Shawshank Redemption (1994)
The Dark Knight (2008)
Fight Club (1999)
Batman Begins (2005)
The Big Lebowski (1998)
Snatch (2000)
La Haine (1995)
The Good, the Bad and the Ugly (1966)
Gladiator (2000)
Saving Private Ryan (1998)
Amélie (2001)
American History X (1998)
In Bruges (2008)
Crash (I) (2004)
Big Fish (2003)
Seven Samurai (1954)
Layer Cake (2004)
The Godfather (1972)
Coming to America (1988)
O Brother, Where Art Thou? (2000)
Friday (1995)
Donnie Brasco (1997)
Raging Bull (1980)
Ed Wood (1994)
Primal Fear (1996)
Half Baked (1998)
Field of Dreams (1989)
Analyze This (1999)
The Godfather Part II (1974)
Mean Streets (1973)
The Dirty Dozen (1967)
The Longest Day (1962)
Combat de boxe (1927)
The Errand Boy (1961)
The Bellboy (1960)
Six Shooter (2004)
CKY2K (2000 Video)
The Dark Hours (2005)
Edmond (2005)
Man Bites Dog (1992)
Let Him Have It (1991)
Johnny Mad Dog (2008)
The Public Enemy (1931)
American Splendor (2003)
The Football Factory (2004)
The Taking of Pelham One Two Three (1974)
Small Time Crooks (2000)
The Business (2005)
Belly (1998)
Rango (2011)
The Departed (2006)
Transformers (2007)
Limitless (I) (2011)
Pulp Fiction (1994)
Star Trek (2009)
The Town (2010)
300 (2006)
Heat (1995)
Schindler's List (1993)
The Matrix (1999)
Goodfellas (1990)
Umberto D. (1952)
The Usual Suspects (1995)
Up (2009)
Ferris Bueller's Day Off (1986)
Ip Man (2008)
Mean Girls (2004)
American Pie (1999)
Casino Royale (2006)
Back to the Future (1985)
Pan's Labyrinth (2006)
Dumb and Dumber (1994)
White Heat (1949)
Braveheart (1995)
Moon (2009)
Lock, Stock and Two Smoking Barrels (1998)
Old School (2003)
Trainspotting (1996)
Slumdog Millionaire (2008)
Casablanca (1942)
Catch Me If You Can (2002)
The Bourne Identity (2002)
The Boondock Saints (1999)
Eddie Murphy: Raw (1987)
Blow (2001)
The Fast and the Furious: Tokyo Drift (2006)
Blood Diamond (2006)
The Game (1997)
Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1964)
Four Lions (2010)
3:10 to Yuma (2007)
The Bourne Supremacy (2004)
Ghostbusters (1984)
Groundhog Day (1993)"""

# COMMAND ----------

movie_name_array = movie_names.split("\n")

# COMMAND ----------

movie_name_rows = list()
for movie_id in range(len(movie_name_array)):
  movie_name_rows.append({"media_id": movie_id, "media_name": movie_name_array.pop()})

# COMMAND ----------

movie_df = spark.createDataFrame(movie_name_rows, schema="media_id string, media_name string")
movie_df.writeTo(f"{schema}.media_names").createOrReplace()

# COMMAND ----------

def random_acct_profile():
  acct = str(np.random.randint(1, 30, 1)[0].item())
  p = str(np.random.randint(1, 5, 1)[0].item())
  profile = f"{acct}_{p}"
  return (acct, profile)

EVENT_TYPES = ['play', 'stop', 'pause', 'sleep', 'power_off']

def random_event_type():
  idx = np.random.randint(0, len(EVENT_TYPES), 1)[0].item()
  return EVENT_TYPES[idx]

def random_media_id():
  return np.random.randint(0, len(movie_name_rows), 1)[0].item()

# COMMAND ----------

def generate_events(dt):
  iotdf = pd.DataFrame(columns = ["timestamp", "account_id", "profile_id", "media_id", "event_type", "ingest_date"])
  iotdf.at[0, "timestamp", ] = dt
  end_at = dt + timedelta(days=1)
  
  n = 0
  while iotdf.iloc[-1, 0] < end_at:
    x = n
    n = n + 1
    iotdf.at[n, "timestamp"] = iotdf.at[x,"timestamp"] + timedelta(minutes=np.random.randint(1,10,1)[0].item())
    (account_id, profile_id) = random_acct_profile()
    media_id = random_media_id()
    event_type = random_event_type()
    iotdf.at[n, "media_id"] = media_id
    iotdf.at[n, "account_id"] = account_id
    iotdf.at[n, "profile_id"] = profile_id
    iotdf.at[n, "event_type"] = event_type
    iotdf.at[n, "ingest_date"] = dt.strftime("%Y-%m-%d")
    
  return iotdf

# COMMAND ----------

def write_events(dy, pdf):
  new_df = spark.createDataFrame(pdf)
  new_df.write.partitionBy("ingest_date").save(
    path=f"{raw_fake_data_dir}", 
    format='csv', 
    mode='append',
    schema=event_schema
  )

# COMMAND ----------

start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

cur_date_obj = start_date_obj
pdf = None
while (cur_date_obj < end_date_obj):
  # print(f"generating data for {cur_date_obj}")
  pdf = generate_events(cur_date_obj)
  write_events(cur_date_obj, pdf)
  cur_date_obj += timedelta(days=1)
  time.sleep(sleep_between_drops_seconds)
