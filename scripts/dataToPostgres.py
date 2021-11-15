import psycopg2 
import pandas as pd

#create the postgres database
conn = psycopg2.connect(database="biodiversitree", user = "postgres", password = "postgres", host = "127.0.0.1", port = "5432")
cur = conn.cursor()

#create each of the postgres tables
'''
cur.execute(CREATE TABLE AIR_DATA
        (PLOT INT,
        SUBPLOT VARCHAR,
        DIV INT,
        TIME2 TIMESTAMP,
        HEIGHT INT,
        AIRTEMP DOUBLE PRECISION,
        CLEANRH DOUBLE PRECISION,
        RAWRH DOUBLE PRECISION,
        SVP DOUBLE PRECISION,
        VP DOUBLE PRECISION,
        VPD DOUBLE PRECISION,
        BATT_VOLT DOUBLE PRECISION);)
conn.commit()

cur.execute(CREATE TABLE SOIL_DATA
        (PLOT INT,
        SUBPLOT VARCHAR,
        DIV INT,
        TIME2 TIMESTAMP,
        HEIGHT INT,
        SAL DOUBLE PRECISION,
        TEMP DOUBLE PRECISION,
        VWC DOUBLE PRECISION,
        BATT_VOLT DOUBLE PRECISION);)
conn.commit()
'''

#update the postgres database

#create and update the parquet files



