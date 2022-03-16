import pandas
import psycopg2

def connect():
    query='''create table weather(
    City-Name varcahr(20),
    Description varchar(20),
    Temperature numeric(2,2),
    Feels_Like_Temperature numeric(2,2),
    Min_Temperature numeric(2,2),
    Humidity numeric(2,1),
    Clouds numeric(2,1)) 
    '''
    conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow", port="5432")
    cursor = conn.cursor()
    #cursor.execute(query)
    #print(cursor.fetchall())
connect()
