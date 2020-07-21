import os
import sys
import csv
import variables
import glob
import psycopg2

variables.setVariables()
sourceDir = str(os.environ['SQL_SOURCE_FOLDER']) + '/*.sql'

def connection():
    try:
        connect = psycopg2.connect(
            database=str(os.environ['db_name']),
            user=str(os.environ['username']),
            password=str(os.environ['password']),
            host=str(os.environ['host']),
            port=int[os.environ['port']],
            connect_timeout=3)
    except psycopg2.OperationalError as e:
        print("Connection Failed")
        sys.exit(1)
    else:
        print("Connected !")
        return connect

connected = connection()

if connected:
    for fileIn in glob.iglob(sourceDir):
        try:
            cursor = connected.cursor()
            sql_command = open(fileIn, 'r')
            cursor.execute(sql_command.read())
            records = cursor.fetchall()
            print(records)
            connected.commit()
        except psycopg2.Error as e:
            print("Query Failed: " + str(fileIn) + "Error: " + str(e))
        else:
            print("Query Succeeded" + str(fileIn) + "Error: " + str(e))

connected.close()
