#!/usr/local/bin/python3
import json
import requests
import sys
import csv
import pandas as pd

def process():
  df = pd.read_csv(sys.argv[1])
  processFile(df,"Event Value")

def getColumns(df):
  return list(df.columns)

def getColumnLength(df):
  return len(df.columns)

def getEventValueFieldJson(rec):
  data = json.loads(rec)
  return data

def addColumnValues(df,colName,colValue,index):
   df = addNewColumn(df,colName)
   df.at[index,colName] = colValue
   return df

def addNewColumn(df,newCol):
  if newCol not in getColumns(df):
    df.insert(getColumnLength(df),newCol,"")
  return df

def processFile(df,col):
  #allColsArr = getColumns(df)
  for i in range(len(df)):
    csvValue = df[col][i]
    jsonData = getEventValueFieldJson(csvValue)

    for colName,colValue in jsonData.items():
      #add the field
      df = addColumnValues(df,colName,colValue,i)

  df.to_csv('output.csv',index=False)

if __name__=="__main__":
  #check for arguments.  input, output file.
  process()
