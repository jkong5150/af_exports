#!/usr/local/bin/python3
import sys
import json
import argparse
import csv
import numpy as np
import pandas as pd
from timer import timeit
import time

def getColumns(df):
  return list(df.columns)

def getColumnLength(df):
  return len(df.columns)

def writeFile(df,fileName):
  if fileName is None:
    fileName='output.csv'
  df.to_csv(fileName,index=False)

def addColumnValues(df,colName,colValue,index):
  if colName not in getColumns(df):
    df = addNewColumn(df,colName)
  df.at[index,colName] = colValue
  return df

def addNewColumn(df,newCol):
  #if newCol not in getColumns(df):
  df.insert(getColumnLength(df),newCol,"")
  return df

def getEventValueFieldJson(fieldValue):
  def is_json(fieldValue):
    try:
      json.loads(fieldValue)
    except ValueError as e:
      return False
    return True
  if is_json(fieldValue):
    return json.loads(fieldValue)
  return None

@timeit
def processFile(df,col):
  for i in range(len(df)):
    csvValue = df[col][i]
    jsonData = getEventValueFieldJson(csvValue)
    if jsonData is not None:
      for colName,colValue in jsonData.items():
        df = addColumnValues(df,colName,colValue,i)
  return df

@timeit
def readFile():
  return pd.read_csv(args.csvfile,dtype={"Event Revenue": np.float64,"Postal Code":str,"Region":str,"DMA":str})

@timeit
def process():
  print("Processing file...")
  df = readFile()
  df = processFile(df,args.col)
  writeFile(df,args.output)
  

if __name__=='__main__':
  sTime = time.time()
  parser = argparse.ArgumentParser(description='Add columns to AppsFlyer csv file by processing the json in the Event Value column.')
  parser.add_argument('csvfile', type=str, help='AppsFlyer csv export')
  parser.add_argument('col', type=str, help='Column Name to parse json')  
  parser.add_argument('--output', type=str, help='Output file name.  If not provided, the output file will be named output.csv')  
  args = parser.parse_args()
  print("argparse time: {}".format(time.time()-sTime))
  process()
