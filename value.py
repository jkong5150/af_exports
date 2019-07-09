#!/usr/local/bin/python3
import sys
import json
import argparse
import csv
import numpy as np
import pandas as pd
from timer import timeit
import urllib.parse as urlparse

@timeit
def writeFile(df,fileName):
  if fileName is None:
    fileName='output.csv'
  df.to_csv(fileName,index=False)

def getField(fieldValue):
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
def initializeColumnDict(df,col):
  cols = dict()
  values = df[col].values
  keys=None
  jsonData=None
  for val in values:
    jsonData = getField(val)
    if jsonData is not None:
      keys = jsonData.keys()
      #have to intialize...not many colunns...
      for key in keys:
        cols[key]=[]
  #get columns from params. (different than json)
  return cols

def processJson(val,cols):
  jsonData = getField(val)
  if jsonData is not None:
    for k in cols:
      cols.get(k).append(jsonData.get(k) if jsonData.get(k) is not None else "")
  else:
    for k in cols:
      cols.get(k).append("")
  return cols

def addColumns(df,cols):
  for k,v in cols.items():
    df[k]=v  
  return df

def processUrlParams(val,cols):
  url = urlparse.parse_qs(urlparse.urlparse(url).query)
  if url is not None:
    for k in cols:
      cols.get(k).append(jsonData.get(k) if jsonData.get(k) is not None else "")
  else:
    for k in cols:
      cols.get(k).append("")
  return cols

@timeit
def processFile(df,col):
  #create a set of columns
  cols = initializeColumnDict(df,col)
  #define options
  options={'urlparams': processUrlParams,'json': processJson}
  
  #for every column
  for val in df[col].values:
    #cols = processJson(val,cols)
    cols = options[args.format](val,cols)

  #add the columnns - might be better way...
  return addColumns(df,cols)
  
  
def readFile():
  return pd.read_csv(args.csvfile,dtype={"Event Revenue": np.float64,"Postal Code":str,"Region":str,"DMA":str})

@timeit
def process():
  print("Processing file...")
  df = readFile()
  df = processFile(df,args.col)
  writeFile(df,args.output)

if __name__=='__main__':
  parser = argparse.ArgumentParser(description='Add columns to AppsFlyer csv file by processing the json in the Event Value column.')
  parser.add_argument('csvfile', type=str, help='AppsFlyer csv export')
  parser.add_argument('col', type=str, help='Column Name to parse (json or url params')
  parser.add_argument('--format', default='urlparams', help='json or params')
  parser.add_argument('--output', type=str, help='Output file name.  If not provided, the output file will be named output.csv')  
  args = parser.parse_args()
  process()
