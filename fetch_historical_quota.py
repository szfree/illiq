import ystockquote
import sqlite3
import sys
import datetime
import urllib2
import time
import httplib
import string
import os
import socket


setting = {
    'country' : 'cn',
    'db' : 'data/cnhistory.db',
    'stockfile' : 'data/cn.csv',
    'begin': '2005-04-08',
    'end' : '2015-07-01'
}

        

def LoadStockList():
    stocks = []
    sfs = setting['stockfile'].split(',')
    for sf in sfs:
        f = open(sf)
        data = f.read().replace('\n','') # this is for compatible with windows and Mac
        f.close()
        
        lines = data.split('\r')
        for line in lines:
            if line == '':
                continue
            symbol = line.split(',')[0].strip()
            
            if setting['country'] == 'us':
                if (symbol.upper()!='SYMBOL') and (symbol.find('^')<0) and (symbol.find('/')<0):
                    stocks.append(symbol)
            
            if setting['country'] == 'cn':
                stocks.append(symbol)
        
    return stocks
    
def DataLoaded(symbol, cu): #check the data loaded in the db or not
    sql = 'select count(*) from quotation where id = "' + symbol +'"'
    cu.execute(sql)
    r = cu.fetchone()
    if r[0] > 0:
        return True
    else:
        return False
    

def FetchQuotaFromYahoo(stocks):

    print("fetch historical data from yahoo")
    
    nTotal = len(stocks)
    total = str(nTotal)
    count = 0
    nSuccess = 0
    nNodata = 0
    nFail = 0
    
    db = sqlite3.connect(setting['db'])
    cu = db.cursor()
    
    for symbol in stocks:
        count += 1
        errmsg = str(count)+"/"+total+" : " + symbol
        nRetry = 200
        nTimeout = 10
        
        if DataLoaded(symbol, cu):
            errmsg = errmsg + ' skipped.'
            print(errmsg)
            continue
        
        while True:
            try:
                print(errmsg)
                quotas = ystockquote.get_historical_prices(symbol, setting['begin'], setting['end'])
                nSuccess += 1
                SaveToDB(symbol, quotas, cu)
                db.commit()
                break
            except urllib2.HTTPError as httpe:
                print('fetch failed: '+str(httpe))
                nFail += 1
                break
            except urllib2.URLError as urle:
                print('fetch failed: '+str(urle))
            except socket.timeout as toe:
                print('fetch failed: '+str(toe))
            except socket.error as se:
                print('fetch failed: '+str(se))
        
            if nRetry == 0:
                nFail += 1
                break
            else:
                time.sleep(nTimeout)
                nRetry = nRetry - 1
 
        
    cu.close()
    db.close()
    
    print("Fetch stocks, "+str(nSuccess)+" done, "+str(nFail)+" failed.")
    return
    
def BuildDB():
   if os.path.exists(setting['db']):
       return
       
   db = sqlite3.connect(setting['db'])
   cu = db.cursor()
       
   sql = "create table quotation (id varchar(20), date varchar(10), open real, close real, turnover real, volume integer, illiq real, adjclose real, pb real)"
   cu.execute(sql)
   sql = "create index idx_idv on quotation (id, date, volume)"
   cu.execute(sql)
   sql = "create index idx_td on quotation(turnover, date)"
   cu.execute(sql)
   sql = "create index idx_date on quotation(date)"
   cu.execute(sql) 
   sql = "create index idx_dv on quotation (volume, date)"
   cu.execute(sql)
   
   cu.close()
   db.close()

def SaveToDB(symbol, quota, cu):
    for key in quota:
        data = quota[key]
        volume = string.atof(data["Volume"])
        turnover = (string.atof(data["High"]) + string.atof(data["Low"]))/2 * volume
        data["Turnover"] = str(turnover)
        open = string.atof(data["Open"])
        close = string.atof(data["Close"])
        if (open == 0) or (turnover == 0):
            data["Illiq"] = "0"
        else:
            data["Illiq"] = str(abs(close-open)/open/turnover)

        sql = "insert into quotation (id, date, open, close, adjclose, volume, turnover, illiq) values "
        sql += "('"+symbol+"','"+key+"',"+data["Open"]+","+data["Close"]+","+data["Adj Close"]+","+data["Volume"]+","+data["Turnover"]+","+data["Illiq"]+")" 
        cu.execute(sql)

    
def main():

    BuildDB()
    stocks = LoadStockList()
    FetchQuotaFromYahoo(stocks)
    


    return

main()
