import ystockquote
import sqlite3
import sys
import datetime
import urllib2
import Common
import time
import httplib
import string
import os
import socket


setting = {
    'db' : 'us.db',
    'stockfile' : 'nasdaq.csv,amex.csv,nyse.csv',
}


def SwitchMarketCap(mc):
    c = mc[-1].upper()
    try:
        v = string.atof(mc[:-1])
    except:
        return 10*1000*1000*1000*1000
        
    if c=='K':
        v = v*1000
    if c=='M':
        v = v*1000*1000
    if c=='B':
        v = v*1000*1000*1000
    return v
        

def LoadStockList():
    stocks = []
    sfs = setting['stockfile'].split(',')
    for sf in sfs:
        f = open('data/'+sf)
        line = f.readline()
        while line:
            items = line.split(',')
            symbol = items[0].strip('"')
            if (symbol.upper()!='SYMBOL') and (symbol.find('^')<0) and (symbol.find('/')<0):
                stocks.append(symbol)
            line = f.readline()
        f.close()
        
    return stocks
        
        

      

def LoadQuotaIntoDB(symbol, quota, dbcursor):
    for key in quota:
        data = quota[key]
        volume = string.atof(data["Volume"])
        turnover = (string.atof(data["High"]) + string.atof(data["Low"]))/2 * volume
        data["Turnover"] = str(turnover)
        open = string.atof(data["Open"])
        close = string.atof(data["Close"])
        #if (open == 0) or (turnover == 0):
        if (open == 0) or (volume == 0):
            data["Illiq"] = "0"
        else:
            #data["Illiq"] = str(abs(close-open)/open/turnover)
            data['Illiq'] = str(abs(close-open)/open/volume)

        sql = "insert into quotation (id, date, open, close, adjclose, high, low, volume, turnover, illiq) values "
        sql += "('"+setting["google"]+symbol+"','"+key+"',"+data["Open"]+","+data["Close"]+","+data["Adj Close"]+","+data["High"]+","+data["Low"]+","+data["Volume"]+","+data["Turnover"]+","+data["Illiq"]+")" 
        dbcursor.execute(sql)
    realquota = ystockquote.get_all(symbol)
    mc = realquota['market_cap']
    try:
        pb = string.atof(realquota['price_book_ratio'])
    except:
        pb = 1000
    sql = 'update quotation set pb='+str(pb)+', currcapital_a='+str(SwitchMarketCap(mc))+' where id="' + symbol +'"'
    dbcursor.execute(sql)
        
    return


def FetchQuotaFromYahoo(stocks):

    print("fetch data from yahoo for today")
    
    total = str(len(stocks))
    count = 0
    nSuccess = 0
    nNodata = 0
    nFail = 0

    quotas = []
    for symbol in stocks:

        count += 1
        errmsg = str(count)+"/"+total+" : " + symbol
        nRetry = 12
        nTimeout = 10
        while True:
            try:
                print(errmsg)
                quota = ystockquote.get_all(symbol)
                if quota['last_trade_date'].upper() == 'N/A':
                    print('fetch failed: data is N/A!')
                    nFail += 1
                else:
                    quota['last_trade_date'] = time.strftime('%Y-%m-%d', time.strptime(quota['last_trade_date'].strip('"'), '%m/%d/%Y'))
                    date = quota['last_trade_date']
                    quota['extra'] = ystockquote.get_historical_prices(symbol, date, date)[date]
                    nSuccess += 1
                    quotas.append(quota)
                break
            except urllib2.HTTPError as httpe:
                print('fetch failed: '+str(httpe))
                nFail += 1
                break
            except urllib2.URLError as urle:
                print('fetch failed: '+str(urle))
                pass
            except socket.timeout as toe:
                print('fetch failed: '+str(toe))
                pass
            except socket.error as se:
                print('fetch failed: '+str(se))
                pass
        
            if nRetry == 0:
                nFail += 1
                break
            else:
                time.sleep(nTimeout)
                nRetry = nRetry - 1
            

        
    print("Fetch stocks, "+str(nSuccess)+" done, "+str(nNodata)+" no data found, "+str(nFail)+" failed.")
    return quotas

def LoadQuotasIntoDB(quotas):
    db = sqlite3.connect(setting['db'])
    cu = db.cursor()
    
    try:
        trade_date = quotas[0]['last_trade_date']
        sql = 'delete from quota where date="'+trade_date+'"'
        cu.execute(sql)
        
        for quota in quotas:
            close = string.atoi(quota['extra']['Close'])
            open = string.atoi(quota['extra']['Open'])
            turnover = string.atoi(quota['extra']['Volume'])
            illiq = abs(close-open)/open/turnover
            if quota['stock_exchange'].lower().index('nasdaq') >= 0:
                se = 'NASDAQ'
            if quota['stock_exchange'].lower().index('nasdaq') >= 0:
                se = 'NYSE'
            
            sql = 'insert into quota (id, date, open, close, adjclose, high, low, volume, turnover, illiq, pb, mcap, stock_exchange) values '
            sql += '('+quota['symbol']+',"'+quota['last_trade_date']+'",'+quota['extra']['Open']+','+quota['extra']['Close']+','+quota['extra']['Adj Close']+","+quota['extra']['High']+","+quota['extra']['Low']+','+quota['extra']['Volume']+','+quota['extra']['Volume']+','+str(illiq)+','+quota['price_book_ratio']+','+SwitchMarketCap(quota['market_cap'])+',"'+se+'")'
            cu.execute(sql)
            
        
    finally:
        cu.close()
        db.close()

def main():

    stocks = LoadStockList()
    quotas = FetchQuotaFromYahoo(stocks)
    LoadQuotasIntoDB(quotas)

    return

main()
