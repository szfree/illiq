

import sqlite3
import sys
import datetime
import urllib2
import time
import httplib
import string
import os
from urllib2 import urlopen
import gzip
import StringIO

work_dir = sys.path[0]

def SwitchToYahooID(symbol):
    market = str(symbol[0:2]).upper()
    if market == 'SH':
        market = 'SS'
    return str(symbol[2:8])+'.'+market


def LoadQuotaIntoDB(quotas):
    

    #db = sqlite3.connect(work_dir+'/'+'data/cnstock.db')

    db = sqlite3.connect('.\\data\\cnstock.db')
    cu = db.cursor()
    
    try:
        date = quotas[0]['Date']
        sql = 'delete from quotation where date="'+date+'"'
        cu.execute(sql)
       
        
        for quota in quotas:
            #swith sina symbol to yahoo symbol
            symbol = SwitchToYahooID(quota['Symbol'])

            #print(symbol+': pb='+quota['PB'])
            sql = "insert into quotation (id, date, open, close, adjclose, high, low, volume, turnover, illiq, pb, currcapital_a) values "
            sql += "('"+symbol+"','"+date+"',"+quota["Open"]+","+quota["Close"]+","+quota["Adj Close"]+","+quota["High"]+","+quota["Low"]+","+quota["Volume"]+","+quota["Turnover"]+","+quota["Illiq"]+","+quota['PB']+","+quota['Currcapital_a']+")"
            cu.execute(sql)
        db.commit()
    except Exception, e:
        print(e)
        db.rollback()

        
    cu.close()
    db.close()
    
    return

def FetchExtraDataFromSina(quotas):
    print("fetch market value and pb from sina...")
    
    ncount = 1
    ntotal = len(quotas)
    for quota in quotas:
        doclen = 64*1024 #read 64k data

        content = ''
        link = "http://finance.sina.com.cn/realstock/company/"+quota['Symbol']+"/nc.shtml"
        doc = None
        bSuccess = False
        bFirstTry = True
        while bSuccess == False:
            lta = 1000000000000000 #set lta to max value to skip the stock if can not get it from sina
            book = 0.0001 # set the book to min value to skip the stock if could not get it from sina
            if bFirstTry == False:
                print(quota['Symbol']+': read content falied, retry in 2 seconds...')
                time.sleep(2)
               
            bFirstTry = False 
            try:
                doc = urlopen(link)
                
                if doc.info().has_key('Content-Length'):
                    try:
                        doclen = int(doc.info()['Content-Length'])
                    except Exception, de:
                        doclen = 64*1024

                content = doc.read(doclen)
                if doclen>len(content):
                    print(quota['Symbol']+': read data less than expected, data len is '+str(len(content)))
                    continue
                    
                
                #gzip
                if doc.info().has_key('Content-Encoding'):
                    if doc.info()['Content-Encoding'].strip()=='gzip':
                        gdata = StringIO.StringIO(content)
                        gz = gzip.GzipFile(fileobj=gdata)
                        gcontent = gz.read()
                        gz.close()
                        content = gcontent

            except Exception, e:
                print(quota['Symbol']+': open web link failed...')
                continue
            finally:
                if doc != None:
                    doc.close()
                    doc = None
        
            
            # get the lta, current capital in A market
            target = 'var lta = '
            i = content.find(target)
            if i>-1:
                end = content.find(';',i)
                if end>-1:
                    lta = string.atof(content[i+len(target):end])*10000
            else:
                print(quota['Symbol']+': read lta failed, change buf len to '+str(doclen))
                continue
            
            # get the net book for each share
            target = 'var mgjzc = '
            i = content.find(target)
            if i>-1:
                end = content.find(';',i)
                if end>-1:
                    book = string.atof(content[i+len(target):end])
                    if book == 0:
                        book = 0.0001 # to make PB very high, if book is close to zero
            else:
                print(quota['Symbol']+': read book value failed, buf len change to '+str(doclen))
                continue
            
        
            close = string.atof(quota['Close'])
            lastclose = string.atof(quota['LastClose'])
        
            if close>0:
                #print('close:'+str(close))
                #print('book:' + str(book))
                quota['PB'] = str(close/book)
                quota['Currcapital_a'] = str(close*lta)
            else: #no trade for target date
                quota['PB'] = str(lastclose/book)
                quota['Currcapital_a'] = str(lastclose*lta)
            
            
            print(str(ncount)+'/'+str(ntotal)+': '+quota['Symbol']+',  pb and current capital')    
            bSuccess = True
            ncount += 1
    
    



def ParseQuotaData(data):
    stock_data_array = data.split("\n")
    
    quotas = []
    
    for stock_data in stock_data_array:
        if len(stock_data)<30:
            print "skip " + stock_data + "\n"
            continue

        data = {}
        stock_attrs = stock_data.split(",")
        data['Symbol'] = stock_attrs[0].split("=")[0][-8:]
        stock_name = stock_attrs[0].split("=")[1]
        data['Open'] = str(stock_attrs[1])
        data['LastClose'] = str(stock_attrs[2])
        data['Close'] = str(stock_attrs[3])
        data['High'] = str(stock_attrs[4])
        data['Low'] = str(stock_attrs[5])
        data['Turnover'] = str(stock_attrs[9] )
        data['Volume'] = str(stock_attrs[8] )
        data['Adj Close'] = data['Close']
        data['Date']= stock_attrs[30].strip()
        close = string.atof(data['Close'])
        open = string.atof(data['Open'])
        turnover = string.atof(data['Turnover'])
        if turnover == 0:
            illiq = 0
        else:
            illiq = abs(close-open)/open/turnover
        data['Illiq'] = str(illiq)
        quotas.append(data)
    return quotas



def FetchData(stock_ids):
    
    sina = httplib.HTTPConnection("hq.sinajs.cn")
    sina.request("GET", "/list=" + stock_ids);
    
    bReturn = True
    while bReturn:
        try:
            data = sina.getresponse()
            bReturn = False
        except Exception, e:
            print('fetch data from sina failed, try again...')        
            
    data = data.read()
    data = data.rstrip("\n") # remove ending line break
    sina.close()
    return  data

def FetchQuotaFromSina(stocks):
    stock_id_list = ""
    quotas = []
    total = len(stocks)
    count = 0
    data = ''
    
    for stock in stocks:
        count += 1
        print(str(count)+"/"+str(total)+": "+stock)
        if len(stock_id_list)==0:
            stock_id_list = stock.lower()
        else:
            stock_id_list += "," + stock.lower()
            
        if count%30 == 0:
            #print(stock_id_list)
            data +=  FetchData(stock_id_list)+'\n'
            stock_id_list = ''
            
         
            
    if len(stock_id_list)>0:
        data += FetchData(stock_id_list)
            

    quotas = ParseQuotaData(data)
    FetchExtraDataFromSina(quotas)
    LoadQuotaIntoDB(quotas)
    return
    


#get stock list from file
def GetStockList(stock_file):
    file = open(work_dir+'/'+"data/"+stock_file)
    data = file.read()
    file.close()

    newline = '\n'
    if data.find('\r\n')>-1:
        newline = '\r\n'
    else:
        if data.find('\r')>-1:
            newline = '\r'

    lines = data.split(newline)

    list = []
    for line in lines:
        line = line.strip('\n').strip()
        if line == "":
            continue
        items = line.split(",")
        symbol = str(items[0]).strip('"').strip()
        list.append(symbol)

    return list
    


def main():
    #os.chdir('/home/ec2-user/quanta')
    

    stocks = GetStockList('cn.csv')
    FetchQuotaFromSina(stocks)

    return

main()
