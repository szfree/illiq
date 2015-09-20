import calendar as cal
import sys
import datetime
import string
import sqlite3
import time
import os

# this is for cn stock
setting = {
    'database' : 'data/cnhistory.db',
    'limit' : 25,
    'cycle' : 1 , #month
    'stockfile' : 'data/cn.csv',
    'resultfile' : 'data/portfolio.csv',
    'pbmax' : 100,
    'mclimit' : False

}

#save the result to csv file
def SaveResult(cu):
    sql = 'select calcday, avg((sellprice - buyprice)/buyprice) from portfolio group by calcday order by calcday asc'
    cu.execute(sql)
    rows = cu.fetchall()

    ls = os.linesep
    try:  
        fobj = open(setting['resultfile'],  'w')  
    except IOError as err:  
        print('file open error: {0}'.format(err))    


    for row in  rows:
        fobj.write(row[0]+','+str(row[1])+'\n')
        #fobj.write(row[0]+','+str(row[1])+ls)
          
    fobj.close()  
      
    print('Save Portfolio done!')





# get the list of calc day for each month
def GetCalcDayList(cu):
    sql = 'select date, count(*) from quotation where volume>0 group by date having count(*)>500 order by date asc'
    cu.execute(sql)
    rows = cu.fetchall()

    calcDayList = []
    m = ''
    d = ''
    for row in rows:
        items = row[0].split('-')
        if (m != items[1]):
            if (m!=''):
                calcDayList.append(d)
            m = items[1]
        d = row[0]
    
    return calcDayList

# build illiq result tables
def InitDB(cu):
    sql = 'select count(*) from sqlite_master where type = "table" and name="portfolio"'
    cu.execute(sql)
    r = cu.fetchone()
    if r[0] > 0: #table exists
        sql = 'delete from portfolio'
    else:
        sql = 'create table portfolio (id varchar(20), calcday varchar(10), buyday varchar(10), sellday varchar(10), buyprice real, sellprice real, pb real)'
    cu.execute(sql)


# build memeory db and copy table 'quotation'
def BuildMemDB():
    print('building memory db...')
    memdb = sqlite3.connect(':memory:')
    memcu = memdb.cursor()

    db = sqlite3.connect(setting['database'])
    
    sqls = db.iterdump()
    count = 0
    for sql in sqls:
        count += 1
        print('generate memory db: '+str(count))
        if count>5375742:
            print(sql)
        #memcu.execute(sql)

    raise
    db.close()
    memcu.close()
    return memdb


def illiq(calcday, cu):

    calc_day = calcday
    print('calc day: '+calc_day)
      
    start = CalcStartDay(calc_day, cu)

    if setting['mclimit']:
        mclimit = GetMarketValueLimit(cu,calc_day)
        sql = "select id,'" + calc_day + "', avg(illiq) from quotation where currcapital_a<=" +str(mclimit)+" and turnover>0 and date>='"+start+"' and date<='"+calc_day+"' group by id having count(*)==5 order by avg(illiq) desc limit "+str(setting['limit'])
    else:
        sql = "select id,'" + calc_day + "', avg(illiq) from quotation where turnover>0 and date>='"+start+"' and date<='"+calc_day+"' group by id having count(*)==5 order by avg(illiq) desc limit "+str(setting['limit'])
    cu.execute(sql)
    stocks = cu.fetchall()

    trade_day = Output(stocks, calc_day, cu)
    
    

def main():

    #db = BuildMemDB()
    db = sqlite3.connect(setting['database'])
    cu = db.cursor()

    InitDB(cu)
    print("calc days...")
    days = GetCalcDayList(cu)

    l = len(days)
    for day in days:
        if day == days[l-1]:
            continue
        illiq(day, cu)


    db.commit()

    SaveResult(cu)

    cu.close()
    db.close()
    
    
#get illiq cacl start day for specific month
def CalcStartDay(calc_day, cu):
    cu.execute('select distinct date from quotation where date<="' + calc_day+'" order by date desc limit 5')
    rows = cu.fetchall()
    return str(rows[len(rows)-1][0])    

    
# return [], #1 is sellday, item2 is sellprice
def GetSellPrice(stockid, minusOneDay, cu):
    sql = 'select close, date, adjclose from quotation where date>="' +minusOneDay+ '" and volume>0 and id="'+stockid+'" order by date asc limit 3'
    cu.execute(sql)
    rows = cu.fetchall()
    
    # handle stop trading stocks
    if len(rows)<3:
        return None # skip the stock
        rsellday = rows[0][1]
        rsellprice = rows[0][2]
        r = []
        r.append(rsellday)
        r.append(rsellprice)
        return r
        
    # check to hit the decreasing limit
    if (rows[1][0] - rows[0][0])/rows[0][0] < -0.098: 
        return GetSellPrice(stockid, rows[1][1], cu)
        
    rsellday = rows[1][1]
    rsellprice = rows[1][2]

    dMinusOneDay = datetime.datetime.strptime(minusOneDay,'%Y-%m-%d')
    dSellDay = datetime.datetime.strptime(rsellday,'%Y-%m-%d')
    distance = (dSellDay - dMinusOneDay).days
    if distance>12:
        return None #skip the stock

    r = []
    r.append(rsellday)
    r.append(rsellprice)
    return r
    
    
#return none then skip the stock
def GetBuyPrice(stockid, calcday, buyday, cu):
    
    sql = 'select date, close, adjclose from quotation where id="'+stockid+'" and date>="'+calcday+'" and volume>0 order by date asc limit 2'
    
    cu.execute(sql)
    rows = cu.fetchall()
    
    if rows[1][0] != buyday : 
        return None  # skip the stock
    
    #hit the increasing top point
    if (rows[1][1]-rows[0][1])/rows[0][1] > 0.098 : 
        return None # skip the stock
    
    return rows[1][2]
    
    
def Output(stocks, calc_day, cu):

    buyday = CalcBuyDay(calc_day, cu)
    print(buyday)
    sellday = CalcSellDay(buyday, cu)
    print(sellday)

    for stock in stocks:  

        buyprice = GetBuyPrice(stock[0], calc_day, buyday, cu)
        if buyprice == None:
            continue


        sql = 'select date from quotation where date<"'+sellday +'" and volume>0 and id="'+stock[0]+'" order by date desc limit 1'
        cu.execute(sql)
        row = cu.fetchone()
        
        
        s = GetSellPrice(stock[0], row[0], cu)
        if s == None:
            continue


        sql = "insert into portfolio (id, calcday, buyday, buyprice,sellday,sellprice) values ('"+stock[0]+"','"+calc_day+"','"+buyday+"',"+str(buyprice)+",'"+s[0]+"',"+str(s[1])+")"
        
        try:
            cu.execute(sql)
        except:
            print(sql)
            raise
        
        
    return buyday
    
def CalcBuyDay(calc_day, cu):
    sql = 'select date from quotation where date>"'+calc_day+'" and volume>0 group by date order by date asc limit 1'
    cu.execute(sql)
    row = cu.fetchone()
    return row[0]

def CalcSellDay(buyday, cu):
    items = buyday.split('-')
    y = int(items[0])
    m = int(items[1])

    if m != 12:
        m = m+1
    else:
        m = 1
        y = y + 1

    sellday = str(y)+'-'+'%02d'%m+'-01'
    sql = 'select date from quotation where date>="'+sellday+'" and volume>0 group by date order by date asc limit 1'
    cu.execute(sql)
    row = cu.fetchone()
    return row[0]

# the market value of choosen stocks must lower than the target    
def GetMarketValueLimit(cu,calc_day):
    sql = 'select count(pb) from quotation where date="' + calc_day+'"'
    cu.execute(sql)
    total = cu.fetchone()[0]
    
    target = total // 10
    if target < 10 * setting['limit']:
        target = 10 * setting['limit']
        
    sql = 'select currcapital_a from quotation where date="' + calc_day+'" order by currcapital_a asc limit ' + str(target)
    
    cu.execute(sql)
    rows = cu.fetchall()
    market_value_limit = rows[len(rows)-1][0]
    
    return market_value_limit
    


main()
    
    
