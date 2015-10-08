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
    'stockfile' : None
    }




def main():
    #os.chdir('/home/ec2-user/quanta')
    if len(sys.argv) > 2:
        print('use command like : python illiq.py 2014-05-01')
        print('          or like: python illiq.py')
        print('to output the portfolio for specific calculate day or today')
        sys.exit()
     
    if len(sys.argv) == 1:   
        calc_day = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    else:
        calc_day = sys.argv[1]
            
    memdb = sqlite3.connect(':memory:')

    memdb.execute('attach "'+setting['database']+'" as history')
    cu = memdb.cursor()

    # build stock pool and setup the condition
    poolcondition = ''
    if setting['stockfile'] != None:
        BuildStockPool(setting['stockfile'], cu)
        poolcondition = ' (id in (select pool.id from pool)) and '

    

    tradedays = CalcStartDay(calc_day, cu)
    start = tradedays[len(tradedays)-1][0]
    print('start: ' + start)
    print('end :  ' + calc_day)

    str_where = ' turnover>0 and date>="'+start+'" and date<="'+calc_day+'" group by id having count(*)>=5 order by avg(illiq) desc limit '+str(setting['limit'])
    str_where = poolcondition + str_where

    print('**************************************')
    print('low market value portofolio')
    lowmc_limit = GetMarketValueLimit(cu, calc_day)
    print('market value limit: ' + str(lowmc_limit))

    sql = 'select id,"' + calc_day + '", avg(illiq) from history.quotation where currcapital_a<='+str(lowmc_limit)+' and ' + str_where
    cu.execute(sql)
    stocks = cu.fetchall()
    print('stocks: ' + str(len(stocks)))
    trade_day = Output(stocks, calc_day, cu, True, tradedays)
    print("trade day: "+trade_day)

    print('***************************************')
    print(' normal portofolio')
    sql = 'select id,"'+calc_day+'", avg(illiq) from history.quotation where ' + str_where
    cu.execute(sql)
    
    stocks = cu.fetchall()
    print('stocks: ' + str(len(stocks)))
    trade_day = Output(stocks, calc_day, cu, False, tradedays)
    print("trade day: "+trade_day)

    cu.close()
    memdb.close()
    
    
# the market value of choosen stocks must lower than the target    
def GetMarketValueLimit(cu,calc_day):
    sql = 'select count(pb) from history.quotation where date="' + calc_day+'"'
    cu.execute(sql)
    total = cu.fetchone()[0]
    
    target = total // 10
    if target < 10 * setting['limit']:
        target = 10 * setting['limit']
        
    poolcondition = ''
    if setting['stockfile'] != None:
        poolcondition = ' and (id in (select pool.id from pool)) '

    sql = 'select currcapital_a from history.quotation where date="' + calc_day+'" '+poolcondition+' order by currcapital_a asc limit ' + str(target)
    cu.execute(sql)
    rows = cu.fetchall()
    market_value_limit = rows[len(rows)-1][0]
    
    return market_value_limit
    

    
    
    
    
#get illiq cacl start day for specific month
def CalcStartDay(calc_day, cu):
    cu.execute('select distinct date from quotation where date<="' + calc_day+'" and volume>0 order by date desc limit 5')
    rows = cu.fetchall()
    return rows
    
    

    
    
def Output(stocks, target_date, cu, islowmc, tradedays):
    if islowmc:
        fportfolio = 'data/'+target_date+'_lowmc.csv'
    else:
        fportfolio = 'data/'+target_date+'.csv'
    
    f=open(fportfolio,'w')
    for stock in stocks:
        sql = 'select close, date from history.quotation where date>="'+target_date+'" and id="'+stock[0]+'" and volume>0 order by date asc limit 2'
        cu.execute(sql)
        rows = cu.fetchall()
        trade_day = target_date
        if len(rows)==1: # no record for the day after calc day, get the price for the calc day
            price = rows[0][0]
            trade_day = rows[0][1]
            prev_day = tradedays[1][0]
        else:            # get the price for the day after calc day
            price = rows[1][0]
            trade_day = rows[1][1]
            prev_day = rows[0][1]

        #hit the increase limit, >9.75
        sql = 'select close from history.quotation where date="'+prev_day+'" and id="'+stock[0]+'"'
        cu.execute(sql)
        row = cu.fetchone()
        prev_price = row[0]

        #if price/prev_price-1>0.0975: #hit limit
        #    print('hit growth limit on buy day, skip '+stock[0])
        #    continue

        id = YahooToGoogle(stock[0])
        try:
            shares = 1000000//setting['limit']//price
        except:
            shares = 1
        commission = 1000000//setting['limit']*0.002
        
        f.write(id+','+str(price)+','+str(shares)+','+str(commission)+','+'buy'+','+trade_day+'\n')
    f.close()
    return trade_day
    
    
def YahooToGoogle(stockid):
    market = 'SHE:'
    if stockid.upper().find('.SS')>-1:
        market = 'SHA:'
    return market+stockid[0:6]
    
def BuildStockPool(stockfile, cu):
    if stockfile == None:
        return

    sql = 'create table pool (id varchar(20))'
    cu.execute(sql)

    f = open(stockfile)
    data = f.read()
    f.close()

    data = data.replace('\r', '')
    lines = data.split('\n')
    
    for line in lines:
        sql = 'insert into pool (id) values ("'+line+'")'
        cu.execute(sql)

    sql = 'select count(*) from pool'
    cu.execute(sql)
    row = cu.fetchone()
    print('stock pool: '+str(row[0]))

main()
    
    
