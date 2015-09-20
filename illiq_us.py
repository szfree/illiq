import calendar as cal
import sys
import datetime
import string
import sqlite3
import Common
import time
import os


setting = {
    'database' : 'us.db',
    'limit' : '30',
    'cycle' : 1 , #month
    'stockfile' : 'nasdaq.csv',
    'google' : 'NASDAQ:'
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

    memdb.execute('attach "data/'+setting['database']+'" as history')
    cu = memdb.cursor()
    
    BuildStockPool(setting['stockfile'], cu)
    pb = GetPBLimit(cu, calc_day)
    print('max pb : ' + str(pb))
    market_value = GetMarketValueLimit(cu, calc_day)
    print('max market value: ' + str(market_value))
      
    start = CalcStartDay(calc_day, cu)
    print('start: ' + start)
    print('end :  ' + calc_day)
    
    sql = 'select count(*) from stock'
    cu.execute(sql)
    row = cu.fetchone()
    print('stock pool: '+str(row[0]))
    
    sql = "select id,'" + calc_day + "', avg(illiq) from history.quotation where id in (select id from stock) and turnover>0 and date>='"+start+"' and date<='"+calc_day+"' group by id order by avg(illiq) desc limit "+setting['limit']
    cu.execute(sql)
    
    stocks = cu.fetchall()
    print('stocks: ' + str(len(stocks)))
    Output(stocks, calc_day, cu)
    cu.close()
    memdb.close()
    
#build stock pool to create the portofolio, remove ST stocks, remove 10% highest PB stocks, and select 10% lowest market value as a base market value, and choose all stocks which market value < 1.1 * base market value 
def BuildStockPool(stock_file, cu):
    #build memory table
    cu.execute('create table stock (id varchar(20), name varchar(20))')
    
    #load stock info from file
    sf = open('data/'+stock_file)
    lines = sf.readlines()
    sf.close()
    

    for line in lines:
        line = line.strip().strip('\r').strip('\n')
        if  line == '':
            continue
        items = line.split(',')
        symbol = items[0].strip('\"').rstrip('\"')
        if symbol.upper() == 'SYMBOL':
            continue
        #symbol = SwitchToYahooID(str(items[0]).strip())
        #name = str(items[1]).strip()
    
        symbol = setting['google']+symbol
        sql = 'insert into stock(id, name) values ("'+symbol+'","'+symbol+'")'   
        cu.execute(sql)
    
    return
    
#get the bottom line of top 10% highest PB
def GetPBLimit(cu, calc_day):
    print(calc_day)
    sql = 'select count(pb) from history.quotation where id in (select id from stock) and date="' + calc_day+'"'
    cu.execute(sql)
    total = cu.fetchone()[0]
    print(total)
    
    target = total // 10
    sql = 'select pb from history.quotation where id in (select id from stock) and date="'+calc_day+'" order by pb desc limit '+str(target)
    cu.execute(sql)
    rows = cu.fetchall()
    print(len(rows))
    pb_limit = rows[len(rows)-1][0]
    
    sql = 'select id from history.quotation where date="'+calc_day+'" and pb>='+str(pb_limit)
    cu.execute(sql)
    rows = cu.fetchall()
    for row in rows:
        sql = 'delete from stock where id="'+str(row[0])+'"'
        cu.execute(sql)
    
    
    return pb_limit
    
    
# the market value of choosen stocks must lower than the target    
def GetMarketValueLimit(cu,calc_day):
    sql = 'select count(pb) from history.quotation where id in (select id from stock) and date="' + calc_day+'"'
    cu.execute(sql)
    total = cu.fetchone()[0]
    
    target = total // 10
    sql = 'select currcapital_a from history.quotation where id in (select id from stock)  and date="' + calc_day+'" order by currcapital_a asc limit ' + str(target)
    cu.execute(sql)
    rows = cu.fetchall()
    market_value_limit = rows[len(rows)-1][0] * 1.1
    
    sql = 'select id from history.quotation where date="'+calc_day+'" and currcapital_a>=' + str(market_value_limit)
    cu.execute(sql)
    rows = cu.fetchall()
    for row in rows:
        sql = 'delete from stock where id="'+str(row[0])+'"'
        cu.execute(sql)
    
    return market_value_limit
    
    
    
    
#get illiq cacl start day for specific month
def CalcStartDay(calc_day, cu):
    cu.execute('select distinct date from quotation where date<="' + calc_day+'" order by date desc limit 5')
    rows = cu.fetchall()
    return str(rows[len(rows)-1][0])
    
    

    
    
def Output(stocks, target_date, cu):
    fportfolio = 'data/'+target_date+'.csv'
    
    f=open(fportfolio,'w')
    
    f.write('id, buy, illiq\n')
    for stock in stocks:
        sql = 'select close from history.quotation where date="'+target_date+'" and id="'+stock[0]+'"'
        cu.execute(sql)
        row = cu.fetchone()
        
        id = stock[0]
        price = row[0]
        shares = 20000//price
        commission = 40
        
        f.write(id+','+str(price)+','+str(shares)+','+str(commission)+','+'buy'+','+target_date+'\n')
    f.close()
    
def SwitchToYahooID(symbol):
    market = str(symbol[0:2]).upper()
    if market == 'SH':
        market = 'SS'
    return str(symbol[2:8])+'.'+market
    
def YahooToGoogle(stockid):
    market = 'SHE:'
    if stockid.upper().find('.SS')>-1:
        market = 'SHA:'
    return market+stockid[0:6]
    
    


main()
    
    
