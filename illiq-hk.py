import calendar as cal
import sys
import datetime
import string
import sqlite3
import Common
import time
import os

# this is for cn stock
# setting = {
#     'database' : 'cnstock.db',
#     'limit' : '30',
#     'cycle' : 1 , #month
#     'stockfile' : 'cn.csv',
#     'pbmax' : 3
# }
# 
# 

#this is for hk stock
setting = {
    'database' : 'hk.db',
    'limit' : '20',
    'cycle' : 1 , #month
    'stockfile' : 'hk.csv',
    'pbmax' : 100
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
    
    #BuildStockPool(setting['stockfile'], cu)
    pb = GetPBLimit(cu, calc_day)
    print('max pb : ' + str(pb))
    
    market_value = GetMarketValueLimit(cu, calc_day)
    print('max market value: ' + str(market_value))
      
    start = CalcStartDay(calc_day, cu)
    print('start: ' + start)
    print('end :  ' + calc_day)
    
    sql = 'select count(*) from history.quotation where date="'+calc_day+'" and pb<='+str(pb)+' and currcapital_a<='+str(market_value)
    cu.execute(sql)
    row = cu.fetchone()
    print('stock pool: '+str(row[0]))
    
    sql = "select id,'" + calc_day + "', avg(illiq) from history.quotation where turnover>5000000 and pb<="+str(pb)+" and currcapital_a<="+str(market_value)+" and turnover>0 and date>='"+start+"' and date<='"+calc_day+"' group by id having count(*)>=4 order by avg(illiq) desc limit "+setting['limit']
    cu.execute(sql)
    
    stocks = cu.fetchall()
    print('stocks: ' + str(len(stocks)))
    trade_day = Output(stocks, calc_day, cu)
    print("trade day: "+trade_day)
    cu.close()
    memdb.close()
    
#build stock pool to create the portofolio, remove ST stocks, remove 10% highest PB stocks, and select 10% lowest market value as a base market value, and choose all stocks which market value < 1.1 * base market value 
def BuildStockPool(stock_file, cu):
    #build memory table
    cu.execute('create table stock (id varchar(20), name varchar(20))')
    
    #load stock info from file
    sf = open('data/'+stock_file)
    data = sf.read()
    sf.close()
    
    #for mac
    #lines = data.split('\r')
    #print(str(len(lines)))

    lines = data.split('\n')
    


    for line in lines:
        
        if line.strip() == '':
            continue
        items = line.split(',')
        symbol = SwitchToYahooID(str(items[0]).strip())
        name = str(items[1]).strip()
        
        sql = 'insert into stock(id, name) values ("'+symbol+'","'+name+'")'   
        cu.execute(sql)

    
    return
    
#get the bottom line of top 10% highest PB
def GetPBLimit(cu, calc_day):
    sql = 'select count(pb) from history.quotation where date="' + calc_day+'"'
    cu.execute(sql)
    total = cu.fetchone()[0]

    target = total // 10
    sql = 'select pb from history.quotation where date="'+calc_day+'" order by pb desc limit '+str(target)
    cu.execute(sql)
    rows = cu.fetchall()

    pb_limit = rows[len(rows)-1][0]
    
    if pb_limit>setting['pbmax']:
        pb_limit = setting['pbmax']
    
    return pb_limit
	
    
    
# the market value of choosen stocks must lower than the target    
def GetMarketValueLimit(cu,calc_day):
    sql = 'select count(currcapital_a) from history.quotation where date="' + calc_day+'"'
    cu.execute(sql)
    total = cu.fetchone()[0]
    
    target = total // 10
    if target < 200:
        target = 200
        
    sql = 'select currcapital_a from history.quotation where date="' + calc_day+'" and turnover>5000000 order by currcapital_a asc limit ' + str(target)
    cu.execute(sql)
    rows = cu.fetchall()
    market_value_limit = rows[len(rows)-1][0]
    
    return market_value_limit
    
    
# the market value of choosen stocks must lower than the target    
def GetLowestMarketValueLimit(cu,calc_day):
    sql = 'select count(pb) from history.quotation where id in (select id from stock) and date="' + calc_day+'"'
    cu.execute(sql)
    total = cu.fetchone()[0]
    
    target = int(setting['limit'])
        
    sql = 'select currcapital_a from history.quotation where id in (select id from stock)  and date="' + calc_day+'" order by currcapital_a asc limit ' + str(target)
    cu.execute(sql)
    rows = cu.fetchall()
    market_value_limit = rows[len(rows)-1][0]
    
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
        sql = 'select close, date from history.quotation where date>="'+target_date+'" and id="'+stock[0]+'" order by date asc limit 2'
        cu.execute(sql)
        rows = cu.fetchall()
        trade_day = target_date
        if len(rows)==1: # no record for the day after calc day, get the price for the calc day
            price = rows[0][0]
            trade_day = rows[0][1]
        else:            # get the price for the day after calc day
            price = rows[1][0]
            trade_day = rows[1][1]
        
        id = YahooToGoogle(stock[0])
        try:
            shares = 120000//price
        except:
            shares = 1
        commission = 160
        
        f.write(id+','+str(price)+','+str(shares)+','+str(commission)+','+'buy'+','+trade_day+'\n')
    f.close()
    return trade_day
    
# only for hk stock
def SwitchToYahooID(symbol):
    id = symbol
    if len(id)==4:
        id = '0'+symbol
    return id+'.HK'

    
def YahooToGoogle(stockid):
    market = 'HKG:'
    return market+stockid[1:5]
    
    


main()
    
    
