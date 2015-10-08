import calendar
import sys
import datetime
import string
import sqlite3
import os


setting = {
    'database' : 'data/cnhistory.db',
    'folder' : '1509'

}





#stock_id, stock_name, date, open, close, high, low, volume, currcapital_a, turnover, pb


def main():
    csvs = get_csv_list()

    for csv in csvs:
        load_csv(csv)


def get_value(strval):
    return strval.strip().replace('"','').replace(',','')


def get_csv_list():
    data = os.walk(setting['folder'])
    for root, dirs, files in data:
        if root.upper() == setting['folder'].upper():
            return files



def load_csv(csv):
    print('load data from csv file: ' + csv + '...')
    
    
    f = open(setting['folder']+'\\'+csv)
    lines = f.readlines()
    f.close()
    
    db =  sqlite3.connect(setting['database'])
    cu = db.cursor()
    
    skipfirstline = True
    for line in lines:

        if skipfirstline:
            skipfirstline = False
            continue

        items = line.split('","')
        
        sid = get_value(items[0]).replace('SH','SS')
        sdate = get_value(items[2])
        sopen = get_value(items[3])
        sclose = get_value(items[4])
        shigh = get_value(items[5])
        slow = get_value(items[6])
        svolume = get_value(items[7])
    
        scurvalue = get_value(items[8])
        sturnover = get_value(items[9])
    


        if (sopen=='') or (scurvalue=='') or (svolume=='') or (sturnover==''):
            continue
    
        try:
            t = string.atof(sturnover)
        except:
            t = 0
        
        try:
            v = string.atof(svolume)
        except:
            v = 0
            
        c = string.atof(sclose)
        o = string.atof(sopen)
        if (t == 0) or (o == 0):
            illiq = 0
        else:
            illiq = abs(c-o)/o/t
            

        #sql = 'delete from quotation where date="'+sdate+'" and id="'+sid+'"'
        #cu.execute(sql)
        sql = 'insert into quotation (id, date, open, close, high, low, turnover, volume, illiq, currcapital_a) values ("'+sid+'", "'+sdate+'",'+sopen+','+sclose+','+shigh+','+slow+','+sturnover+','+svolume+','+str(illiq)+','+scurvalue+')'
        
        try:
            cu.execute(sql)
        except:
            print('sql error: ' + sql)
            raise

        print(sid + '--' + sdate)
        
        
        
    cu.close()
    db.commit()
    db.close()
    


main()
    
    
