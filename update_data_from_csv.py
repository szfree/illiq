import calendar
import sys
import datetime
import string
import sqlite3
import os


setting = {
    'database' : 'data/cnhistory.db',
    'folder' : 'csv'

}





#stock_id, stock_name, date, ttmpe, adjclose,pe,pb


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
        sttmpe = get_value(items[3])
        sadjclose = get_value(items[4])
        spe = get_value(items[5])
        spb = get_value(items[6])


        if (sttmpe=='') or (sadjclose=='') or (spb==''):
            continue
            
        sdate = sdate.replace('/','-')
        if spe == '':
            sql = 'update quotation set ttm_pe='+sttmpe+', adjclose='+sadjclose+', pb='+spb+' where id="'+sid+'" and date="'+sdate+'"'
        else:
            sql = 'update quotation set ttm_pe='+sttmpe+', adjclose='+sadjclose+', pe='+spe+', pb='+spb+' where id="'+sid+'" and date="'+sdate+'"'

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
    
    
