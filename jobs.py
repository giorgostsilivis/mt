import pandas as pd
import os
import sqlite3
import logging

logging.basicConfig(level=logging.INFO,filename='app.log', filemode='w', format='%(asctime)s %(name)s - %(levelname)s - %(message)s')

def myvars():
  global cnx
  global ifolder
  global ofolder
  cnx = sqlite3.connect('db.sqlite')
  ifolder = 'input/'
  ofolder = 'output/'
  return cnx,ifolder,ofolder


def job():
    cnx,ifolder,ofolder = myvars()
    try:
        input = pd.read_csv(ifolder+'CUSTOMER_SAMPLE.csv')
    except:
        logging.info('Input File not found')
        return
    customers = list()
    for i in range(0,len(input)):
        customers.append(input['CUSTOMER_CODE'][i])

    input_data = pd.read_sql_query("SELECT * FROM input_data", cnx)
    input_data = input_data.query("CUSTOMER_CODE == @customers")

    '''
        queries
    '''

    customer = input_data[['CUSTOMER_CODE','FIRSTNAME','LASTNAME']].drop_duplicates()
    invoice = input_data[['CUSTOMER_CODE','INVOICE_CODE','AMOUNT','DATE']].drop_duplicates()
    invoice_item = input_data[['INVOICE_CODE','ITEM_CODE','AMOUNT','QUANTITY']].drop_duplicates()

    '''
        outputs
    '''

    customer.to_csv(ofolder+'CUSTOMER.csv',index=False)
    invoice.to_csv(ofolder+'INVOICE.csv',index=False)
    invoice_item.to_csv(ofolder+'INVOICE_ITEM.csv',index=False)

    
    os.system('mv input/* processed/CUSTOMER_SAMPLE.csv')
    logging.info('Files parsed successfully')
