import pandas as pd
import os
import sqlite3
import logging

logging.basicConfig(level=logging.INFO,filename='app.log', filemode='w', format='%(asctime)s %(name)s - %(levelname)s - %(message)s')

def myvars():
  global cnx
  global ifolder
  global ofolder
  cnx = sqlite3.connect('db.sqlite', timeout=30000)
  ifolder = 'input/'
  ofolder = 'output/'
  return cnx,ifolder,ofolder


def job():
    cnx,ifolder,ofolder = myvars()
    try:
        input = pd.read_csv(ifolder+'vessels_dynamic.csv')
        input2 = pd.read_csv(ifolder+'vessels_static.csv')
        input3 = pd.read_csv(ifolder+'port_events.csv')
    except:
        logging.info('Input File not found')
        return

    '''
        queries
    '''

    input.to_sql('vessels_dynamic', cnx,if_exists='replace',index=False)
    input2.to_sql('vessels_static', cnx,if_exists='replace',index=False)
    input3.to_sql('port_events', cnx,if_exists='replace',index=False)
    view = '''
    WITH temporaryTable (ship_id,port_name,event_type,event_timestamp) as
    (select ship_id,port_name,event_type,max(event_timestamp) from port_events group by ship_id)

    SELECT vessels_dynamic.ship_id,vessels_dynamic.lon,
    vessels_dynamic.lat,vessels_dynamic.course,
    vessels_dynamic.speed,vessels_dynamic."timestamp",
    vessels_static.shipname,vessels_static."length",
    vessels_static.width, temporaryTable.port_name, temporaryTable.event_type,
    temporaryTable.event_timestamp
    FROM ((vessels_dynamic
    INNER JOIN vessels_static ON vessels_dynamic.ship_id = vessels_static.ship_id)
    LEFT JOIN temporaryTable ON vessels_static.ship_id = temporaryTable.ship_id);
    '''
    output_data = pd.read_sql_query(view, cnx)
    # print(input_data)

    '''
        outputs
    '''

    output_data.to_csv(ofolder+'output.csv',index=False)


    os.system('mv input/* processed/')
    logging.info('Files parsed successfully')
