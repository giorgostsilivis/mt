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
    with dynastatic (ship_id,lon,lat,course,speed,timestamp,shipname,length,width) AS
    (select vessels_dynamic.ship_id,vessels_dynamic.lon,
        vessels_dynamic.lat,vessels_dynamic.course,
        vessels_dynamic.speed,vessels_dynamic."timestamp",
        vessels_static.shipname,vessels_static."length",
        vessels_static.width from vessels_dynamic inner join vessels_static on vessels_dynamic.ship_id = vessels_static.ship_id),
    
    final (ship_id,lon,lat,course,speed,timestamp,shipname,length,width,port_name,event_type,event_timestamp) as
    (select dynastatic.ship_id,dynastatic.lon,
        dynastatic.lat,dynastatic.course,
        dynastatic.speed,dynastatic."timestamp",
        dynastatic.shipname,dynastatic."length",
        dynastatic.width,port_events.port_name,port_events.event_type,port_events.event_timestamp from dynastatic left join port_events on dynastatic.ship_id = port_events.ship_id),
    
    myview as (select ship_id,lon,lat,course,speed,timestamp,shipname,length,width,
    case when timestamp<event_timestamp then null else event_timestamp end as event_timestamp,
    case when timestamp<event_timestamp then null else port_name end as port_name,
    case when timestamp<event_timestamp then null else event_type end as event_type
    from final)
    
    select ship_id,lon,lat,course,speed,timestamp,shipname,length,width,max(port_name) as port_name,max(event_type) as event_type,max(event_timestamp) as event_timestamp from myview group by ship_id,lon,lat,course,speed,timestamp,shipname,length,width order by timestamp desc
    '''
    output_data = pd.read_sql_query(view, cnx)
    # print(input_data)

    '''
        outputs
    '''

    output_data.to_csv(ofolder+'output.csv',index=False)


    os.system('mv input/* processed/')
    logging.info('Files parsed successfully')
